package listener

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/charmbracelet/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5" // Import NATS JetStream
	"github.com/valyala/fastjson"
)

type Stream struct {
	dbConn            *pgx.Conn
	pgConn            *pgconn.PgConn
	dbConfig          pgx.ConnConfig
	ctx               context.Context
	cancel            context.CancelFunc
	restartLSN        atomic.Uint64
	heartbeatInterval time.Duration
	slotName          string
	logger            *log.Logger
	failover          bool
	pluginArguments   map[string]string
	ChangeChannel     chan WalMessage
	isStreaming       bool
	lastError         error
	ackChannel        chan pglogrepl.LSN
	closeSync         sync.Once
}

type PgStreamConfig struct {
	DbConfig              pgx.ConnConfig
	SlotName              string
	BaseLogger            *log.Logger
	Failover              bool
	StreamOldData         bool
	StandbyMessageTimeout time.Duration
	Tables                []string
	ChangeChannel         chan WalMessage
}

type WalMessage struct {
	Error  error
	Change *WalChanges
}

type WalChanges struct {
	Lsn     pglogrepl.LSN `json:"nextlsn"`
	Changes []WalChange   `json:"change"`
}

type KeyType struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

type KeyValue struct {
	KeyType
	Value *interface{} `json:"value"`
}

type WalChange struct {
	Kind   string      `json:"kind"`
	Schema string      `json:"schema"`
	Table  string      `json:"table"`
	New    *[]KeyValue `json:"new"`
	Old    *[]KeyValue `json:"old"`
	PK     *[]KeyType  `json:"pk"`
}

func NewPgStream(ctx context.Context, config PgStreamConfig) (*Stream, error) {
	// Set replication=database to enable logical replication
	if config.DbConfig.RuntimeParams == nil {
		config.DbConfig.RuntimeParams = make(map[string]string)
	}
	config.DbConfig.RuntimeParams["replication"] = "database"
	config.DbConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	dbConn, err := pgx.ConnectConfig(ctx, &config.DbConfig)
	if err != nil {
		return nil, err
	}

	logger := config.BaseLogger.WithPrefix("pglogical")

	pluginArguments := map[string]string{
		//"write-in-chunks":     "true",
		"include-xids":        "true",
		"include-lsn":         "true",
		"include-transaction": "false",
		"add-tables":          strings.Join(config.Tables, ","),
		"include-pk":          "true",
		"pretty-print":        "false",
		"actions":             "insert,update,delete,truncate",
	}

	if len(config.Tables) == 0 {
		return nil, errors.New("no tables specified")
	}

	stream := &Stream{
		dbConn:            dbConn,
		dbConfig:          config.DbConfig,
		slotName:          config.SlotName,
		logger:            logger,
		failover:          config.Failover,
		pluginArguments:   pluginArguments,
		pgConn:            dbConn.PgConn(),
		heartbeatInterval: time.Second * 5,
		ChangeChannel:     config.ChangeChannel,
	}
	stream.ctx, stream.cancel = context.WithCancel(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, stream.pgConn)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to identify system")
	}

	logger.Debugf("System identifier: %s, timeline: %d, xlogpos: %d, dbname: %s", sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	var confirmedFlushLSNUnparsed string
	var consistentPoint string
	var slotActive bool
	//var snapshotName string

	confirmedFlushResult := dbConn.QueryRow(ctx, fmt.Sprintf("SELECT confirmed_flush_lsn, active FROM pg_replication_slots WHERE slot_name = '%s'", config.SlotName))
	if err := confirmedFlushResult.Scan(&confirmedFlushLSNUnparsed, &slotActive); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.WithDetail(err, "failed to get confirmed flush LSN")
		}

		logger.Debugf("Replication slot %s does not exist, creating it", config.SlotName)

		// TODO: Support snapshotting (Maybe???)
		result, err := pglogrepl.CreateReplicationSlot(ctx, stream.pgConn, config.SlotName, "wal2json", pglogrepl.CreateReplicationSlotOptions{})

		if err != nil {
			return nil, errors.WithDetail(err, "failed to create logical replication slot")
		}

		consistentPoint = result.ConsistentPoint
		//snapshotName = result.SnapshotName
	} else {
		if slotActive {
			logger.Debugf("Replication slot %s is already active", config.SlotName)
			return nil, ReplicationSlotInUseError{SlotName: config.SlotName}
		}
		logger.Debugf("Replication slot %s with LSN %s exists, using it", config.SlotName, confirmedFlushLSNUnparsed)
		consistentPoint = confirmedFlushLSNUnparsed
	}

	parsedLSN, err := pglogrepl.ParseLSN(consistentPoint)
	stream.setRestartLSN(parsedLSN)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to parse consistent point")
	}

	err = stream.startLr()

	stream.logger.Info("Started logical replication", "slot_name", config.SlotName, "consistent_point", stream.restartLSN)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to start logical replication")
	}
	go stream.sendHeartbeats()
	go stream.streamMessagesAsync()

	return stream, nil
}

func (s *Stream) GetRestartLSN() pglogrepl.LSN {
	// load and cast the atomic value to pglogrepl.LSN
	return pglogrepl.LSN(s.restartLSN.Load())
}

func (s *Stream) setRestartLSN(lsn pglogrepl.LSN) (prev pglogrepl.LSN, ok bool) {
	for {
		prev := pglogrepl.LSN(s.restartLSN.Load())
		if lsn <= prev {
			return prev, false
		}

		if s.restartLSN.CompareAndSwap(uint64(prev), uint64(lsn)) {
			return prev, true
		}
	}
}

func (s *Stream) sendHeartbeats() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.Tick(s.heartbeatInterval):
			{
				err := s.sendStandbyStatus()
				if err != nil {
					s.logger.Warn("Failed to send heartbeat", "error", err)
				}
			}
		}

	}
}

func (s *Stream) AckLSN(lsn pglogrepl.LSN) error {
	_, ok := s.setRestartLSN(lsn)
	if !ok {
		return nil
	}

	return s.sendStandbyStatus()
}

func (s *Stream) sendStandbyStatus() error {
	lsn := s.GetRestartLSN()
	err := pglogrepl.SendStandbyStatusUpdate(s.ctx, s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
	})
	if err != nil {
		return errors.WithDetail(err, "failed to send standby status")
	}
	return nil
}

func (s *Stream) streamMessagesAsync() {
	jsonParser := fastjson.Parser{}

	sendErrAndStop := func(err error) {
		s.lastError = err
		s.ChangeChannel <- WalMessage{Error: err}
		s.Stop()
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			rawMsg, err := s.pgConn.ReceiveMessage(s.ctx)
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				sendErrAndStop(err)
				return
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				sendErrAndStop(errors.New(errMsg.Message))
				return
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				s.logger.Warnf("Received unexpected message: %T\n", rawMsg)
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					sendErrAndStop(errors.WithDetail(err, "failed to parse primary keepalive message"))
					return
				}

				if pkm.ReplyRequested {
					err = s.sendStandbyStatus()
					if err != nil {
						sendErrAndStop(errors.WithDetail(err, "failed to send standby status"))
						return
					}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					s.logger.Fatalf("ParseXLogData failed: %s", err.Error())
				}

				parsed, err := jsonParser.ParseBytes(xld.WALData)
				if err != nil {
					sendErrAndStop(errors.WithDetail(err, "failed to parse WAL data"))
					return
				}
				unparsedLsn := string(parsed.GetStringBytes("nextlsn"))
				parsedLsn, err := pglogrepl.ParseLSN(unparsedLsn)

				if err != nil {
					sendErrAndStop(errors.WithDetail(err, "failed to parse next LSN"))
					return
				}

				changes := WalChanges{
					Lsn:     parsedLsn,
					Changes: []WalChange{},
				}

				parseKeyTypes := func(names []*fastjson.Value, types []*fastjson.Value) []KeyType {
					kts := make([]KeyType, len(names))
					for i, name := range names {
						kts[i] = KeyType{
							Key:  string(name.GetStringBytes()),
							Type: string(types[i].GetStringBytes()),
						}
					}
					return kts
				}

				parseKVs := func(names []*fastjson.Value, types []*fastjson.Value, values []*fastjson.Value) []KeyValue {
					kvs := make([]KeyValue, len(names))
					for i, name := range names {
						var value interface{} = nil
						if values != nil {
							switch values[i].Type() {
							case fastjson.TypeNumber:
								value = values[i].GetFloat64()
							case fastjson.TypeString:
								value = string(values[i].GetStringBytes())
							case fastjson.TypeTrue:
								value = true
							case fastjson.TypeFalse:
								value = false
							case fastjson.TypeNull:
								value = nil
							}
						}

						ref := &value
						if value == nil {
							ref = nil
						}

						kvs[i] = KeyValue{
							KeyType: KeyType{
								Key:  string(name.GetStringBytes()),
								Type: string(types[i].GetStringBytes()),
							},
							Value: ref,
						}
					}
					return kvs
				}

				changesArray := parsed.GetArray("change")
				for _, change := range changesArray {
					var newKVs, oldKVs *[]KeyValue
					var pks *[]KeyType
					if change.Exists("oldkeys") {
						oldkeys := change.Get("oldkeys")
						kvArr := parseKVs(oldkeys.GetArray("keynames"), oldkeys.GetArray("keytypes"), oldkeys.GetArray("keyvalues"))
						oldKVs = &kvArr
					}
					if change.Exists("columnnames") && change.Exists("columntypes") && change.Exists("columnvalues") {
						kvArr := parseKVs(change.GetArray("columnnames"), change.GetArray("columntypes"), change.GetArray("columnvalues"))
						newKVs = &kvArr
					}
					if change.Exists("pk") {
						pk := change.Get("pk")
						pkArr := parseKeyTypes(pk.GetArray("pknames"), pk.GetArray("pktypes"))
						pks = &pkArr
					}

					changes.Changes = append(changes.Changes, WalChange{
						Kind:   string(change.GetStringBytes("kind")),
						Schema: string(change.GetStringBytes("schema")),
						Table:  string(change.GetStringBytes("table")),
						New:    newKVs,
						Old:    oldKVs,
						PK:     pks,
					})
				}

				s.ChangeChannel <- WalMessage{Change: &changes}
			default:
				s.logger.Warnf("Received unexpected message: %T\n", rawMsg)
			}
		}
	}
}

func (s *Stream) startLr() error {
	var pluginArgumentsArr []string
	for k, v := range s.pluginArguments {
		pluginArgumentsArr = append(pluginArgumentsArr, fmt.Sprintf("\"%s\" '%s'", k, v))
	}
	err := pglogrepl.StartReplication(s.ctx, s.pgConn, s.slotName, s.GetRestartLSN(), pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgumentsArr,
	})
	if err != nil {
		return errors.WithDetail(err, "failed to start replication")
	}
	s.isStreaming = true
	return nil
}

func (s *Stream) IsStreaming() bool {
	return s.isStreaming
}

func (s *Stream) Stop() (err error) {
	s.closeSync.Do(func() {
		// Send the last LSN to the server
		s.sendStandbyStatus()

		s.cancel() // Cancel the context to stop goroutines
		s.isStreaming = false

		// Drain the change channel
		for range s.ChangeChannel {
		}

		// Close the database connection
		if err = s.dbConn.Close(context.Background()); err != nil {
			err = errors.Wrap(err, "failed to close database connection")
		}

		s.logger.Info("Logical replication stream stopped")
		close(s.ChangeChannel)
	})
	return
}

type ReplicationSlotInUseError struct {
	SlotName string
}

func (e ReplicationSlotInUseError) Error() string {
	return fmt.Sprintf("replication slot %s is already active", e.SlotName)
}

type ReplicationSlotSyncedError struct {
	SlotName string
}

func (e ReplicationSlotSyncedError) Error() string {
	return fmt.Sprintf("replication slot %s was synced from a standby, and cannot be used for logical replication", e.SlotName)
}
