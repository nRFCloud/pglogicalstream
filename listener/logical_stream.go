package listener

import (
	"context"
	"fmt"
	"math/rand"
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
	probeInterval     time.Duration
	slotName          string
	logger            *log.Logger
	failover          bool
	pluginArguments   map[string]string
	ChangeChannel     chan WalMessage
	isStreaming       bool
	lastError         error
	ackChannel        chan pglogrepl.LSN
	closeSync         sync.Once
	probeConn         *pgx.Conn
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
	MaxWaitForSlot        time.Duration
	SlotCheckInterval     time.Duration
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

func verifyPrimary(ctx context.Context, dbConn *pgx.Conn) (sysId string, isInRecovery bool, err error) {
	err = dbConn.QueryRow(ctx, "SELECT system_identifier, pg_is_in_recovery() FROM pg_control_system()").Scan(&sysId, &isInRecovery)
	return
}

func NewPgStream(ctx context.Context, config PgStreamConfig) (stream *Stream, err error) {
	ctx, cancel := context.WithCancel(ctx)

	defer func() {
		if err != nil {
			cancel()
		}
	}()
	// Set replication=database to enable logical replication
	probeConnConfig := config.DbConfig.Copy()
	probeConn, err := pgx.ConnectConfig(ctx, probeConnConfig)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to connect probe connection")
	}

	dbConfig := config.DbConfig.Copy()
	if dbConfig.RuntimeParams == nil {
		dbConfig.RuntimeParams = make(map[string]string)
	}
	dbConfig.RuntimeParams["replication"] = "database"
	dbConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	dbConn, err := pgx.ConnectConfig(ctx, dbConfig)
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

	stream = &Stream{
		dbConn:            dbConn,
		dbConfig:          config.DbConfig,
		slotName:          config.SlotName,
		logger:            logger,
		probeConn:         probeConn,
		failover:          config.Failover,
		probeInterval:     time.Second * 5,
		pluginArguments:   pluginArguments,
		pgConn:            dbConn.PgConn(),
		heartbeatInterval: time.Second * 5,
		ChangeChannel:     config.ChangeChannel,
		ctx:               ctx,
		cancel:            cancel,
	}

	// Set default values if not provided
	if config.SlotCheckInterval == 0 {
		config.SlotCheckInterval = 5 * time.Second
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, stream.pgConn)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to identify system")
	}

	logger.Debugf("System identifier: %s, timeline: %d, xlogpos: %d, dbname: %s", sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	// Verify that both connections are connected to the same node and that we are connected to a primary
	// IDENTIFY_SYSTEM will NOT work on our probe connection, so we need to use the dbConn to verify that we are connected to a primary
	// select system_identifier, pg_is_in_recovery from pg_control_system(), pg_is_in_recovery();
	dbConnSysId, dbConnIsInRecovery, err := verifyPrimary(ctx, dbConn)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to verify that we are connected to a primary")
	}
	probeConnSysId, probeConnIsInRecovery, err := verifyPrimary(ctx, probeConn)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to verify that we are connected to a primary")
	}

	if dbConnSysId != probeConnSysId {
		return nil, errors.New("database connections are connected to different nodes")
	}

	if dbConnIsInRecovery || probeConnIsInRecovery {
		return nil, errors.New("Logical replication is not supported on standby nodes, aborting WAL stream")
	}

	var confirmedFlushLSNUnparsed string
	var consistentPoint string
	var slotActive bool
	var slotPlugin string

	for {
		confirmedFlushResult := dbConn.QueryRow(ctx, fmt.Sprintf("SELECT confirmed_flush_lsn, active, plugin FROM pg_replication_slots WHERE slot_name = '%s'", config.SlotName))
		err := confirmedFlushResult.Scan(&confirmedFlushLSNUnparsed, &slotActive, &slotPlugin)

		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				logger.Debugf("Replication slot %s does not exist, creating it", config.SlotName)
				result, err := pglogrepl.CreateReplicationSlot(ctx, stream.pgConn, config.SlotName, "wal2json", pglogrepl.CreateReplicationSlotOptions{})
				if err != nil {
					return nil, errors.WithDetail(err, "failed to create logical replication slot")
				}
				consistentPoint = result.ConsistentPoint
				break
			}
			return nil, errors.WithDetail(err, "failed to get confirmed flush LSN")
		}

		if !slotActive {
			logger.Debugf("Replication slot %s exists and is inactive, using it", config.SlotName)
			consistentPoint = confirmedFlushLSNUnparsed
			break
		}

		if slotPlugin != "wal2json" {
			return nil, ReplicationSlotPluginMismatchError{SlotName: config.SlotName, ExpectedPlugin: "wal2json", ActualPlugin: slotPlugin}
		}
	}

	parsedLSN, err := pglogrepl.ParseLSN(consistentPoint)
	stream.setRestartLSN(parsedLSN)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to parse consistent point")
	}

	go stream.startLr()

	return stream, nil
}

func (s *Stream) startLr() error {
	var pluginArgumentsArr []string
	for k, v := range s.pluginArguments {
		pluginArgumentsArr = append(pluginArgumentsArr, fmt.Sprintf("\"%s\" '%s'", k, v))
	}
	acquired := false

	for !acquired {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
			s.logger.Info("Waiting for slot to become inactive...")

			err := pglogrepl.StartReplication(s.ctx, s.pgConn, s.slotName, s.GetRestartLSN(), pglogrepl.StartReplicationOptions{
				PluginArgs: pluginArgumentsArr,
			})

			if err != nil {
				// If the slot is in use, we need to wait for it to become inactive
				if errors.Is(err, pgx.ErrTxClosed) {
					s.logger.Errorf("Replication slot %s is in use, retrying", s.slotName, "error", err)

					// Generate a random jitter between 0 and 100ms
					jitter := rand.Intn(100)

					select {
					case <-s.ctx.Done():
						return s.ctx.Err()
					case <-time.After(100*time.Millisecond + time.Duration(jitter)*time.Millisecond):
						continue
					}
				}

				// Send error and stop
				s.lastError = err
				s.ChangeChannel <- WalMessage{Error: err}
				s.Stop()
				return err
			}

			acquired = true
		}
	}

	s.logger.Info("Replication slot is now inactive, starting to stream messages", "slot_name", s.slotName)

	s.isStreaming = true
	go s.sendHeartbeats()
	go s.streamMessagesAsync()
	go s.probePrimary()
	return nil
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

func (s *Stream) probePrimary() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.Tick(s.probeInterval):
			_, isInRecovery, err := verifyPrimary(s.ctx, s.probeConn)
			if err != nil {
				s.logger.Warn("Failed to probe connection", "error", err)
				s.Stop()
				return
			}
			if isInRecovery {
				s.logger.Warn("Probing connection to primary failed, stopping stream", "slot_name", s.slotName)
				s.Stop()
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
					s.logger.Warn("Timeout receiving message, continuing", "error", err)
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

			if _, ok := rawMsg.(*pgproto3.CommandComplete); ok {
				sendErrAndStop(errors.New("Stream ended prematurely"))
				return
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				// s.logger.Warnf("Received unexpected message: %T\n", rawMsg)
				sendErrAndStop(errors.New("Received unexpected message"))
				return
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
				//sendErrAndStop(errors.New("Received unexpected message"))
				return
			}
		}
	}
}

func (s *Stream) IsStreaming() bool {
	return s.isStreaming
}

func (s *Stream) closeConnections() (err error) {
	if s.dbConn != nil {
		if err = s.dbConn.Close(context.Background()); err != nil {
			err = errors.Wrap(err, "failed to close database connection")
		}
	}

	if s.probeConn != nil {
		if err = s.probeConn.Close(context.Background()); err != nil {
			err = errors.Wrap(err, "failed to close probe connection")
		}
	}

	return
}

func (s *Stream) GetLastError() error {
	return s.lastError
}

func (s *Stream) Stop() (err error) {
	s.closeSync.Do(func() {
		// Send the last LSN to the server
		s.sendStandbyStatus()

		s.cancel() // Cancel the context to stop goroutines
		s.isStreaming = false

		// Close the database connection
		if err = s.closeConnections(); err != nil {
			err = errors.Wrap(err, "failed to close connections")
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

type ReplicationSlotPluginMismatchError struct {
	SlotName       string
	ExpectedPlugin string
	ActualPlugin   string
}

func (e ReplicationSlotPluginMismatchError) Error() string {
	return fmt.Sprintf("replication slot %s is using plugin %s, expected plugin %s", e.SlotName, e.ActualPlugin, e.ExpectedPlugin)
}
