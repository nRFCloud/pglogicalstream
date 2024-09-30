package listener

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/charmbracelet/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/valyala/fastjson"
)

type Stream struct {
	dbConn            *pgx.Conn
	pgConn            *pgconn.PgConn
	dbConfig          pgx.ConnConfig
	ctx               context.Context
	cancel            context.CancelFunc
	restartLSN        pglogrepl.LSN
	heartbeatInterval time.Duration
	slotName          string
	logger            *log.Logger
	failover          bool
	pluginArguments   map[string]string
	changeChannel     chan WalChanges
	isStreaming       bool
	lastError         error
}

type PgStreamConfig struct {
	DbConfig              pgx.ConnConfig
	SlotName              string
	BaseLogger            *log.Logger
	Failover              bool
	StreamOldData         bool
	StandbyMessageTimeout time.Duration
	Tables                []string
	ChangeChannel         chan WalChanges
}

type WalChanges struct {
	Error   error
	Lsn     pglogrepl.LSN `json:"nextlsn"`
	Changes []WalChange   `json:"change"`
}

type WalChange struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnTypes  []string      `json:"columntypes"`
	ColumnValues []interface{} `json:"columnvalues"`
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
		"pretty-print":        "false",
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
		changeChannel:     config.ChangeChannel,
	}
	stream.ctx, stream.cancel = context.WithCancel(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, stream.pgConn)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to identify system")
	}

	logger.Debugf("System identifier: %s, timeline: %d, xlogpos: %d, dbname: %s", sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	var confirmedFlushLSNUnparsed string
	var consistentPoint string
	//var snapshotName string

	confirmedFlushResult := dbConn.QueryRow(ctx, fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", config.SlotName))
	if err := confirmedFlushResult.Scan(&confirmedFlushLSNUnparsed); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.WithDetail(err, "failed to get confirmed flush LSN")
		}

		logger.Debugf("Replication slot %s does not exist, creating it", config.SlotName)

		// TODO: Support snapshotting
		result, err := pglogrepl.CreateReplicationSlot(ctx, stream.pgConn, config.SlotName, "wal2json", pglogrepl.CreateReplicationSlotOptions{})

		if err != nil {
			return nil, errors.WithDetail(err, "failed to create logical replication slot")
		}

		consistentPoint = result.ConsistentPoint
		//snapshotName = result.SnapshotName
	} else {
		logger.Debugf("Replication slot %s with LSN %s exists, using it", config.SlotName, confirmedFlushLSNUnparsed)
		consistentPoint = confirmedFlushLSNUnparsed
	}

	stream.restartLSN, err = pglogrepl.ParseLSN(consistentPoint)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to parse consistent point")
	}

	err = stream.startLr()

	stream.logger.Info("Started logical replication", "slot_name", config.SlotName, "consistent_point", consistentPoint)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to start logical replication")
	}
	go stream.sendHeartbeats()
	go stream.streamMessagesAsync()

	return stream, nil
}

func (s *Stream) sendHeartbeats() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.Tick(s.heartbeatInterval):
			{
				s.logger.Debug("Sending heartbeat", "lsn", s.restartLSN)
				err := s.sendStandbyStatus()
				if err != nil {
					s.logger.Warn("Failed to send heartbeat", "error", err)
				}
			}
		}

	}
}

func (s *Stream) AckLSN(lsn pglogrepl.LSN) error {
	s.restartLSN = lsn

	err := pglogrepl.SendStandbyStatusUpdate(s.ctx, s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.restartLSN,
	})
	if err != nil {
		return errors.WithDetail(err, "failed to send standby status")
	}
	return nil
}

func (s *Stream) sendStandbyStatus() error {
	err := pglogrepl.SendStandbyStatusUpdate(s.ctx, s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.restartLSN,
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
		s.changeChannel <- WalChanges{Error: err}
		//s.Stop()
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

				changesArray := parsed.GetArray("change")
				for _, change := range changesArray {
					changes.Changes = append(changes.Changes, WalChange{
						Kind:   string(change.GetStringBytes("kind")),
						Schema: string(change.GetStringBytes("schema")),
						Table:  string(change.GetStringBytes("table")),
						ColumnNames: func() []string {
							var columnNames []string
							for _, col := range change.GetArray("columnnames") {
								columnNames = append(columnNames, string(col.GetStringBytes()))
							}
							return columnNames
						}(),
						ColumnTypes: func() []string {
							var columnTypes []string
							for _, col := range change.GetArray("columntypes") {
								columnTypes = append(columnTypes, string(col.GetStringBytes()))
							}
							return columnTypes
						}(),
						ColumnValues: func() []interface{} {
							var columnValues []interface{}
							for _, col := range change.GetArray("columnvalues") {
								switch col.Type() {
								case fastjson.TypeNumber:
									columnValues = append(columnValues, col.GetFloat64())
								case fastjson.TypeString:
									columnValues = append(columnValues, string(col.GetStringBytes()))
								case fastjson.TypeTrue:
									columnValues = append(columnValues, true)
								case fastjson.TypeFalse:
									columnValues = append(columnValues, false)
								case fastjson.TypeNull:
									columnValues = append(columnValues, nil)
								default:
									columnValues = append(columnValues, col.String())
								}
							}
							return columnValues
						}(),
					})
				}

				s.changeChannel <- changes
			}
		}
	}
}

func (s *Stream) startLr() error {
	var pluginArgumentsArr []string
	for k, v := range s.pluginArguments {
		pluginArgumentsArr = append(pluginArgumentsArr, fmt.Sprintf("\"%s\" '%s'", k, v))
	}
	err := pglogrepl.StartReplication(s.ctx, s.pgConn, s.slotName, s.restartLSN, pglogrepl.StartReplicationOptions{
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

func (s *Stream) Stop() error {
	// Send the last LSN to the server
	s.AckLSN(s.restartLSN)

	s.cancel() // Cancel the context to stop goroutines
	s.isStreaming = false

	// Drain the change channel
	draining := true
	for draining {
		select {
		case _, ok := <-s.changeChannel:
			if !ok {
				// Channel is closed, we're done draining
				draining = false
			}
		default:
			// Channel is empty, we're done draining
			close(s.changeChannel)
			draining = false
		}
	}

	// Close the database connection
	if err := s.dbConn.Close(context.Background()); err != nil {
		return errors.Wrap(err, "failed to close database connection")
	}

	s.logger.Info("Logical replication stream stopped")
	return nil
}
