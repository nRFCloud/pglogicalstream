package benthos

// import (
// 	"context"
// 	"log"
// 	"pglogicalstream/listener"
// 	"runtime"
// 	"sync"
// 	"sync/atomic"

// 	"github.com/jackc/pglogrepl"
// 	"github.com/jackc/pgx/v5"
// 	"github.com/redpanda-data/benthos/v4/public/service"
// )

// var pglogicalInputSpec = service.NewConfigSpec().
// 	Summary("Reads from a wal2json compatible PostgreSQL logical replication stream.").
// 	Beta().
// 	Field(service.NewStringField("dsn")).
// 	Field(service.NewStringListField("tables").
// 		Description("A list of tables to replicate.").
// 		Example([]string{"public.table1", "public.table2"})).
// 	Field(service.NewStringField("slot").
// 		Description("The name of the slot to create and replicate from.").
// 		Example("benthos_slot"))

// func init() {
// 	err := service.RegisterInput("pglogical", pglogicalInputSpec)
// }

// type lsnCommitTracker struct {
//     buffer   []lsnEntry
//     nextAdd  int
//     nextAck  int
//     listener *listener.Stream
//     mu       sync.Mutex
// }

// type lsnEntry struct {
//     lsn       pglogrepl.LSN
//     committed atomic.Bool
// }

// func newLSNCommitTracker(size int, listener *listener.Stream) *lsnCommitTracker {
//     return &lsnCommitTracker{
//         buffer:   make([]lsnEntry, size),
//         listener: listener,
//     }
// }

// func (t *lsnCommitTracker) AddLSN(lsn pglogrepl.LSN) func() {
//     t.mu.Lock()
//     for (t.nextAdd+1)%len(t.buffer) == t.nextAck {
//         t.mu.Unlock()
//         runtime.Gosched() // Yield to other goroutines
//         t.mu.Lock()
//     }

//     index := t.nextAdd
//     t.buffer[index].lsn = lsn
//     t.buffer[index].committed.Store(false)
//     t.nextAdd = (t.nextAdd + 1) % len(t.buffer)
//     t.mu.Unlock()

//     return func() {
//         t.buffer[index].committed.Store(true)
//         t.tryCommit()
//     }
// }

// func (t *lsnCommitTracker) tryCommit() {
//     t.mu.Lock()
//     defer t.mu.Unlock()

//     for t.nextAck != t.nextAdd {
//         if !t.buffer[t.nextAck].committed.Load() {
//             break
//         }
//         t.listener.AckLSN(t.buffer[t.nextAck].lsn)
//         t.nextAck = (t.nextAck + 1) % len(t.buffer)
//     }
// }

// type pglogicalInput struct {
// 	dsn string
// 	connConfig *pgx.ConnConfig
// 	slotName   string
// 	tables     []string
// 	pgStream   *listener.Stream
// 	walMsgChannel chan listener.WalMessage
// 	changeChannel chan WalChangeWithAck
// 	channelLock sync.Mutex
// 	maxPendingLSNs int
// 	pendingLSNs []pglogrepl.LSN
// 	lsnTracker *lsnCommitTracker
// }

// type WalChangeWithAck struct {
// 	WalMessage service.MessageBatch
// 	AckFunc func()
// }

// func newPglogicalInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*pglogicalInput, error) {
// 	dsn, err := conf.FieldString("dsn")
// 	if err != nil {
// 		return nil, err
// 	}
// 	slotName, err := conf.FieldString("slot")
// 	if err != nil {
// 		return nil, err
// 	}

// 	connConfig, err := pgx.ParseConfig(dsn)
// 	if err != nil {
// 		return nil, err
// 	}

// 	tables, err := conf.FieldStringList("tables")
// 	if err != nil {
// 		return nil, err
// 	}

// 	changeChannel := make(chan listener.WalMessage, 100)
// 	walMsgChannel := make(chan listener.WalMessage, 100)

// 	return &pglogicalInput{
// 		dsn: dsn,
// 		connConfig: connConfig,
// 		slotName: slotName,
// 		tables: tables,
// 		changeChannel: changeChannel,
// 		walMsgChannel: walMsgChannel,
// 	}, nil
// }

// func (p *pglogicalInput) Connect(ctx context.Context) error {
// 	pgStream, err := listener.NewPgStream(ctx, listener.PgStreamConfig{
// 		DbConfig: *p.connConfig,
// 		SlotName: p.slotName,
// 		Tables: p.tables,
// 		ChangeChannel: p.walMsgChannel,
// 	})

// 	p.lsnTracker = newLSNCommitTracker(100, pgStream)

// 	if err != nil {
// 		return err
// 	}

// 	p.pgStream = pgStream

// 	go func() {
// 		defer p.Close(ctx)

// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case msg, ok := <-p.walMsgChannel:
// 				if !ok {
// 					return
// 				}
// 				if msg.Error != nil {
// 					log.Printf("Error in walMsgChannel: %v", msg.Error)
// 					return
// 				}

// 				msgArr := make([]*service.Message, len(msg.Change.Changes))
// 				for i, change := range msg.Change.Changes {
// 					msgArr[i] = service.NewMessage(nil)
// 					msgArr[i].SetStructured(change)
// 				}

// 				walChangeWithAck := WalChangeWithAck{
// 					WalMessage: msgArr,
// 					AckFunc: p.lsnTracker.AddLSN(msg.Change.Lsn),
// 				}

// 				p.changeChannel <- walChangeWithAck
// 			}
// 		}
// 	}()

// 	return nil
// }

// func (p *pglogicalInput) Close(ctx context.Context) error {
// 	return p.pgStream.Stop()
// }

// func (p *pglogicalInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
// 	p.channelLock.Lock()
// 	msgChan := p.changeChannel
// 	p.channelLock.Unlock()

// 	var batch service.MessageBatch

// 	select {
// 	case m, open := <-msgChan:
// 		if !open {
// 			return nil, nil, service.ErrNotConnected
// 		}

// 	}
// }
//
