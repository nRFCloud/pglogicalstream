package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"pglogicalstream/listener"
	"syscall"
	"time"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	processedChanges      *prometheus.CounterVec
	processedTransactions *prometheus.CounterVec
}

var shutdownHooks []func()

type ShutdownHook struct {
	hooks []func()
	done  chan bool
}

func (s *ShutdownHook) AddHook(h func()) {
	s.hooks = append(s.hooks, h)
}

func RegisterShutdownHook() *ShutdownHook {
	hook := &ShutdownHook{
		done: make(chan bool, 1),
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		for _, h := range hook.hooks {
			h()
		}
		hook.done <- true
	}()

	return hook
}

func (s *ShutdownHook) WaitForExit() {
	<-s.done
}

var DefaultShutdownHook = RegisterShutdownHook()

func newMetrics(reg *prometheus.Registry) *metrics {
	m := &metrics{
		processedChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "processed_changes",
			Help: "Number of processed changes",
		}, []string{"table"}),
		processedTransactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "processed_transactions",
			Help: "Number of processed transactions",
		}, []string{"table"}),
	}
	reg.MustRegister(m.processedChanges)
	return m
}

func main() {
	go func() {
		doListen()
		os.Exit(0)
	}()

	DefaultShutdownHook.WaitForExit()
}

func doListen() {
	connConfig, err := pgx.ParseConfig(
		"postgres://john:eGqpHPBXAPkNBrTovdMWwXghhht4g0SfrqdJmzmzmotOGCAFEAZqi7NA4Fv4Txhl@default-best-cluster.brumby-dragon.ts.net/postgres",
	)
	if err != nil {
		panic(err)
	}

	logger := log.NewWithOptions(os.Stdout, log.Options{
		Level:        log.DebugLevel,
		ReportCaller: true,
	})

	changeChannel := make(chan listener.WalMessage)

	streamConfig := listener.PgStreamConfig{
		DbConfig:              *connConfig,
		StandbyMessageTimeout: time.Duration(10) * time.Second,
		SlotName:              "test_slot_3",
		BaseLogger:            logger,
		StreamOldData:         false,
		Failover:              false,
		Tables:                []string{"public.device"},
		ChangeChannel:         changeChannel,
	}

	natsConnection, err := nats.Connect("nats://default-nats-cluster.brumby-dragon.ts.net:4222")
	if err != nil {
		logger.Errorf("error connecting to nats: %s", err)
		return
	}

	defer natsConnection.Close()

	logger.Debugf("Waiting for lock")
	lock, err := NewNatsKVLock(context.Background(), natsConnection, "test_lock", 30*time.Second, 10*time.Second)

	if err != nil {
		logger.Errorf("error acquiring lock: %s", err)

		return
	}

	DefaultShutdownHook.AddHook(func() { lock.Unlock() })

	stream, err := listener.NewPgStream(lock.LockCtx, streamConfig)

	if err != nil {
		logger.Errorf("error creating stream: %s", err)
		return
	}

	go http.ListenAndServe(":2112", nil)

	for stream.IsStreaming() {
		select {
		case msg, ok := <-changeChannel:
			if !ok {
				stream.Stop()
			}
			if msg.Error != nil {
				logger.Error("error", "error", msg.Error)
				stream.Stop()
				break
			}
			logger.Info("Received message", "lsn", msg.Change.Lsn, "count", len(msg.Change.Changes))
			for _, change := range msg.Change.Changes {
				logger.Info("Change", "table", change.Table, "type", change.Kind, "schema", change.Schema, "New", change.New, "Old", change.Old)
			}
			err = stream.AckLSN(msg.Change.Lsn)
			if err != nil {
				logger.Errorf("error acknowledging LSN: %s", err)
				stream.Stop()
			}
		}
	}

	logger.Info("Done")
}

type NatsKVLock struct {
	client          *nats.Conn
	kv              jetstream.KeyValue
	LockCtx         context.Context
	cancel          context.CancelFunc
	ownerId         string
	lockName        string
	kvWatcher       jetstream.KeyWatcher
	lockRevision    uint64
	lockExpireDelay time.Duration
	lockTTL         time.Duration
}

func NewNatsKVLock(ctx context.Context, client *nats.Conn, lockName string, lockTTL time.Duration, lockExpireDelay time.Duration) (*NatsKVLock, error) {
	lockCtx, cancel := context.WithCancel(ctx)
	ownerId := uuid.New().String()

	js, err := jetstream.New(client)
	if err != nil {
		cancel()
		return nil, err
	}

	// Create or update the key value store
	kv, err := js.CreateOrUpdateKeyValue(lockCtx, jetstream.KeyValueConfig{
		Bucket:  lockName,
		TTL:     lockTTL,
		Storage: jetstream.MemoryStorage,
	})

	kvWatcher, err := kv.Watch(lockCtx, "lock")
	if err != nil {
		cancel()
		return nil, err
	}

	lockRevision, err := kv.Create(lockCtx, "lock", []byte(ownerId))

	// acquire loop
	for err != nil {
		// Try to create the lock
		// Wait for the lock to be released
		select {
		case <-lockCtx.Done():
			// Lock was cancelled
			return nil, lockCtx.Err()
		case item := <-kvWatcher.Updates():
			if item == nil {
				break
			}
			if item.Operation() == jetstream.KeyValueDelete {
				// Lock was deleted by ttl, so we should wait a bit before trying to create it again.
				log.Info("Lock deleted by ttl, waiting before trying to create it again")
				time.Sleep(lockExpireDelay)
				lockRevision, err = kv.Create(lockCtx, "lock", []byte(ownerId))
			}
			if item.Operation() == jetstream.KeyValuePurge {
				// Lock was purged, so we should try to create it again.
				lockRevision, err = kv.Create(lockCtx, "lock", []byte(ownerId))
			}
		}
	}

	lock := NatsKVLock{
		client:       client,
		kv:           kv,
		LockCtx:      lockCtx,
		cancel:       cancel,
		ownerId:      ownerId,
		lockName:     lockName,
		lockRevision: lockRevision,
		kvWatcher:    kvWatcher,
	}

	go lock.heartbeat()
	return &lock, nil
}

func (l *NatsKVLock) heartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer l.kvWatcher.Stop()
	for {
		select {
		case <-l.LockCtx.Done():
			return
		case <-ticker.C:
			rev, err := l.kv.Update(l.LockCtx, "lock", []byte(l.ownerId), l.lockRevision)
			if err != nil {
				log.Error("Error updating lock: %s", err)
				l.Unlock()
				return
			}
			l.lockRevision = rev
		case item := <-l.kvWatcher.Updates():
			if item != nil && item.Revision() > l.lockRevision {
				// Something terrible has happened, we lost the lock
				log.Error("Lost lock", "item", item)
				l.Unlock()
				return
			}
		}
	}
}

func (l *NatsKVLock) Unlock() error {
	l.cancel()
	err := l.kv.Purge(context.Background(), "lock", jetstream.LastRevision(l.lockRevision))
	if err != nil {
		log.Error("Error purging lock", "error", err)
	} else {
		log.Info("Lock purged")
	}
	return err
}
