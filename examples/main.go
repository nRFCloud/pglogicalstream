package main

import (
	"context"
	"github.com/charmbracelet/log"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"os/signal"
	"pglogicalstream/listener"
	"syscall"
	"time"
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
	go doListen()

	DefaultShutdownHook.WaitForExit()
}

func doListen() {
	connConfig, err := pgx.ParseConfig(
		"postgres://postgres:postgres@central-1.brumby-dragon.ts.net/postgres?sslmode=disable",
	)
	if err != nil {
		panic(err)
	}

	logger := log.NewWithOptions(os.Stdout, log.Options{
		Level:        log.DebugLevel,
		ReportCaller: true,
	})

	changeChannel := make(chan listener.WalChanges)
	mReg := prometheus.NewRegistry()
	m := newMetrics(mReg)

	http.Handle("/metrics", promhttp.HandlerFor(mReg, promhttp.HandlerOpts{}))

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

	natsConnection, err := nats.Connect("nats://central-1.brumby-dragon.ts.net:4222")
	if err != nil {
		logger.Errorf("error connecting to nats: %s", err)
		return
	}

	defer natsConnection.Close()

	logger.Debugf("Waiting for lock")
	lock, err := NewNatsLock(context.Background(), natsConnection, "test_lock")

	if err != nil {
		logger.Errorf("error acquiring lock: %s", err)
		return
	}

	defer lock.Unlock()

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
				logger.Errorf("error: %s", msg.Error)
				stream.Stop()
				break
			}
			//logger.Info("Received message", "lsn", msg.Lsn, "changes", msg.Changes)
			err = stream.AckLSN(msg.Lsn)
			if err != nil {
				logger.Errorf("error acknowledging LSN: %s", err)
				stream.Stop()
			}
			m.processedTransactions.WithLabelValues(msg.Changes[0].Table).Inc()
			for _, change := range msg.Changes {
				m.processedChanges.WithLabelValues(change.Table).Inc()
			}
		}
	}

	logger.Info("Done")
}

type NatsLock struct {
	client           *nats.Conn
	jsContext        jetstream.JetStream
	lockName         string
	lockDeliverGroup string
	LockCtx          context.Context
	cancel           context.CancelFunc
	lockMsg          jetstream.Msg
}

func NewNatsLock(ctx context.Context, client *nats.Conn, lockGroup string) (*NatsLock, error) {
	lockCtx, cancel := context.WithCancel(ctx)

	jsContext, err := jetstream.New(client)

	if err != nil {
		cancel()
		return nil, err
	}

	lock := &NatsLock{
		client:           client,
		jsContext:        jsContext,
		lockName:         lockGroup,
		lockDeliverGroup: lockGroup + ".group",
		LockCtx:          lockCtx,
		cancel:           cancel,
	}

	stream, err := jsContext.CreateOrUpdateStream(lockCtx, jetstream.StreamConfig{
		Name:                 lockGroup,
		Subjects:             []string{lockGroup + ".*"},
		Storage:              jetstream.FileStorage,
		MaxMsgs:              1,
		Discard:              jetstream.DiscardNew,
		MaxMsgsPerSubject:    1,
		Retention:            jetstream.WorkQueuePolicy,
		DiscardNewPerSubject: true,
	})

	if err != nil {
		return nil, err
	}

	consumer, err := stream.CreateOrUpdateConsumer(lockCtx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       10 * time.Second,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		Durable:       "test",
	})

	if err != nil {
		return nil, err
	}

	client.Publish(lockGroup+".lock", []byte("lock"))

	messages, err := consumer.Messages(
		jetstream.PullMaxMessages(1),
		jetstream.PullThresholdMessages(1),
		jetstream.PullHeartbeat(2*time.Second),
	)

	if err != nil {
		return nil, err
	}

	msg, err := messages.Next()

	if err != nil {
		return nil, err
	}

	lock.lockMsg = msg

	go lock.heartbeat()

	return lock, nil
}

func (n *NatsLock) heartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.LockCtx.Done():
			return
		case <-ticker.C:
			err := n.lockMsg.InProgress()
			if err != nil {
				log.Errorf("Error sending in progress: %s", err)
				n.Unlock()
			}
		}
	}
}

func (n *NatsLock) Unlock() error {
	err := n.lockMsg.Nak()
	n.cancel()
	if err != nil {
		log.Errorf("Error acking lock: %s", err)
		return err
	}
	return nil
}
