package main

import (
	"context"
	"os"
	"os/signal"
	_ "pglogicalstream/benthos"
	"syscall"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/connect/public/bundle/free/v4"
)

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

func main() {
	// service.RunCLI(context.Background())

	builder := service.NewStreamBuilder()

	err := builder.AddInputYAML(
		`
pg_wal:
  dsn: "postgres://john:letmein@default-best-cluster-svc.brumby-dragon.ts.net/postgres"
  tables:
    - "public.device"
  slot: "test_slot_4"
  checkpoint_limit: 1
  commit_period: 1s
  auto_replay_nacks: true
  failover: true
  batching:
    count: 10
    period: 10s
`)

	if err != nil {
		panic(err)
	}

	err = builder.AddOutputYAML(`
nats_jetstream:
  urls: ["nats://default-nats-cluster.brumby-dragon.ts.net:4222"]
  subject: "pg_cdc.device"
`)

	if err != nil {
		panic(err)
	}

	err = builder.SetLoggerYAML(`
level: ALL
`)
	if err != nil {
		panic(err)
	}

	stream, err := builder.Build()
	if err != nil {
		panic(err)
	}

	DefaultShutdownHook.AddHook(func() {
		stream.Stop(context.Background())
	})

	go stream.Run(context.Background())

	DefaultShutdownHook.WaitForExit()
}
