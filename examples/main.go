package main

import (
	"context"
	"github.com/charmbracelet/log"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"time"
)
import "pglogicalstream-locking/listener"

type metrics struct {
	processedChanges      *prometheus.CounterVec
	processedTransactions *prometheus.CounterVec
}

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

	stream, err := listener.NewPgStream(context.Background(), streamConfig)

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
