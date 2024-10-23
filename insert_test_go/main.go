package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	poolSize := flag.Int("pool", 10, "Connection pool size")
	numQueries := flag.Int("queries", 1000, "Number of queries to run")
	batchSize := flag.Int("batch", 100, "Batch size for queries")
	dsn := flag.String("dsn", "", "Database connection string")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("DSN is required. Use the -dsn flag to provide it.")
	}

	config, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		log.Fatalf("Error parsing config: %v", err)
	}
	config.MaxConns = int32(*poolSize)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	query := `
		INSERT INTO device (tenant_id, device_id, name, data) 
		VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), '{"key": "value"}');
	`

	var wg sync.WaitGroup
	var completedQueries int64
	start := time.Now()

	// Progress reporting goroutine
	done := make(chan bool)
	go reportProgress(start, &completedQueries, *numQueries, done)

	// Worker function
	worker := func() {
		defer wg.Done()
		batch := &pgx.Batch{}
		for i := 0; i < *batchSize; i++ {
			batch.Queue(query)
		}

		for {
			br := pool.SendBatch(context.Background(), batch)
			_, err := br.Exec()
			if err != nil {
				log.Printf("Error executing batch: %v", err)
				return
			}
			br.Close()
			atomic.AddInt64(&completedQueries, int64(*batchSize))
			if atomic.LoadInt64(&completedQueries) >= int64(*numQueries) {
				return
			}
		}
	}

	// Start workers
	for i := 0; i < *poolSize; i++ {
		wg.Add(1)
		go worker()
	}

	wg.Wait()
	done <- true
	duration := time.Since(start)

	finalCompleted := atomic.LoadInt64(&completedQueries)
	rate := float64(finalCompleted) / duration.Seconds()
	fmt.Printf("Completed %d queries in %.2f seconds (%.2f queries/sec)\n", finalCompleted, duration.Seconds(), rate)
}

func reportProgress(start time.Time, completedQueries *int64, totalQueries int, done chan bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start).Seconds()
			completed := atomic.LoadInt64(completedQueries)
			rate := float64(completed) / elapsed
			fmt.Printf("Progress: %d/%d queries, %.2f queries/sec\n", completed, totalQueries, rate)
		case <-done:
			return
		}
	}
}
