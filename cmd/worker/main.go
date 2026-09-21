package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dimen61/runstate/internal/config"
	"github.com/dimen61/runstate/internal/store"
	runworker "github.com/dimen61/runstate/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	workerID := flag.String("id", config.String("RUNSTATE_WORKER_ID", ""), "stable worker identifier")
	concurrencyDefault, err := config.Int("RUNSTATE_WORKER_CONCURRENCY", 1)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	concurrency := flag.Int("concurrency", concurrencyDefault, "number of execution slots")
	flag.Parse()
	if *workerID == "" || *concurrency <= 0 {
		logger.Error("--id and a positive --concurrency are required")
		os.Exit(2)
	}
	leaseDuration := mustDuration(logger, "RUNSTATE_LEASE_DURATION", 30*time.Second)
	heartbeatInterval := mustDuration(logger, "RUNSTATE_HEARTBEAT_INTERVAL", 10*time.Second)
	pollInterval := mustDuration(logger, "RUNSTATE_POLL_INTERVAL", time.Second)
	if heartbeatInterval >= leaseDuration {
		logger.Error("heartbeat interval must be shorter than lease duration")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, config.String("RUNSTATE_DATABASE_URL", config.DefaultDatabaseURL))
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		logger.Error("ping database", "error", err)
		os.Exit(1)
	}
	database := store.New(pool, store.Options{LeaseDuration: leaseDuration})
	process := runworker.New(database, runworker.Options{
		WorkerID: *workerID, Concurrency: *concurrency, PollInterval: pollInterval,
		HeartbeatInterval: heartbeatInterval, Logger: logger,
	})
	if err := process.Run(ctx); err != nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func mustDuration(logger *slog.Logger, name string, fallback time.Duration) time.Duration {
	value, err := config.Duration(name, fallback)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	return value
}
