package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dimen61/runstate/internal/config"
	"github.com/dimen61/runstate/internal/scheduler"
	"github.com/dimen61/runstate/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	pollInterval, err := config.Duration("RUNSTATE_SCHEDULER_POLL_INTERVAL", time.Second)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
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
	process := scheduler.New(store.New(pool, store.Options{}), scheduler.Options{PollInterval: pollInterval, Logger: logger})
	if err := process.Run(ctx); err != nil {
		logger.Error("scheduler stopped", "error", err)
		os.Exit(1)
	}
}
