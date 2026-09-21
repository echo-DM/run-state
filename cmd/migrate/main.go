package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/dimen61/runstate/internal/config"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, config.String("RUNSTATE_DATABASE_URL", config.DefaultDatabaseURL))
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := migrations.Run(ctx, pool); err != nil {
		logger.Error("migrate database", "error", err)
		os.Exit(1)
	}
	logger.Info("database migrations complete")
}
