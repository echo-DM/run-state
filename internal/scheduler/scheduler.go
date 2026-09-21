package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/dimen61/runstate/internal/store"
)

type Options struct {
	PollInterval time.Duration
	Logger       *slog.Logger
}

type Scheduler struct {
	store   *store.Store
	options Options
}

func New(database *store.Store, options Options) *Scheduler {
	if options.PollInterval <= 0 {
		options.PollInterval = time.Second
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Scheduler{store: database, options: options}
}

func (scheduler *Scheduler) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		for ctx.Err() == nil {
			woke, err := scheduler.store.WakeDueRetry(ctx)
			if err != nil {
				scheduler.options.Logger.Error("retry scan failed", "error", err)
				break
			}
			if !woke {
				break
			}
			scheduler.options.Logger.Info("retry ready")
		}
		wait(ctx, scheduler.options.PollInterval)
	}
	return nil
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
