package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dimen61/runstate/internal/executor"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

type Options struct {
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	Logger            *slog.Logger
	FakeToolURL       string
}

type Worker struct {
	store   *store.Store
	options Options
}

func New(database *store.Store, options Options) *Worker {
	if options.Concurrency <= 0 {
		options.Concurrency = 1
	}
	if options.PollInterval <= 0 {
		options.PollInterval = time.Second
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = 10 * time.Second
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Worker{store: database, options: options}
}

func (worker *Worker) Run(ctx context.Context) error {
	if worker.options.WorkerID == "" {
		return errors.New("worker id is required")
	}
	var slots sync.WaitGroup
	for range worker.options.Concurrency {
		slots.Add(1)
		go func() {
			defer slots.Done()
			worker.runSlot(ctx)
		}()
	}
	<-ctx.Done()
	slots.Wait()
	return nil
}

func (worker *Worker) runSlot(ctx context.Context) {
	for ctx.Err() == nil {
		claim, err := worker.store.ClaimTask(ctx, worker.options.WorkerID)
		switch {
		case err == nil:
			worker.options.Logger.Info("task claimed", "task", claim.TaskID, "worker", claim.WorkerID, "lease_version", claim.LeaseVersion)
			worker.executeClaim(ctx, claim)
		case errors.Is(err, store.ErrNoTask):
			wait(ctx, worker.options.PollInterval)
		case ctx.Err() != nil:
			return
		default:
			worker.options.Logger.Error("claim failed", "worker", worker.options.WorkerID, "error", err)
			wait(ctx, worker.options.PollInterval)
		}
	}
}

func (worker *Worker) executeClaim(parent context.Context, claim store.Claim) {
	taskContext, cancelTask := context.WithDeadline(parent, claim.DeadlineAt)
	heartbeatDone := make(chan error, 1)
	go worker.heartbeat(taskContext, claim.Token(), cancelTask, heartbeatDone)
	defer func() {
		cancelTask()
		<-heartbeatDone
	}()

	for taskContext.Err() == nil {
		loaded, err := worker.store.LoadTask(taskContext, claim.TaskID)
		if err != nil {
			worker.options.Logger.Error("load claimed task failed", "task", claim.TaskID, "worker", claim.WorkerID, "lease_version", claim.LeaseVersion, "error", err)
			return
		}
		if loaded.Status != task.StatusRunning || loaded.CurrentStep >= len(loaded.Steps) {
			return
		}
		step := loaded.Steps[loaded.CurrentStep]
		attempt, err := worker.store.StartStep(taskContext, claim.Token(), step.ID)
		if err != nil {
			worker.options.Logger.Warn("start step rejected", "task", claim.TaskID, "step", step.ID, "worker", claim.WorkerID, "lease_version", claim.LeaseVersion, "error", err)
			return
		}
		worker.options.Logger.Info("step started", "task", claim.TaskID, "step", step.ID, "worker", claim.WorkerID, "lease_version", claim.LeaseVersion, "attempt", attempt.Attempt)

		stepContext, cancelStep := context.WithTimeout(taskContext, time.Duration(attempt.TimeoutSeconds)*time.Second)
		output, checkpoint, executeErr := executor.Execute(stepContext, attempt, executor.Options{FakeToolURL: worker.options.FakeToolURL})
		cancelStep()
		if errors.Is(taskContext.Err(), context.DeadlineExceeded) {
			worker.timeoutClaim(claim)
			return
		}
		if taskContext.Err() != nil {
			return
		}
		outcome := store.StepOutcome{Output: output, Checkpoint: checkpoint}
		if executeErr != nil {
			errorText := executeErr.Error()
			if errors.Is(executeErr, context.DeadlineExceeded) {
				errorText = "step_timeout"
			}
			outcome = store.StepOutcome{Error: errorText, FailureClass: executor.Classify(executeErr)}
		}
		finishContext, cancelFinish := context.WithTimeout(context.WithoutCancel(taskContext), 2*time.Second)
		err = worker.store.FinishStep(finishContext, claim.Token(), attempt, outcome)
		cancelFinish()
		if errors.Is(err, store.ErrDeadlineExceeded) {
			worker.timeoutClaim(claim)
			return
		}
		if err != nil {
			worker.options.Logger.Warn("finish step rejected", "task", claim.TaskID, "step", step.ID, "worker", claim.WorkerID, "lease_version", claim.LeaseVersion, "attempt", attempt.Attempt, "error", err)
			return
		}
		worker.options.Logger.Info("step finished", "task", claim.TaskID, "step", step.ID, "worker", claim.WorkerID, "lease_version", claim.LeaseVersion, "attempt", attempt.Attempt, "failed", executeErr != nil)
		if executeErr != nil {
			return
		}
	}
}

func (worker *Worker) timeoutClaim(claim store.Claim) {
	controlContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := worker.store.TimeoutOwnedTask(controlContext, claim.Token()); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		worker.options.Logger.Warn("task timeout finalization rejected", "task", claim.TaskID, "worker", claim.WorkerID, "lease_version", claim.LeaseVersion, "error", err)
	}
}

func (worker *Worker) heartbeat(ctx context.Context, lease store.LeaseToken, cancel context.CancelFunc, done chan<- error) {
	ticker := time.NewTicker(worker.options.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := worker.store.RenewLease(ctx, lease); err != nil {
				worker.options.Logger.Warn("heartbeat stopped", "task", lease.TaskID, "worker", lease.WorkerID, "lease_version", lease.LeaseVersion, "error", err)
				cancel()
				done <- err
				return
			}
		}
	}
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
