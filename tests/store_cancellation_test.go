package tests_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

func TestCancelNonRunningControlStatesWithoutClaim(t *testing.T) {
	for _, status := range []string{task.StatusScheduled, task.StatusRetryWait, task.StatusWaitingApproval} {
		t.Run(status, func(t *testing.T) {
			pool := isolatedTestPool(t)
			ctx := context.Background()
			if err := migrations.Run(ctx, pool); err != nil {
				t.Fatalf("migrate test schema: %v", err)
			}
			database := store.New(pool, store.Options{})
			created, err := database.CreateTask(ctx, timeoutDefinition(30))
			if err != nil {
				t.Fatalf("create task: %v", err)
			}
			if _, err := pool.Exec(ctx, `UPDATE tasks SET status = $2 WHERE id = $1`, created.ID, status); err != nil {
				t.Fatalf("prepare %s task: %v", status, err)
			}
			result, err := database.CancelTask(ctx, created.ID)
			if err != nil || result != store.CancelledSynchronously {
				t.Fatalf("cancel %s = %v, %v", status, result, err)
			}
			loaded, err := database.LoadTask(ctx, created.ID)
			if err != nil || loaded.Status != task.StatusCancelled || loaded.Steps[0].Status != task.StepFailed {
				t.Fatalf("cancelled %s task = %+v, %v", status, loaded, err)
			}
		})
	}
}

func TestCancelAndSuccessRaceHasOneTerminalWinner(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(ctx, timeoutDefinition(30))
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim, err := database.ClaimTask(ctx, "cancel-race-worker")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	attempt, err := database.StartStep(ctx, claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start step: %v", err)
	}

	barrier := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-barrier
		_, cancelErr := database.CancelTask(ctx, created.ID)
		results <- cancelErr
	}()
	go func() {
		<-barrier
		results <- database.FinishStep(ctx, claim.Token(), attempt, store.StepOutcome{Output: []byte(`{"value":"done"}`)})
	}()
	close(barrier)
	first, second := <-results, <-results
	for _, result := range []error{first, second} {
		if result != nil && !errors.Is(result, store.ErrConflict) && !errors.Is(result, store.ErrCancelled) && !errors.Is(result, store.ErrLeaseLost) {
			t.Fatalf("race result = %v", result)
		}
	}
	if _, processErr := database.ProcessDueControl(ctx); processErr != nil {
		t.Fatalf("converge cancellation race: %v", processErr)
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || (loaded.Status != task.StatusSucceeded && loaded.Status != task.StatusCancelled) {
		t.Fatalf("terminal race result = %+v, %v", loaded, err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_SUCCEEDED": boolCount(loaded.Status == task.StatusSucceeded), "TASK_CANCELLED": boolCount(loaded.Status == task.StatusCancelled)})
}

func TestLostOwnerCannotPersistCancellation(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 40 * time.Millisecond})
	created, err := database.CreateTask(ctx, timeoutDefinition(30))
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	first, err := database.ClaimTask(ctx, "cancel-owner-a")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := database.ClaimTask(ctx, "cancel-owner-b")
	if err != nil {
		t.Fatalf("takeover claim: %v", err)
	}
	if _, err := database.CancelTask(ctx, created.ID); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	if err := database.CancelOwnedTask(ctx, first.Token()); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("lost owner cancellation = %v, want lease lost", err)
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusRunning || loaded.WorkerID == nil || *loaded.WorkerID != second.WorkerID {
		t.Fatalf("lost owner changed cancellation state: %+v, %v", loaded, err)
	}
	if err := database.CancelOwnedTask(ctx, second.Token()); err != nil {
		t.Fatalf("current owner cancellation: %v", err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_CANCELLED": 1})
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
