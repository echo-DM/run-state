package tests_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTaskDeadlineSurvivesLeaseTakeover(t *testing.T) {
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
	first, err := database.ClaimTask(ctx, "timeout-owner-a")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := database.ClaimTask(ctx, "timeout-owner-b")
	if err != nil {
		t.Fatalf("takeover claim: %v", err)
	}
	if !second.DeadlineAt.Equal(first.DeadlineAt) {
		t.Fatalf("takeover deadline = %s, want %s", second.DeadlineAt, first.DeadlineAt)
	}
	if err := database.TimeoutOwnedTask(ctx, first.Token()); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("old owner timeout = %v, want lease lost", err)
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusRunning || loaded.LeaseVersion != second.LeaseVersion {
		t.Fatalf("old owner changed task: %+v, %v", loaded, err)
	}
}

func TestSchedulerCancellationPrecedesDeadlineAndRetry(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `
		UPDATE tasks SET status = 'RETRY_WAIT', retry_at = clock_timestamp() - interval '1 second',
		deadline_at = clock_timestamp() - interval '1 second', first_started_at = clock_timestamp() - interval '31 seconds',
		cancel_requested = true WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("prepare competing controls: %v", err)
	}
	processed, err := database.ProcessDueControl(ctx)
	if err != nil || !processed {
		t.Fatalf("process controls = %v, %v", processed, err)
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusCancelled || loaded.Steps[0].Error == nil || *loaded.Steps[0].Error != "user_cancelled" {
		t.Fatalf("control priority result = %+v, %v", loaded, err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_CANCELLED": 1, "TASK_TIMED_OUT": 0, "TASK_RETRY_READY": 0})
}

func TestConcurrentSchedulersAndLateSuccessProduceOneTimeout(t *testing.T) {
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
	claim, err := database.ClaimTask(ctx, "deadline-race-worker")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	attempt, err := database.StartStep(ctx, claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start step: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET deadline_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("expire deadline: %v", err)
	}

	start := make(chan struct{})
	errorsSeen := make(chan error, 3)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, processErr := database.ProcessDueControl(ctx)
			errorsSeen <- processErr
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		<-start
		errorsSeen <- database.FinishStep(ctx, claim.Token(), attempt, store.StepOutcome{Output: json.RawMessage(`{"value":"too late"}`)})
	}()
	close(start)
	group.Wait()
	close(errorsSeen)
	for result := range errorsSeen {
		if result != nil && !errors.Is(result, store.ErrDeadlineExceeded) && !errors.Is(result, store.ErrLeaseLost) {
			t.Fatalf("race result: %v", result)
		}
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusTimedOut || loaded.Steps[0].Status != task.StepFailed || loaded.Steps[0].Error == nil || *loaded.Steps[0].Error != "task_timeout" {
		t.Fatalf("deadline race result = %+v, %v", loaded, err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_TIMED_OUT": 1, "TASK_SUCCEEDED": 0, "STEP_SUCCEEDED": 0})
}

func timeoutDefinition(seconds int) task.Definition {
	return task.Definition{
		TaskTimeoutSeconds: seconds,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"deadline"}`), TimeoutSeconds: 5, MaxAttempts: 3,
		}},
	}
}

func assertStoredEventCounts(t *testing.T, pool *pgxpool.Pool, taskID string, want map[string]int) {
	t.Helper()
	for eventType, expected := range want {
		var got int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM task_events WHERE task_id = $1 AND event_type = $2`, taskID, eventType).Scan(&got); err != nil {
			t.Fatalf("count %s events: %v", eventType, err)
		}
		if got != expected {
			t.Fatalf("%s event count = %d, want %d", eventType, got, expected)
		}
	}
}
