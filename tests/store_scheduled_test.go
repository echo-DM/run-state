package tests_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

func TestSchedulerWakesScheduledTaskOnceWhenRunAtIsDue(t *testing.T) {
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}

	var runAt time.Time
	if err := database.pool.QueryRow(ctx, `SELECT clock_timestamp() + interval '250 milliseconds'`).Scan(&runAt); err != nil {
		t.Fatalf("choose database-relative run_at: %v", err)
	}
	durableStore := store.New(database.pool, store.Options{})
	created, err := durableStore.CreateTask(ctx, task.Definition{
		RunAt:              &runAt,
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"scheduled"}`), TimeoutSeconds: 5, MaxAttempts: 3,
		}},
	})
	if err != nil {
		t.Fatalf("create scheduled task: %v", err)
	}
	if created.Status != task.StatusScheduled || !created.RunAt.Equal(runAt) {
		t.Fatalf("created task = status %s at %s, want SCHEDULED at %s", created.Status, created.RunAt, runAt)
	}
	if _, err := durableStore.ClaimTask(ctx, "early-worker"); err != store.ErrNoTask {
		t.Fatalf("claim before run_at = %v, want no task", err)
	}
	if processed, err := durableStore.ProcessDueControl(ctx); err != nil || processed {
		t.Fatalf("process before run_at = %v, %v; want false, nil", processed, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		processed, err := durableStore.ProcessDueControl(ctx)
		if err != nil {
			t.Fatalf("process due task: %v", err)
		}
		if processed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ready, err := durableStore.LoadTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("load scheduled task: %v", err)
	}
	if ready.Status != task.StatusRunnable || ready.RetryAt != nil || ready.LeaseVersion != 0 || ready.FirstStartedAt != nil || ready.DeadlineAt != nil {
		t.Fatalf("woken task = %+v, want RUNNABLE without claim or deadline", ready)
	}
	if ready.Steps[0].Status != task.StepPending || ready.Steps[0].Attempt != 0 {
		t.Fatalf("woken step = %+v, want pending without an attempt", ready.Steps[0])
	}
	if processed, err := durableStore.ProcessDueControl(ctx); err != nil || processed {
		t.Fatalf("process after wake = %v, %v; want false, nil", processed, err)
	}
	events, err := durableStore.LoadEvents(ctx, created.ID)
	if err != nil {
		t.Fatalf("load scheduled events: %v", err)
	}
	readyEvents := 0
	for _, event := range events {
		if event.Type == "TASK_RUNNABLE" {
			readyEvents++
		}
	}
	if readyEvents != 1 {
		t.Fatalf("scheduled wake events = %d, want 1", readyEvents)
	}
}

func TestScheduledWakeRollsBackWhenReadyEventCannotBeWritten(t *testing.T) {
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	var runAt time.Time
	if err := database.pool.QueryRow(ctx, `SELECT clock_timestamp() + interval '150 milliseconds'`).Scan(&runAt); err != nil {
		t.Fatalf("choose database-relative run_at: %v", err)
	}
	durableStore := store.New(database.pool, store.Options{})
	created, err := durableStore.CreateTask(ctx, task.Definition{
		RunAt:              &runAt,
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"atomic wake"}`), TimeoutSeconds: 5, MaxAttempts: 3,
		}},
	})
	if err != nil {
		t.Fatalf("create scheduled task: %v", err)
	}
	waitForDatabaseCondition(t, database, created.ID, 2*time.Second, `clock_timestamp() >= run_at`)
	installRejectingEventTrigger(t, database.pool, "TASK_RUNNABLE")
	if _, err := durableStore.ProcessDueControl(ctx); err == nil {
		t.Fatal("scheduled wake succeeded while TASK_RUNNABLE event was rejected")
	}
	stillScheduled, err := durableStore.LoadTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("load rolled-back scheduled task: %v", err)
	}
	if stillScheduled.Status != task.StatusScheduled || stillScheduled.Steps[0].Attempt != 0 {
		t.Fatalf("scheduled state escaped event rollback: %+v", stillScheduled)
	}
	events, err := durableStore.LoadEvents(ctx, created.ID)
	if err != nil {
		t.Fatalf("load rolled-back events: %v", err)
	}
	for _, event := range events {
		if event.Type == "TASK_RUNNABLE" {
			t.Fatalf("ready event escaped rollback: %+v", event)
		}
	}

	dropRejectingEventTrigger(t, database.pool)
	if processed, err := durableStore.ProcessDueControl(ctx); err != nil || !processed {
		t.Fatalf("scheduled wake after removing trigger = %v, %v; want true, nil", processed, err)
	}
}
