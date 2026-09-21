package tests_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

func TestTemporaryFailureWaitsDurablyAndSchedulerWakesSameStep(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"retry"}`), TimeoutSeconds: 5, MaxAttempts: 3,
		}},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim, err := database.ClaimTask(context.Background(), "worker-a")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	attempt, err := database.StartStep(context.Background(), claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start step: %v", err)
	}
	beforeFailure := time.Now()
	if err := database.FinishStep(context.Background(), claim.Token(), attempt, store.StepOutcome{
		Error: "upstream_unavailable", FailureClass: store.FailureTemporary,
	}); err != nil {
		t.Fatalf("record temporary failure: %v", err)
	}

	waiting, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load waiting task: %v", err)
	}
	if waiting.Status != task.StatusRetryWait || waiting.RetryAt == nil || waiting.WorkerID != nil || waiting.LeaseExpiresAt != nil {
		t.Fatalf("waiting task = %+v", waiting)
	}
	if waiting.Steps[0].Status != task.StepFailed || waiting.Steps[0].Attempt != 1 || waiting.Steps[0].Error == nil || *waiting.Steps[0].Error != "upstream_unavailable" {
		t.Fatalf("waiting step = %+v", waiting.Steps[0])
	}
	if waiting.RetryAt.Before(beforeFailure.Add(900*time.Millisecond)) || waiting.RetryAt.After(beforeFailure.Add(2*time.Second)) {
		t.Fatalf("retry_at = %v, want about one second after failure", waiting.RetryAt)
	}
	if _, err := database.ClaimTask(context.Background(), "worker-b"); !errors.Is(err, store.ErrNoTask) {
		t.Fatalf("claim during retry wait = %v, want no task", err)
	}
	if woke, err := database.WakeDueRetry(context.Background()); err != nil || woke {
		t.Fatalf("early wake = %v, %v; want false, nil", woke, err)
	}

	if _, err := pool.Exec(context.Background(), `UPDATE tasks SET retry_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	woke, err := database.WakeDueRetry(context.Background())
	if err != nil || !woke {
		t.Fatalf("wake due retry = %v, %v; want true, nil", woke, err)
	}
	runnable, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load runnable task: %v", err)
	}
	if runnable.Status != task.StatusRunnable || runnable.RetryAt != nil || runnable.CurrentStep != 0 || runnable.Steps[0].Attempt != 1 || runnable.Steps[0].Status != task.StepFailed {
		t.Fatalf("runnable task = %+v, steps=%+v", runnable, runnable.Steps)
	}

	events, err := database.LoadEvents(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["STEP_FAILED"] != 1 || counts["TASK_RETRY_WAIT"] != 1 || counts["TASK_RETRY_READY"] != 1 || counts["TASK_FAILED"] != 0 {
		t.Fatalf("retry event counts = %v", counts)
	}
}

func TestConcurrentSchedulersProduceOneRetryTransition(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps:              []task.StepDefinition{{Type: "echo", Input: json.RawMessage(`{"value":"retry"}`), TimeoutSeconds: 5, MaxAttempts: 3}},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim, err := database.ClaimTask(context.Background(), "worker-a")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	attempt, err := database.StartStep(context.Background(), claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start step: %v", err)
	}
	if err := database.FinishStep(context.Background(), claim.Token(), attempt, store.StepOutcome{Error: "temporary", FailureClass: store.FailureTemporary}); err != nil {
		t.Fatalf("record temporary failure: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE tasks SET retry_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("make retry due: %v", err)
	}

	var transitions atomic.Int32
	var failures atomic.Int32
	start := make(chan struct{})
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			woke, err := database.WakeDueRetry(context.Background())
			if err != nil {
				failures.Add(1)
			}
			if woke {
				transitions.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()
	if failures.Load() != 0 || transitions.Load() != 1 {
		t.Fatalf("scheduler results: failures=%d transitions=%d", failures.Load(), transitions.Load())
	}
	events, err := database.LoadEvents(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	readyEvents := 0
	for _, event := range events {
		if event.Type == "TASK_RETRY_READY" {
			readyEvents++
		}
	}
	if readyEvents != 1 {
		t.Fatalf("retry-ready events = %d, want 1", readyEvents)
	}
}

func TestRetryTransitionsRollBackWithTheirEvents(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(ctx, task.Definition{
		TaskTimeoutSeconds: 60,
		Steps:              []task.StepDefinition{{Type: "echo", Input: json.RawMessage(`{"value":"atomic"}`), TimeoutSeconds: 5, MaxAttempts: 3}},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim, err := database.ClaimTask(ctx, "worker-a")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	attempt, err := database.StartStep(ctx, claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start step: %v", err)
	}
	installRejectingEventTrigger(t, pool, "TASK_RETRY_WAIT")
	outcome := store.StepOutcome{Error: "temporary", FailureClass: store.FailureTemporary}
	if err := database.FinishStep(ctx, claim.Token(), attempt, outcome); err == nil {
		t.Fatal("temporary finish succeeded while retry event was rejected")
	}
	running, err := database.LoadTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("load rolled back running task: %v", err)
	}
	if running.Status != task.StatusRunning || running.Steps[0].Status != task.StepRunning || running.RetryAt != nil {
		t.Fatalf("temporary failure escaped rollback: %+v steps=%+v", running, running.Steps)
	}
	dropRejectingEventTrigger(t, pool)
	if err := database.FinishStep(ctx, claim.Token(), attempt, outcome); err != nil {
		t.Fatalf("record temporary failure after trigger removal: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET retry_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	installRejectingEventTrigger(t, pool, "TASK_RETRY_READY")
	if _, err := database.WakeDueRetry(ctx); err == nil {
		t.Fatal("retry wake succeeded while ready event was rejected")
	}
	waiting, err := database.LoadTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("load rolled back waiting task: %v", err)
	}
	if waiting.Status != task.StatusRetryWait || waiting.RetryAt == nil {
		t.Fatalf("retry wake escaped rollback: %+v", waiting)
	}
}

func TestRetryWakeSkipsCancelledExpiredAndTerminalTasks(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{})
	for _, state := range []string{
		`status = 'RETRY_WAIT', cancel_requested = true`,
		`status = 'RETRY_WAIT', deadline_at = clock_timestamp() - interval '1 second'`,
		`status = 'FAILED'`,
	} {
		created, err := database.CreateTask(ctx, task.Definition{
			TaskTimeoutSeconds: 60,
			Steps:              []task.StepDefinition{{Type: "echo", Input: json.RawMessage(`{"value":"skip"}`), TimeoutSeconds: 5, MaxAttempts: 3}},
		})
		if err != nil {
			t.Fatalf("create task: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE tasks SET worker_id = NULL, lease_expires_at = NULL, retry_at = clock_timestamp() - interval '1 second', `+state+` WHERE id = $1`, created.ID); err != nil {
			t.Fatalf("prepare ineligible retry: %v", err)
		}
	}
	woke, err := database.WakeDueRetry(ctx)
	if err != nil || woke {
		t.Fatalf("ineligible retry wake = %v, %v; want false, nil", woke, err)
	}
}

func TestRetryBackoffDoublesAndCapsAtSixtySeconds(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(ctx, retryDefinition(8))
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	for index, wantSeconds := range []int{1, 2, 4, 8, 16, 32, 60} {
		claim, err := database.ClaimTask(ctx, "backoff-worker")
		if err != nil {
			t.Fatalf("claim attempt %d: %v", index+1, err)
		}
		attempt, err := database.StartStep(ctx, claim.Token(), created.Steps[0].ID)
		if err != nil {
			t.Fatalf("start attempt %d: %v", index+1, err)
		}
		before := time.Now()
		if err := database.FinishStep(ctx, claim.Token(), attempt, store.StepOutcome{Error: "temporary", FailureClass: store.FailureTemporary}); err != nil {
			t.Fatalf("fail attempt %d: %v", index+1, err)
		}
		waiting, err := database.LoadTask(ctx, created.ID)
		if err != nil {
			t.Fatalf("load attempt %d: %v", index+1, err)
		}
		if waiting.RetryAt == nil {
			t.Fatalf("attempt %d has no retry_at", index+1)
		}
		delay := waiting.RetryAt.Sub(before)
		want := time.Duration(wantSeconds) * time.Second
		if delay < want-100*time.Millisecond || delay > want+time.Second {
			t.Fatalf("attempt %d delay = %v, want about %v", index+1, delay, want)
		}
		if _, err := pool.Exec(ctx, `UPDATE tasks SET retry_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`, created.ID); err != nil {
			t.Fatalf("make attempt %d due: %v", index+1, err)
		}
		if woke, err := database.WakeDueRetry(ctx); err != nil || !woke {
			t.Fatalf("wake attempt %d = %v, %v", index+1, woke, err)
		}
	}
}
