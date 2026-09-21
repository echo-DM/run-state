package tests_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestExpiredRunningAttemptIsRecoveredAndOldWorkerIsFenced(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 150 * time.Millisecond})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{
			{Type: "echo", Input: json.RawMessage(`{"value":"committed"}`), TimeoutSeconds: 5, MaxAttempts: 3},
			{Type: "echo", Input: json.RawMessage(`{"value":"recover me"}`), TimeoutSeconds: 5, MaxAttempts: 3},
		},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claimA, err := database.ClaimTask(context.Background(), "worker-a")
	if err != nil {
		t.Fatalf("worker A claim: %v", err)
	}
	first, err := database.StartStep(context.Background(), claimA.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start committed step: %v", err)
	}
	if err := database.FinishStep(context.Background(), claimA.Token(), first, store.StepOutcome{Output: json.RawMessage(`{"value":"committed"}`)}); err != nil {
		t.Fatalf("finish committed step: %v", err)
	}
	abandoned, err := database.StartStep(context.Background(), claimA.Token(), created.Steps[1].ID)
	if err != nil {
		t.Fatalf("start abandoned step: %v", err)
	}

	claimB := waitForClaim(t, database, "worker-b", time.Second)
	if claimB.LeaseVersion != claimA.LeaseVersion+1 {
		t.Fatalf("replacement lease version = %d, want %d", claimB.LeaseVersion, claimA.LeaseVersion+1)
	}
	if err := database.FinishStep(context.Background(), claimA.Token(), abandoned, store.StepOutcome{Output: json.RawMessage(`{"worker":"a"}`)}); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("old worker finish error = %v, want lease lost", err)
	}
	recovered, err := database.StartStep(context.Background(), claimB.Token(), created.Steps[1].ID)
	if err != nil {
		t.Fatalf("start recovered step: %v", err)
	}
	if recovered.Attempt != 2 || string(recovered.ResolvedInput) != string(abandoned.ResolvedInput) || recovered.IdempotencyKey != abandoned.IdempotencyKey {
		t.Fatalf("recovered attempt = %+v, abandoned = %+v", recovered, abandoned)
	}
	if err := database.FinishStep(context.Background(), claimB.Token(), recovered, store.StepOutcome{Output: json.RawMessage(`{"worker":"b"}`)}); err != nil {
		t.Fatalf("finish recovered step: %v", err)
	}

	finished, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load recovered task: %v", err)
	}
	if finished.Status != "SUCCEEDED" || finished.Steps[0].Attempt != 1 || finished.Steps[1].Attempt != 2 || string(finished.Steps[1].Output) != `{"worker": "b"}` {
		t.Fatalf("recovered task = %+v, steps=%+v", finished, finished.Steps)
	}
	events, err := database.LoadEvents(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load recovery events: %v", err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["LEASE_EXPIRED"] != 1 || counts["TASK_RECOVERED"] != 1 || counts["STEP_FAILED"] != 1 || counts["TASK_CLAIMED"] != 2 {
		t.Fatalf("recovery event counts = %v", counts)
	}
}

func TestClaimBeforeStartDoesNotConsumeAttempt(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 100 * time.Millisecond})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"not started"}`), TimeoutSeconds: 5, MaxAttempts: 3,
		}},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claimA, err := database.ClaimTask(context.Background(), "worker-a")
	if err != nil {
		t.Fatalf("worker A claim: %v", err)
	}
	claimB := waitForClaim(t, database, "worker-b", time.Second)
	if err := database.RenewLease(context.Background(), claimA.Token()); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("old lease renewal error = %v, want lease lost", err)
	}
	beforeStart, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load reclaimed task: %v", err)
	}
	if beforeStart.Steps[0].Attempt != 0 || beforeStart.Steps[0].Status != "PENDING" {
		t.Fatalf("unstarted step = %+v, want pending attempt 0", beforeStart.Steps[0])
	}
	attempt, err := database.StartStep(context.Background(), claimB.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start after reclaim: %v", err)
	}
	if attempt.Attempt != 1 {
		t.Fatalf("attempt after reclaim = %d, want 1", attempt.Attempt)
	}
}

func TestRepeatedCrashesExhaustAttemptBudget(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 100 * time.Millisecond})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"crash twice"}`), TimeoutSeconds: 5, MaxAttempts: 2,
		}},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claimA, err := database.ClaimTask(context.Background(), "worker-a")
	if err != nil {
		t.Fatalf("worker A claim: %v", err)
	}
	if _, err := database.StartStep(context.Background(), claimA.Token(), created.Steps[0].ID); err != nil {
		t.Fatalf("worker A start: %v", err)
	}
	claimB := waitForClaim(t, database, "worker-b", time.Second)
	if _, err := database.StartStep(context.Background(), claimB.Token(), created.Steps[0].ID); err != nil {
		t.Fatalf("worker B start: %v", err)
	}
	waitForLeaseExpiry(t, pool, created.ID, time.Second)
	if _, err := database.ClaimTask(context.Background(), "worker-c"); !errors.Is(err, store.ErrNoTask) {
		t.Fatalf("worker C claim error = %v, want no task after terminalizing exhausted workflow", err)
	}
	failed, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load exhausted task: %v", err)
	}
	if failed.Status != "FAILED" || failed.WorkerID != nil || failed.LeaseExpiresAt != nil || failed.Steps[0].Status != "FAILED" || failed.Steps[0].Attempt != 2 || failed.Steps[0].Error == nil || *failed.Steps[0].Error != "worker_lost" {
		t.Fatalf("exhausted task = %+v, steps=%+v", failed, failed.Steps)
	}
}

func waitForClaim(t *testing.T, database *store.Store, workerID string, within time.Duration) store.Claim {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		claim, err := database.ClaimTask(context.Background(), workerID)
		if err == nil {
			return claim
		}
		if !errors.Is(err, store.ErrNoTask) {
			t.Fatalf("claim while waiting for expiry: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lease did not become claimable before deadline")
	return store.Claim{}
}

func waitForLeaseExpiry(t *testing.T, pool *pgxpool.Pool, taskID string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var expired bool
		if err := pool.QueryRow(context.Background(), `SELECT lease_expires_at <= clock_timestamp() FROM tasks WHERE id = $1`, taskID).Scan(&expired); err != nil {
			t.Fatalf("read lease expiry: %v", err)
		}
		if expired {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lease did not expire before deadline")
}
