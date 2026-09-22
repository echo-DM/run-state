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
)

func TestApprovalDecisionAndMatchingReplayPersistOnce(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{})
	created, stepID := createWaitingApproval(t, database)

	decision, err := database.DecideApproval(ctx, created.ID, stepID, "approved")
	if err != nil || decision.Decision != "approved" || decision.TaskStatus != task.StatusSucceeded {
		t.Fatalf("approve final step = %+v, %v", decision, err)
	}
	replay, err := database.DecideApproval(ctx, created.ID, stepID, "approved")
	if err != nil || replay.Decision != "approved" || replay.TaskStatus != task.StatusSucceeded {
		t.Fatalf("replay final approval = %+v, %v", replay, err)
	}
	if _, err := database.DecideApproval(ctx, created.ID, stepID, "rejected"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("opposite decision = %v, want conflict", err)
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusSucceeded || loaded.CurrentStep != 1 || loaded.Steps[0].Attempt != 0 || loaded.Steps[0].Status != task.StepSucceeded || loaded.Steps[0].ApprovalDecision == nil || *loaded.Steps[0].ApprovalDecision != "approved" {
		t.Fatalf("approved task = %+v, %v", loaded, err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_APPROVED": 1, "TASK_SUCCEEDED": 1})
}

func TestApprovalDecisionRollsBackWhenItsEventCannotBeWritten(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{})
	created, stepID := createWaitingApproval(t, database)
	installRejectingEventTrigger(t, pool, "TASK_APPROVED")
	if _, err := database.DecideApproval(ctx, created.ID, stepID, "approved"); err == nil {
		t.Fatal("approval succeeded while its event insert was rejected")
	}
	dropRejectingEventTrigger(t, pool)

	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusWaitingApproval || loaded.CurrentStep != 0 || loaded.Steps[0].Status != task.StepWaitingApproval || loaded.Steps[0].ApprovalDecision != nil {
		t.Fatalf("failed approval write partially changed state: %+v, %v", loaded, err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_APPROVED": 0, "TASK_SUCCEEDED": 0})
	if _, err := database.DecideApproval(ctx, created.ID, stepID, "approved"); err != nil {
		t.Fatalf("approve after event trigger removal: %v", err)
	}
}

func TestApprovalDecisionConfirmsCommitAfterResponseLoss(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{AfterCommit: func(operation string) error {
		if operation == "decide_approval" {
			return errors.New("injected lost commit response")
		}
		return nil
	}})
	created, stepID := createWaitingApproval(t, database)

	decision, err := database.DecideApproval(ctx, created.ID, stepID, "approved")
	if err != nil || decision.Decision != "approved" || decision.TaskStatus != task.StatusSucceeded {
		t.Fatalf("approval after lost commit response = %+v, %v", decision, err)
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusSucceeded || loaded.Steps[0].ApprovalDecision == nil || *loaded.Steps[0].ApprovalDecision != "approved" {
		t.Fatalf("approval after lost commit response was not persisted: %+v, %v", loaded, err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_APPROVED": 1, "TASK_SUCCEEDED": 1})
}

func TestApprovalWaitRollsBackLeaseReleaseWhenItsEventCannotBeWritten(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{})
	created, err := database.CreateTask(ctx, task.Definition{
		TaskTimeoutSeconds: 30,
		Steps: []task.StepDefinition{{
			Type: task.StepTypeApproval, Input: json.RawMessage(`{}`), TimeoutSeconds: 1, MaxAttempts: 1,
		}},
	})
	if err != nil {
		t.Fatalf("create approval task: %v", err)
	}
	claim, err := database.ClaimTask(ctx, "approval-wait-rollback")
	if err != nil {
		t.Fatalf("claim approval task: %v", err)
	}
	stepID := created.Steps[0].ID
	installRejectingEventTrigger(t, pool, "TASK_WAITING_APPROVAL")
	if err := database.WaitForApproval(ctx, claim.Token(), stepID); err == nil {
		t.Fatal("approval wait succeeded while its event insert was rejected")
	}
	dropRejectingEventTrigger(t, pool)
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusRunning || loaded.WorkerID == nil || *loaded.WorkerID != claim.WorkerID || loaded.Steps[0].Status != task.StepPending || loaded.Steps[0].Attempt != 0 || loaded.Steps[0].StartedAt != nil {
		t.Fatalf("failed approval wait partially released task: %+v, %v", loaded, err)
	}
	assertStoredEventCounts(t, pool, created.ID, map[string]int{"TASK_WAITING_APPROVAL": 0})
	if err := database.WaitForApproval(ctx, claim.Token(), stepID); err != nil {
		t.Fatalf("wait after event trigger removal: %v", err)
	}
}

func TestApprovalDecisionChecksDeadlineAfterWaitingForTaskRowLock(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{})
	created, stepID := createWaitingApproval(t, database)

	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin task row blocker: %v", err)
	}
	defer blocker.Rollback(ctx)
	var lockedID string
	if err := blocker.QueryRow(ctx, `SELECT id FROM tasks WHERE id = $1 FOR UPDATE`, created.ID).Scan(&lockedID); err != nil {
		t.Fatalf("lock approval task: %v", err)
	}
	var deadline time.Time
	if err := blocker.QueryRow(ctx, `UPDATE tasks SET deadline_at = clock_timestamp() + interval '750 milliseconds' WHERE id = $1 RETURNING deadline_at`, created.ID).Scan(&deadline); err != nil {
		t.Fatalf("set near-term approval deadline: %v", err)
	}
	type decisionResult struct {
		decision store.ApprovalResult
		err      error
	}
	resultCh := make(chan decisionResult, 1)
	go func() {
		decision, decideErr := database.DecideApproval(ctx, created.ID, stepID, "approved")
		resultCh <- decisionResult{decision: decision, err: decideErr}
	}()
	waitForTaskRowLockWait(t, testDatabase{pool: pool}, 2*time.Second)
	waitForTransactionDeadline(t, 3*time.Second, func() (bool, error) {
		var due bool
		err := blocker.QueryRow(ctx, `SELECT $1::timestamptz <= clock_timestamp()`, deadline).Scan(&due)
		return due, err
	})
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release task row blocker: %v", err)
	}
	select {
	case result := <-resultCh:
		if !errors.Is(result.err, store.ErrConflict) {
			t.Fatalf("approval after deadline returned %+v, %v; want conflict", result.decision, result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("approval decision remained blocked after releasing task row")
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil || loaded.Status != task.StatusWaitingApproval || loaded.Steps[0].ApprovalDecision != nil {
		t.Fatalf("late approval changed waiting task: %+v, %v", loaded, err)
	}
}

func createWaitingApproval(t *testing.T, database *store.Store) (task.Task, string) {
	t.Helper()
	ctx := context.Background()
	created, err := database.CreateTask(ctx, task.Definition{
		TaskTimeoutSeconds: 30,
		Steps: []task.StepDefinition{{
			Type: task.StepTypeApproval, Input: json.RawMessage(`{"reason":"review"}`), TimeoutSeconds: 1, MaxAttempts: 1,
		}},
	})
	if err != nil {
		t.Fatalf("create approval task: %v", err)
	}
	claim, err := database.ClaimTask(ctx, "approval-store-test")
	if err != nil {
		t.Fatalf("claim approval task: %v", err)
	}
	stepID := created.Steps[0].ID
	if err := database.WaitForApproval(ctx, claim.Token(), stepID); err != nil {
		t.Fatalf("wait for approval: %v", err)
	}
	loaded, err := database.LoadTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("load approval task: %v", err)
	}
	return loaded, stepID
}
