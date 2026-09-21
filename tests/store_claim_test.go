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
)

func TestConcurrentClaimHasOneOwner(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"one owner"}`), TimeoutSeconds: 5, MaxAttempts: 3,
		}},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	start := make(chan struct{})
	type result struct {
		claim store.Claim
		err   error
	}
	results := make(chan result, 2)
	var workers sync.WaitGroup
	for _, workerID := range []string{"worker-a", "worker-b"} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			claim, err := database.ClaimTask(context.Background(), workerID)
			results <- result{claim: claim, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	var winner result
	successes, empty := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			winner = result
		case errors.Is(result.err, store.ErrNoTask):
			empty++
		default:
			t.Fatalf("claim returned unexpected error: %v", result.err)
		}
	}
	if successes != 1 || empty != 1 {
		t.Fatalf("claim results: successes=%d no-task=%d, want 1 and 1", successes, empty)
	}
	if winner.claim.TaskID != created.ID || winner.claim.LeaseVersion != 1 || winner.claim.WorkerID == "" {
		t.Fatalf("winning claim = %+v", winner.claim)
	}

	loaded, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load claimed task: %v", err)
	}
	if loaded.Status != "RUNNING" || loaded.WorkerID == nil || *loaded.WorkerID != winner.claim.WorkerID || loaded.LeaseVersion != winner.claim.LeaseVersion {
		t.Fatalf("persisted ownership = status %s worker %v version %d", loaded.Status, loaded.WorkerID, loaded.LeaseVersion)
	}
	events, err := database.LoadEvents(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	claimedEvents := 0
	for _, event := range events {
		if event.Type == "TASK_CLAIMED" {
			claimedEvents++
		}
	}
	if claimedEvents != 1 {
		t.Fatalf("TASK_CLAIMED events = %d, want 1", claimedEvents)
	}
}
