package tests_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

func TestStepsCommitInOrderAndDuplicateFinishDoesNotAdvance(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{
			{Type: "echo", Input: json.RawMessage(`{"value":"first"}`), TimeoutSeconds: 5, MaxAttempts: 3},
			{Type: "echo", Input: json.RawMessage(`{"value":"second"}`), TimeoutSeconds: 5, MaxAttempts: 3},
		},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim, err := database.ClaimTask(context.Background(), "worker-a")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}

	first, err := database.StartStep(context.Background(), claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start first step: %v", err)
	}
	if first.Attempt != 1 || string(first.ResolvedInput) != `{"value": "first"}` {
		t.Fatalf("first attempt = %+v", first)
	}
	firstOutcome := store.StepOutcome{Output: json.RawMessage(`{"echo":"first"}`), Checkpoint: json.RawMessage(`{"last":"first"}`)}
	if err := database.FinishStep(context.Background(), claim.Token(), first, firstOutcome); err != nil {
		t.Fatalf("finish first step: %v", err)
	}
	if err := database.FinishStep(context.Background(), claim.Token(), first, firstOutcome); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate finish error = %v, want conflict", err)
	}

	afterFirst, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load after first step: %v", err)
	}
	if afterFirst.Status != "RUNNING" || afterFirst.CurrentStep != 1 || afterFirst.Steps[0].Status != "SUCCEEDED" || afterFirst.Steps[0].Attempt != 1 || string(afterFirst.Checkpoint) != `{"last": "first"}` {
		t.Fatalf("after first step = %+v, steps=%+v", afterFirst, afterFirst.Steps)
	}

	second, err := database.StartStep(context.Background(), claim.Token(), created.Steps[1].ID)
	if err != nil {
		t.Fatalf("start second step: %v", err)
	}
	if err := database.FinishStep(context.Background(), claim.Token(), second, store.StepOutcome{Output: json.RawMessage(`{"echo":"second"}`)}); err != nil {
		t.Fatalf("finish second step: %v", err)
	}

	finished, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load finished task: %v", err)
	}
	if finished.Status != "SUCCEEDED" || finished.CurrentStep != 2 || finished.WorkerID != nil || finished.LeaseExpiresAt != nil || finished.Steps[1].Status != "SUCCEEDED" {
		t.Fatalf("finished task = %+v, steps=%+v", finished, finished.Steps)
	}
	events, err := database.LoadEvents(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["STEP_STARTED"] != 2 || counts["STEP_SUCCEEDED"] != 2 || counts["TASK_SUCCEEDED"] != 1 {
		t.Fatalf("event counts = %v", counts)
	}
}

func TestCommitResponseLossIsResolvedFromPersistedFacts(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	var loseStartResponse, loseFinishResponse atomic.Bool
	database := store.New(pool, store.Options{
		LeaseDuration: 5 * time.Second,
		AfterCommit: func(operation string) error {
			switch operation {
			case "start_step":
				if loseStartResponse.CompareAndSwap(false, true) {
					return errors.New("injected start commit response loss")
				}
			case "finish_step":
				if loseFinishResponse.CompareAndSwap(false, true) {
					return errors.New("injected finish commit response loss")
				}
			}
			return nil
		},
	})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"durable fact"}`), TimeoutSeconds: 5, MaxAttempts: 3,
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
		t.Fatalf("start must recover from lost commit response: %v", err)
	}
	outcome := store.StepOutcome{Output: json.RawMessage(`{"value":"durable fact"}`)}
	if err := database.FinishStep(context.Background(), claim.Token(), attempt, outcome); err != nil {
		t.Fatalf("finish must recover from lost commit response: %v", err)
	}
	finished, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load committed facts: %v", err)
	}
	if finished.Status != "SUCCEEDED" || finished.Steps[0].Attempt != 1 {
		t.Fatalf("facts after response loss = %+v, steps=%+v", finished, finished.Steps)
	}
	events, err := database.LoadEvents(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["STEP_STARTED"] != 1 || counts["STEP_SUCCEEDED"] != 1 || counts["TASK_SUCCEEDED"] != 1 {
		t.Fatalf("events after response loss = %v", counts)
	}
}

func TestOversizedOutputFailsTaskWithoutPersistingOutput(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"large"}`), TimeoutSeconds: 5, MaxAttempts: 3,
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
	oversized := append([]byte{'"'}, bytes.Repeat([]byte{'x'}, 1<<20)...)
	oversized = append(oversized, '"')
	if err := database.FinishStep(context.Background(), claim.Token(), attempt, store.StepOutcome{Output: oversized}); err != nil {
		t.Fatalf("record oversized output failure: %v", err)
	}
	failed, err := database.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load failed task: %v", err)
	}
	if failed.Status != "FAILED" || failed.Steps[0].Status != "FAILED" || failed.Steps[0].Output != nil || failed.Steps[0].Error == nil || *failed.Steps[0].Error != "output_too_large" {
		t.Fatalf("oversized output result = task %+v steps %+v", failed, failed.Steps)
	}
}

func TestStartStepResolvesCommittedInputs(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := database.CreateTask(context.Background(), task.Definition{
		TaskTimeoutSeconds: 60,
		Steps: []task.StepDefinition{
			{Type: "echo", Input: json.RawMessage(`{"value":"source"}`), TimeoutSeconds: 5, MaxAttempts: 3},
			{Type: "echo", Input: json.RawMessage(`{"value":{"previous":{"$ref":"previous_output"},"state":{"$ref":"checkpoint"}}}`), TimeoutSeconds: 5, MaxAttempts: 3},
		},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim, err := database.ClaimTask(context.Background(), "worker-a")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	first, err := database.StartStep(context.Background(), claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start first step: %v", err)
	}
	if err := database.FinishStep(context.Background(), claim.Token(), first, store.StepOutcome{
		Output: json.RawMessage(`{"value":"from-output"}`), Checkpoint: json.RawMessage(`{"token":"from-checkpoint"}`),
	}); err != nil {
		t.Fatalf("finish first step: %v", err)
	}
	second, err := database.StartStep(context.Background(), claim.Token(), created.Steps[1].ID)
	if err != nil {
		t.Fatalf("start resolved step: %v", err)
	}
	var got any
	if err := json.Unmarshal(second.ResolvedInput, &got); err != nil {
		t.Fatalf("decode resolved input: %v", err)
	}
	want := `{"value":{"previous":{"value":"from-output"},"state":{"token":"from-checkpoint"}}}`
	var expected any
	if err := json.Unmarshal([]byte(want), &expected); err != nil {
		t.Fatalf("decode expected input: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint(expected) {
		t.Fatalf("resolved input = %s, want %s", second.ResolvedInput, want)
	}
}
