package tests_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
)

func TestCompiledWorkerAndRestartedSchedulersRunScheduledTaskOnce(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if workerBinary == "" || schedulerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY and RUNSTATE_SCHEDULER_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)

	var runAt time.Time
	if err := database.pool.QueryRow(ctx, `SELECT clock_timestamp() + interval '2500 milliseconds'`).Scan(&runAt); err != nil {
		t.Fatalf("choose database-relative run_at: %v", err)
	}
	taskID := createScheduledHTTPTask(t, server.URL, runAt)
	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "worker-scheduled")
	t.Cleanup(func() { stopProcess(t, worker) })
	schedulerA, schedulerALogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, schedulerA) })

	queued := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool {
		return view.Status == "SCHEDULED"
	})
	if queued.LeaseVersion != 0 || queued.FirstStartedAt != nil || queued.DeadlineAt != nil || queued.Steps[0].Attempt != 0 {
		t.Fatalf("worker claimed future task early: %+v; worker logs=%s", queued, workerLogs)
	}
	waitForDatabaseCondition(t, database, taskID, 4*time.Second, `clock_timestamp() >= run_at - interval '150 milliseconds'`)
	stopProcess(t, schedulerA)
	waitForDatabaseCondition(t, database, taskID, 2*time.Second, `clock_timestamp() >= run_at`)
	whileStopped := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool {
		return view.Status == "SCHEDULED"
	})
	if whileStopped.Steps[0].Attempt != 0 || whileStopped.FirstStartedAt != nil || whileStopped.DeadlineAt != nil {
		t.Fatalf("task changed while scheduler was stopped: %+v", whileStopped)
	}

	schedulerB, schedulerBLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	schedulerC, schedulerCLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, schedulerB) })
	t.Cleanup(func() { stopProcess(t, schedulerC) })
	finished := waitForTask(t, server.URL, taskID, 6*time.Second, func(view taskView) bool {
		return view.Status == "SUCCEEDED"
	})
	stopProcess(t, schedulerB)
	stopProcess(t, schedulerC)
	if finished.LeaseVersion != 1 || finished.CurrentStep != 1 || finished.Steps[0].Attempt != 1 || finished.FirstStartedAt == nil || finished.DeadlineAt == nil {
		t.Fatalf("scheduled execution result = %+v; worker logs=%s; schedulers=%s/%s/%s", finished, workerLogs, schedulerALogs, schedulerBLogs, schedulerCLogs)
	}
	if finished.RunAt.Before(runAt.Add(-time.Microsecond)) || finished.RunAt.After(runAt.Add(time.Microsecond)) {
		t.Fatalf("persisted run_at = %s, want %s", finished.RunAt, runAt)
	}
	assertScheduledEventCounts(t, server.URL, taskID, 1, 0)
}

func TestHTTPCancellationOfScheduledTaskPreventsLaterWake(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if workerBinary == "" || schedulerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY and RUNSTATE_SCHEDULER_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)

	var runAt time.Time
	if err := database.pool.QueryRow(ctx, `SELECT clock_timestamp() + interval '2500 milliseconds'`).Scan(&runAt); err != nil {
		t.Fatalf("choose database-relative run_at: %v", err)
	}
	taskID := createScheduledHTTPTask(t, server.URL, runAt)
	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "worker-scheduled-cancel")
	t.Cleanup(func() { stopProcess(t, worker) })
	scheduler, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, scheduler) })

	response, err := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatalf("cancel scheduled task: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	cancelled := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool {
		return view.Status == "CANCELLED"
	})
	if cancelled.Steps[0].Status != "FAILED" || cancelled.Steps[0].Attempt != 0 || cancelled.Steps[0].Error == nil || *cancelled.Steps[0].Error != "user_cancelled" {
		t.Fatalf("cancelled scheduled task = %+v", cancelled)
	}
	waitForDatabaseCondition(t, database, taskID, 4*time.Second, `clock_timestamp() >= run_at`)
	stillCancelled := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool {
		return view.Status == "CANCELLED"
	})
	if stillCancelled.LeaseVersion != 0 || stillCancelled.Steps[0].Attempt != 0 {
		t.Fatalf("cancelled task was claimed after run_at: %+v; worker logs=%s; scheduler logs=%s", stillCancelled, workerLogs, schedulerLogs)
	}
	assertScheduledEventCounts(t, server.URL, taskID, 0, 1)
}

func TestSchedulerUsesRetryAtAfterScheduledTaskStarts(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if workerBinary == "" || schedulerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY and RUNSTATE_SCHEDULER_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)

	var runAt time.Time
	if err := database.pool.QueryRow(ctx, `SELECT clock_timestamp() + interval '300 milliseconds'`).Scan(&runAt); err != nil {
		t.Fatalf("choose database-relative run_at: %v", err)
	}
	offset := time.FixedZone("UTC+08", 8*60*60)
	body, err := json.Marshal(map[string]any{
		"run_at": runAt.In(offset).Format(time.RFC3339Nano),
		"steps": []map[string]any{{
			"type": "flaky", "input": map[string]any{"temporary_failures": 1, "value": "retry after scheduled start"},
		}},
	})
	if err != nil {
		t.Fatalf("encode scheduled retry task: %v", err)
	}
	taskID := createHTTPTask(t, server.URL, string(body))
	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "worker-scheduled-retry")
	t.Cleanup(func() { stopProcess(t, worker) })
	schedulerA, schedulerALogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, schedulerA) })

	waiting := waitForTask(t, server.URL, taskID, 5*time.Second, func(view taskView) bool {
		return view.Status == "RETRY_WAIT"
	})
	if waiting.RetryAt == nil || !waiting.RetryAt.After(waiting.RunAt) || waiting.Steps[0].Attempt != 1 || waiting.FirstStartedAt == nil || waiting.DeadlineAt == nil {
		t.Fatalf("scheduled attempt did not enter a distinct retry wait: %+v; worker logs=%s; scheduler logs=%s", waiting, workerLogs, schedulerALogs)
	}
	stopProcess(t, schedulerA)
	waitForDatabaseCondition(t, database, taskID, 3*time.Second, `clock_timestamp() >= retry_at`)
	stillWaiting := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool {
		return view.Status == "RETRY_WAIT"
	})
	if stillWaiting.Steps[0].Attempt != 1 || stillWaiting.RetryAt == nil || !stillWaiting.RetryAt.Equal(*waiting.RetryAt) {
		t.Fatalf("retry state changed without a scheduler: %+v", stillWaiting)
	}

	schedulerB, schedulerBLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, schedulerB) })
	finished := waitForTask(t, server.URL, taskID, 5*time.Second, func(view taskView) bool {
		return view.Status == "SUCCEEDED"
	})
	stopProcess(t, schedulerB)
	if finished.Steps[0].Attempt != 2 || finished.RetryAt != nil || finished.FirstStartedAt == nil || !finished.FirstStartedAt.Equal(*waiting.FirstStartedAt) || finished.DeadlineAt == nil || !finished.DeadlineAt.Equal(*waiting.DeadlineAt) {
		t.Fatalf("retried scheduled task = %+v; worker logs=%s; scheduler logs=%s", finished, workerLogs, schedulerBLogs)
	}
	assertScheduledEventCounts(t, server.URL, taskID, 1, 0)
	assertEventCount(t, server.URL, taskID, "TASK_RETRY_READY", 1)
}

func createScheduledHTTPTask(t *testing.T, baseURL string, runAt time.Time) string {
	t.Helper()
	offset := time.FixedZone("UTC+08", 8*60*60)
	body, err := json.Marshal(map[string]any{
		"run_at": runAt.In(offset).Format(time.RFC3339Nano),
		"steps":  []map[string]any{{"type": "echo", "input": map[string]any{"value": "scheduled"}}},
	})
	if err != nil {
		t.Fatalf("encode scheduled task: %v", err)
	}
	return createHTTPTask(t, baseURL, string(body))
}

func assertScheduledEventCounts(t *testing.T, baseURL, taskID string, wantReady, wantCancelled int) {
	t.Helper()
	response, err := http.Get(baseURL + "/tasks/" + taskID + "/events")
	if err != nil {
		t.Fatalf("get task events: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("events status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var events []struct {
		Type string `json:"type"`
	}
	if err := json.NewDecoder(response.Body).Decode(&events); err != nil {
		t.Fatalf("decode task events: %v", err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["TASK_RUNNABLE"] != wantReady || counts["TASK_CANCELLED"] != wantCancelled {
		t.Fatalf("scheduled event counts = %v; want TASK_RUNNABLE=%d TASK_CANCELLED=%d", counts, wantReady, wantCancelled)
	}
}
