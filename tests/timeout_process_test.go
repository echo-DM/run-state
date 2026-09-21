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

func TestCompiledWorkerRetriesStepTimeoutWithinTaskDeadline(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if workerBinary == "" || schedulerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY and RUNSTATE_SCHEDULER_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":8,"steps":[{"type":"sleep","input":{"duration":"3s"},"timeout_seconds":1,"max_attempts":2}]}`)

	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "step-timeout-worker")
	scheduler, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() {
		stopProcess(t, worker)
		stopProcess(t, scheduler)
	})
	finished := waitForTask(t, server.URL, taskID, 6*time.Second, func(view taskView) bool { return view.Status == "FAILED" })
	if finished.Steps[0].Attempt != 2 || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "step_timeout" {
		t.Fatalf("step timeout result = %+v; worker logs=%s; scheduler logs=%s", finished, workerLogs, schedulerLogs)
	}
	if finished.DeadlineAt == nil || finished.FirstStartedAt == nil || finished.DeadlineAt.Sub(*finished.FirstStartedAt) != 8*time.Second {
		t.Fatalf("task deadline changed across step retries: %+v", finished)
	}
}

func TestCompiledWorkerEndsExecutingTaskWithIndependentControlContext(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	if workerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY is required")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":1,"steps":[{"type":"sleep","input":{"duration":"5s"},"timeout_seconds":4,"max_attempts":3}]}`)

	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "task-timeout-worker")
	t.Cleanup(func() { stopProcess(t, worker) })
	running := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "RUNNING" && view.Steps[0].Status == "RUNNING"
	})
	finished := waitForTask(t, server.URL, taskID, 4*time.Second, func(view taskView) bool { return view.Status == "TIMED_OUT" })
	if running.DeadlineAt == nil || finished.DeadlineAt == nil || !running.DeadlineAt.Equal(*finished.DeadlineAt) {
		t.Fatalf("task deadline was reset: running=%+v finished=%+v", running, finished)
	}
	if finished.WorkerID != nil || finished.RetryAt != nil || finished.Steps[0].Attempt != 1 || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "task_timeout" {
		t.Fatalf("worker-owned task timeout result = %+v; worker logs=%s", finished, workerLogs)
	}
	assertEventCount(t, server.URL, taskID, "TASK_TIMED_OUT", 1)
}

func TestCompiledSchedulerTimesOutRetryWaitBeforeWakingIt(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if workerBinary == "" || schedulerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY and RUNSTATE_SCHEDULER_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":1,"steps":[{"type":"flaky","input":{"temporary_failures":2,"value":"never"},"timeout_seconds":5,"max_attempts":3}]}`)

	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "retry-deadline-worker")
	waiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "RETRY_WAIT" })
	stopProcess(t, worker)
	scheduler, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, scheduler) })
	finished := waitForTask(t, server.URL, taskID, 4*time.Second, func(view taskView) bool { return view.Status == "TIMED_OUT" })
	if waiting.DeadlineAt == nil || finished.DeadlineAt == nil || !waiting.DeadlineAt.Equal(*finished.DeadlineAt) || finished.Steps[0].Attempt != 1 {
		t.Fatalf("retry deadline result = %+v; worker logs=%s; scheduler logs=%s", finished, workerLogs, schedulerLogs)
	}
	assertEventCount(t, server.URL, taskID, "TASK_RETRY_READY", 0)
	assertEventCount(t, server.URL, taskID, "TASK_TIMED_OUT", 1)
}

func TestCompiledSchedulerTimesOutTaskWhileWorkerIsCrashed(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if workerBinary == "" || schedulerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY and RUNSTATE_SCHEDULER_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":2,"steps":[{"type":"sleep","input":{"duration":"5s"},"timeout_seconds":4,"max_attempts":3}]}`)

	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "crash-before-deadline")
	running := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "RUNNING" && view.Steps[0].Status == "RUNNING"
	})
	stopProcess(t, worker)
	schedulerA, schedulerLogsA := startSchedulerProcess(t, schedulerBinary, database.url)
	schedulerB, schedulerLogsB := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() {
		stopProcess(t, schedulerA)
		stopProcess(t, schedulerB)
	})
	finished := waitForTask(t, server.URL, taskID, 4*time.Second, func(view taskView) bool { return view.Status == "TIMED_OUT" })
	if running.DeadlineAt == nil || finished.DeadlineAt == nil || !running.DeadlineAt.Equal(*finished.DeadlineAt) || finished.LeaseVersion != running.LeaseVersion || finished.Steps[0].Attempt != 1 {
		t.Fatalf("crashed task timeout = %+v; worker logs=%s; schedulers=%s %s", finished, workerLogs, schedulerLogsA, schedulerLogsB)
	}
	assertEventCount(t, server.URL, taskID, "TASK_RECOVERED", 0)
	assertEventCount(t, server.URL, taskID, "TASK_TIMED_OUT", 1)
}

func assertEventCount(t *testing.T, baseURL, taskID, eventType string, want int) {
	t.Helper()
	response, err := http.Get(baseURL + "/tasks/" + taskID + "/events")
	if err != nil {
		t.Fatalf("get task events: %v", err)
	}
	defer response.Body.Close()
	var events []struct {
		Type string `json:"type"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&events) != nil {
		t.Fatalf("read task events status = %d", response.StatusCode)
	}
	got := 0
	for _, event := range events {
		if event.Type == eventType {
			got++
		}
	}
	if got != want {
		t.Fatalf("%s event count = %d, want %d; events=%+v", eventType, got, want, events)
	}
}
