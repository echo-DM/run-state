package tests_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
)

func TestHTTPCancelEndsRunnableTaskSynchronouslyAndIsIdempotent(t *testing.T) {
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	runtimeStore := store.New(database.pool, store.Options{})
	server := httptest.NewServer(api.New(runtimeStore))
	t.Cleanup(server.Close)
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"echo","input":{"value":"cancel me"}},{"type":"echo","input":{"value":"leave pending"}}]}`)

	for attempt := 1; attempt <= 2; attempt++ {
		response, err := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", bytes.NewReader(nil))
		if err != nil {
			t.Fatalf("cancel attempt %d: %v", attempt, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("cancel attempt %d status = %d, want 200", attempt, response.StatusCode)
		}
	}

	finished := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "CANCELLED" })
	if finished.Steps[0].Status != "FAILED" || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "user_cancelled" || finished.Steps[1].Status != "PENDING" {
		t.Fatalf("cancelled task steps = %+v", finished.Steps)
	}
	assertEventCount(t, server.URL, taskID, "TASK_CANCELLED", 1)

	missing, err := http.Post(server.URL+"/tasks/missing/cancel", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("cancel missing task: %v", err)
	}
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("cancel missing status = %d, want 404", missing.StatusCode)
	}

	succeededID := createHTTPTask(t, server.URL, `{"steps":[{"type":"echo","input":{"value":"done"}}]}`)
	claim, err := runtimeStore.ClaimTask(context.Background(), "terminal-before-cancel")
	if err != nil {
		t.Fatalf("claim terminal task: %v", err)
	}
	succeededView := waitForTask(t, server.URL, succeededID, time.Second, func(view taskView) bool { return true })
	attempt, err := runtimeStore.StartStep(context.Background(), claim.Token(), succeededView.Steps[0].ID)
	if err != nil {
		t.Fatalf("start terminal task: %v", err)
	}
	if err := runtimeStore.FinishStep(context.Background(), claim.Token(), attempt, store.StepOutcome{Output: []byte(`{"value":"done"}`)}); err != nil {
		t.Fatalf("finish terminal task: %v", err)
	}
	conflict, err := http.Post(server.URL+"/tasks/"+succeededID+"/cancel", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("cancel succeeded task: %v", err)
	}
	conflict.Body.Close()
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("cancel succeeded status = %d, want 409", conflict.StatusCode)
	}
}

func TestCompiledWorkerPropagatesRunningCancellationAndFinalizesIt(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":30,"steps":[{"type":"sleep","input":{"duration":"10s"},"timeout_seconds":20}]}`)

	worker, logs := startWorkerProcess(t, workerBinary, database.url, "cancel-worker")
	t.Cleanup(func() { stopProcess(t, worker) })
	waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "RUNNING" && view.Steps[0].Status == "RUNNING"
	})
	response, err := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("request running cancellation: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("running cancel status = %d, want 202", response.StatusCode)
	}
	finished := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "CANCELLED" })
	if finished.WorkerID != nil || finished.Steps[0].Status != "FAILED" || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "user_cancelled" {
		t.Fatalf("running cancel result = %+v; worker logs=%s", finished, logs)
	}
	assertEventCount(t, server.URL, taskID, "TASK_CANCELLED", 1)
	nextID := createHTTPTask(t, server.URL, `{"steps":[{"type":"echo","input":{"value":"slot released"}}]}`)
	waitForTask(t, server.URL, nextID, 2*time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
}

func TestCompiledWorkerCancellationWinsAtTaskDeadlineWithoutScheduler(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":1,"steps":[{"type":"sleep","input":{"duration":"10s"},"timeout_seconds":5}]}`)

	worker, logs := startWorkerProcessWithCancellationInterval(t, workerBinary, database.url, "cancel-deadline-worker", "5s")
	t.Cleanup(func() { stopProcess(t, worker) })
	waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "RUNNING" && view.Steps[0].Status == "RUNNING"
	})
	waitForDatabaseCondition(t, database, taskID, 2*time.Second, `deadline_at > clock_timestamp() AND deadline_at - clock_timestamp() <= interval '150 milliseconds'`)
	response, err := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("request cancellation near deadline: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("near-deadline cancel status = %d, want 202", response.StatusCode)
	}
	finished := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "CANCELLED" })
	if finished.Steps[0].Error == nil || *finished.Steps[0].Error != "user_cancelled" {
		t.Fatalf("near-deadline cancellation = %+v; logs=%s", finished, logs)
	}
	assertEventCount(t, server.URL, taskID, "TASK_TIMED_OUT", 0)
}

func startWorkerProcessWithCancellationInterval(t *testing.T, binary, databaseURL, workerID, cancellationInterval string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	command := exec.Command(binary, "--id", workerID, "--concurrency", "1")
	command.Env = append(os.Environ(),
		"RUNSTATE_DATABASE_URL="+databaseURL,
		"RUNSTATE_LEASE_DURATION=350ms",
		"RUNSTATE_HEARTBEAT_INTERVAL=75ms",
		"RUNSTATE_POLL_INTERVAL=15ms",
		"RUNSTATE_CANCELLATION_INTERVAL="+cancellationInterval,
	)
	logs := &bytes.Buffer{}
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", workerID, err)
	}
	return command, logs
}

func TestHTTPRunningCancellationRepeats202UntilSchedulerFinalizes(t *testing.T) {
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if schedulerBinary == "" {
		t.Skip("RUNSTATE_SCHEDULER_BINARY is required")
	}
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	runtimeStore := store.New(database.pool, store.Options{LeaseDuration: 5 * time.Second})
	server := httptest.NewServer(api.New(runtimeStore))
	t.Cleanup(server.Close)
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"sleep","input":{"duration":"10s"}}]}`)
	claim, err := runtimeStore.ClaimTask(ctx, "crashed-cancel-owner")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	view := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return true })
	if _, err := runtimeStore.StartStep(ctx, claim.Token(), view.Steps[0].ID); err != nil {
		t.Fatalf("start step: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		response, err := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", bytes.NewReader(nil))
		if err != nil {
			t.Fatalf("running cancel attempt %d: %v", attempt, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("running cancel attempt %d status = %d, want 202", attempt, response.StatusCode)
		}
	}

	schedulerA, logsA := startSchedulerProcess(t, schedulerBinary, database.url)
	schedulerB, logsB := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() {
		stopProcess(t, schedulerA)
		stopProcess(t, schedulerB)
	})
	finished := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "CANCELLED" })
	if finished.LeaseVersion != claim.LeaseVersion || finished.Steps[0].Attempt != 1 {
		t.Fatalf("scheduler cancellation changed ownership/attempt: %+v; logs=%s %s", finished, logsA, logsB)
	}
	assertEventCount(t, server.URL, taskID, "TASK_CANCELLED", 1)
	if err := runtimeStore.CancelOwnedTask(ctx, claim.Token()); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale owner cancellation = %v, want lease lost or idempotent terminal", err)
	}
}

func TestHTTPCancelEndsRetryWaitWithoutWakingTask(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"flaky","input":{"temporary_failures":1,"value":"never"},"max_attempts":3}]}`)
	worker, logs := startWorkerProcess(t, workerBinary, database.url, "retry-cancel-worker")
	waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "RETRY_WAIT" })
	stopProcess(t, worker)
	response, err := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("cancel retry wait: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("retry wait cancel status = %d; worker logs=%s", response.StatusCode, logs)
	}
	finished := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "CANCELLED" })
	if finished.RetryAt != nil || finished.Steps[0].Attempt != 1 || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "user_cancelled" {
		t.Fatalf("retry wait cancellation = %+v", finished)
	}
	assertEventCount(t, server.URL, taskID, "TASK_RETRY_READY", 0)
}
