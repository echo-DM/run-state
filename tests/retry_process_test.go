package tests_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

func TestCompiledWorkerAndSchedulerPreserveRetryAcrossRestart(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"echo","input":{"value":"durable predecessor"}},{"type":"flaky","input":{"temporary_failures":1,"value":"recovered"}}]}`)

	workerA, workerLogs := startWorkerProcess(t, workerBinary, database.url, "worker-retry-a")
	schedulerA, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	waiting := waitForTask(t, server.URL, taskID, 4*time.Second, func(view taskView) bool {
		return view.Status == "RETRY_WAIT"
	})
	if waiting.RetryAt == nil || waiting.Steps[0].Attempt != 1 || waiting.Steps[1].Attempt != 1 {
		t.Fatalf("first retry wait = %+v; worker logs=%s; scheduler logs=%s", waiting, workerLogs, schedulerLogs)
	}
	key := waiting.Steps[1].IdempotencyKey
	resolvedInput := append([]byte(nil), waiting.Steps[1].ResolvedInput...)
	barrier, err := database.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin retry barrier: %v", err)
	}
	if _, err := barrier.Exec(context.Background(), `SELECT id FROM tasks WHERE id = $1 FOR UPDATE`, taskID); err != nil {
		t.Fatalf("lock retry barrier: %v", err)
	}
	stopProcess(t, workerA)
	stopProcess(t, schedulerA)
	if err := barrier.Rollback(context.Background()); err != nil {
		t.Fatalf("release retry barrier: %v", err)
	}
	stopped := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return true })
	if stopped.Status != "RETRY_WAIT" || stopped.RetryAt == nil || !stopped.RetryAt.Equal(*waiting.RetryAt) || stopped.Steps[1].Attempt != 1 {
		t.Fatalf("retry facts changed while stopping processes: before=%+v after=%+v", waiting, stopped)
	}

	time.Sleep(time.Until(*waiting.RetryAt) + 50*time.Millisecond)
	workerB, workerBLogs := startWorkerProcess(t, workerBinary, database.url, "worker-retry-b")
	schedulerB, schedulerBLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() {
		stopProcess(t, workerB)
		stopProcess(t, schedulerB)
	})
	finished := waitForTask(t, server.URL, taskID, 5*time.Second, func(view taskView) bool {
		return view.Status == "SUCCEEDED"
	})
	if finished.Steps[0].Attempt != 1 || finished.Steps[1].Attempt != 2 || finished.Steps[1].IdempotencyKey != key || !bytes.Equal(finished.Steps[1].ResolvedInput, resolvedInput) {
		t.Fatalf("retry result = %+v; worker logs=%s; scheduler logs=%s", finished, workerBLogs, schedulerBLogs)
	}
}

func TestCompiledSchedulersCompeteForOneRetryWake(t *testing.T) {
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	if schedulerBinary == "" {
		t.Skip("RUNSTATE_SCHEDULER_BINARY is required")
	}
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	durableStore := store.New(database.pool, store.Options{LeaseDuration: 5 * time.Second})
	created, err := durableStore.CreateTask(ctx, retryDefinition(3))
	if err != nil {
		t.Fatalf("create retry task: %v", err)
	}
	claim, err := durableStore.ClaimTask(ctx, "prepare-retry")
	if err != nil {
		t.Fatalf("claim retry task: %v", err)
	}
	attempt, err := durableStore.StartStep(ctx, claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start retry task: %v", err)
	}
	if err := durableStore.FinishStep(ctx, claim.Token(), attempt, store.StepOutcome{Error: "temporary", FailureClass: store.FailureTemporary}); err != nil {
		t.Fatalf("wait retry task: %v", err)
	}
	if _, err := database.pool.Exec(ctx, `UPDATE tasks SET retry_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	schedulerA, logsA := startSchedulerProcess(t, schedulerBinary, database.url)
	schedulerB, logsB := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() {
		stopProcess(t, schedulerA)
		stopProcess(t, schedulerB)
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		loaded, loadErr := durableStore.LoadTask(ctx, created.ID)
		if loadErr != nil {
			t.Fatalf("load scheduler task: %v", loadErr)
		}
		if loaded.Status == "RUNNABLE" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopProcess(t, schedulerA)
	stopProcess(t, schedulerB)
	var readyEvents int
	if err := database.pool.QueryRow(ctx, `SELECT count(*) FROM task_events WHERE task_id = $1 AND event_type = 'TASK_RETRY_READY'`, created.ID).Scan(&readyEvents); err != nil {
		t.Fatalf("count ready events: %v", err)
	}
	loaded, err := durableStore.LoadTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("load woken task: %v", err)
	}
	if loaded.Status != "RUNNABLE" || readyEvents != 1 {
		t.Fatalf("scheduler competition: task=%+v ready_events=%d logsA=%s logsB=%s", loaded, readyEvents, logsA, logsB)
	}
}

func TestCompiledWorkerCrashesExhaustAttemptBudget(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"sleep","input":{"duration":"5s"},"max_attempts":2}]}`)
	for attemptNumber := 1; attemptNumber <= 2; attemptNumber++ {
		worker, logs := startWorkerProcess(t, workerBinary, database.url, fmt.Sprintf("crash-worker-%d", attemptNumber))
		running := waitForTask(t, server.URL, taskID, 4*time.Second, func(view taskView) bool {
			return view.Status == "RUNNING" && view.Steps[0].Status == "RUNNING" && view.Steps[0].Attempt == attemptNumber
		})
		if running.RetryAt != nil {
			t.Fatalf("worker_lost attempt entered business backoff: %+v logs=%s", running, logs)
		}
		stopProcess(t, worker)
	}
	converger, logs := startWorkerProcess(t, workerBinary, database.url, "crash-converger")
	t.Cleanup(func() { stopProcess(t, converger) })
	failed := waitForTask(t, server.URL, taskID, 4*time.Second, func(view taskView) bool { return view.Status == "FAILED" })
	if failed.Steps[0].Attempt != 2 || failed.Steps[0].Error == nil || *failed.Steps[0].Error != "worker_lost" || failed.RetryAt != nil {
		t.Fatalf("crash exhaustion = %+v logs=%s", failed, logs)
	}
}

func retryDefinition(maxAttempts int) task.Definition {
	return task.Definition{
		TaskTimeoutSeconds: 120,
		Steps: []task.StepDefinition{{
			Type: "echo", Input: json.RawMessage(`{"value":"retry"}`), TimeoutSeconds: 5, MaxAttempts: maxAttempts,
		}},
	}
}

func TestCompiledWorkerDistinguishesPermanentAndExhaustedFailures(t *testing.T) {
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
	permanentID := createHTTPTask(t, server.URL, `{"steps":[{"type":"flaky","input":{"permanent":true,"value":"never"}}]}`)
	exhaustedID := createHTTPTask(t, server.URL, `{"steps":[{"type":"flaky","input":{"temporary_failures":9,"value":"never"},"max_attempts":2}]}`)
	worker, workerLogs := startWorkerProcess(t, workerBinary, database.url, "worker-failures")
	scheduler, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() {
		stopProcess(t, worker)
		stopProcess(t, scheduler)
	})
	permanent := waitForTask(t, server.URL, permanentID, 4*time.Second, func(view taskView) bool { return view.Status == "FAILED" })
	exhausted := waitForTask(t, server.URL, exhaustedID, 6*time.Second, func(view taskView) bool { return view.Status == "FAILED" })
	if permanent.Steps[0].Attempt != 1 || permanent.Steps[0].Error == nil || *permanent.Steps[0].Error != "controlled_permanent_failure" {
		t.Fatalf("permanent result = %+v; worker logs=%s", permanent, workerLogs)
	}
	if exhausted.Steps[0].Attempt != 2 || exhausted.Steps[0].Error == nil || *exhausted.Steps[0].Error != "controlled_temporary_failure" {
		t.Fatalf("exhausted result = %+v; worker logs=%s; scheduler logs=%s", exhausted, workerLogs, schedulerLogs)
	}
}

func startSchedulerProcess(t *testing.T, binary, databaseURL string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	command := exec.Command(binary)
	command.Env = append(os.Environ(),
		"RUNSTATE_DATABASE_URL="+databaseURL,
		"RUNSTATE_SCHEDULER_POLL_INTERVAL=15ms",
	)
	logs := &bytes.Buffer{}
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}
	return command, logs
}

func stopProcess(t *testing.T, command *exec.Cmd) {
	t.Helper()
	if command == nil || command.Process == nil || command.ProcessState != nil {
		return
	}
	_ = command.Process.Kill()
	_ = command.Wait()
}
