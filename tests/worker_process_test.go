package tests_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func TestCompiledWorkersRecoverAfterProcessCrash(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	if workerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY is required for process recovery test")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)

	body := []byte(`{"steps":[{"type":"echo","input":{"value":"already durable"}},{"type":"sleep","input":{"duration":"900ms"}},{"type":"echo","input":{"value":"after recovery"}}]}`)
	response, err := http.Post(server.URL+"/tasks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create process task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create process task status = %d", response.StatusCode)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode created process task: %v", err)
	}

	workerA, logsA := startWorkerProcess(t, workerBinary, database.url, "worker-a")
	waitForTask(t, server.URL, created.ID, 4*time.Second, func(view taskView) bool {
		return len(view.Steps) == 3 && view.Steps[0].Status == "SUCCEEDED" && view.Steps[1].Status == "RUNNING"
	})
	if err := workerA.Process.Kill(); err != nil {
		t.Fatalf("kill worker A: %v", err)
	}
	if err := workerA.Wait(); err == nil {
		t.Fatal("killed worker A exited successfully, want signal failure")
	}

	workerB, logsB := startWorkerProcess(t, workerBinary, database.url, "worker-b")
	t.Cleanup(func() {
		if workerB.Process != nil {
			_ = workerB.Process.Kill()
			_ = workerB.Wait()
		}
	})
	finished := waitForTask(t, server.URL, created.ID, 6*time.Second, func(view taskView) bool {
		return view.Status == "SUCCEEDED"
	})
	if finished.CurrentStep != 3 || finished.Steps[0].Attempt != 1 || finished.Steps[1].Attempt != 2 || finished.Steps[2].Attempt != 1 {
		t.Fatalf("recovered attempts = %+v; worker A logs=%s; worker B logs=%s", finished, logsA.String(), logsB.String())
	}
}

func TestCompiledWorkerCrashAfterClaimDoesNotConsumeAttempt(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	if workerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY is required for process recovery test")
	}
	database := isolatedTestDatabase(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"echo","input":{"value":"claim window"}}]}`)

	barrier, err := database.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin step barrier: %v", err)
	}
	if _, err := barrier.Exec(ctx, `SELECT id FROM task_steps WHERE task_id = $1 FOR UPDATE`, taskID); err != nil {
		t.Fatalf("lock step barrier: %v", err)
	}
	workerA, _ := startWorkerProcess(t, workerBinary, database.url, "worker-a")
	waitForTask(t, server.URL, taskID, 4*time.Second, func(view taskView) bool {
		return view.Status == "RUNNING" && view.Steps[0].Status == "PENDING" && view.Steps[0].Attempt == 0
	})
	if err := workerA.Process.Kill(); err != nil {
		t.Fatalf("kill worker A: %v", err)
	}
	_ = workerA.Wait()
	if err := barrier.Rollback(ctx); err != nil {
		t.Fatalf("release step barrier: %v", err)
	}

	workerB, _ := startWorkerProcess(t, workerBinary, database.url, "worker-b")
	t.Cleanup(func() {
		if workerB.Process != nil {
			_ = workerB.Process.Kill()
			_ = workerB.Wait()
		}
	})
	finished := waitForTask(t, server.URL, taskID, 5*time.Second, func(view taskView) bool {
		return view.Status == "SUCCEEDED"
	})
	if finished.Steps[0].Attempt != 1 {
		t.Fatalf("attempt after claim-window crash = %d, want 1", finished.Steps[0].Attempt)
	}
}

func TestFullWorkerDoesNotPrefetchBeyondLeaseDuration(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	if workerBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY is required for capacity test")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)
	firstID := createHTTPTask(t, server.URL, `{"steps":[{"type":"sleep","input":{"duration":"800ms"}}]}`)
	secondID := createHTTPTask(t, server.URL, `{"steps":[{"type":"echo","input":{"value":"wait for capacity"}}]}`)

	process, _ := startWorkerProcess(t, workerBinary, database.url, "worker-one-slot")
	t.Cleanup(func() {
		if process.Process != nil {
			_ = process.Process.Kill()
			_ = process.Wait()
		}
	})
	waitForTask(t, server.URL, firstID, 4*time.Second, func(view taskView) bool {
		return view.Status == "RUNNING" && view.Steps[0].Status == "RUNNING"
	})
	waitForDatabaseCondition(t, database, firstID, 2*time.Second, `clock_timestamp() >= first_started_at + interval '450 milliseconds'`)
	queued := waitForTask(t, server.URL, secondID, time.Second, func(view taskView) bool { return true })
	if queued.Status != "RUNNABLE" || queued.LeaseVersion != 0 || queued.WorkerID != nil {
		t.Fatalf("second task was prefetched while only slot was full: %+v", queued)
	}
	waitForTask(t, server.URL, firstID, 3*time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
	waitForTask(t, server.URL, secondID, 3*time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
}

type taskView struct {
	ID             string     `json:"id"`
	Status         string     `json:"status"`
	CurrentStep    int        `json:"current_step"`
	LeaseVersion   int64      `json:"lease_version"`
	WorkerID       *string    `json:"worker_id"`
	RetryAt        *time.Time `json:"retry_at"`
	FirstStartedAt *time.Time `json:"first_started_at"`
	DeadlineAt     *time.Time `json:"deadline_at"`
	Steps          []struct {
		ID             string          `json:"id"`
		Status         string          `json:"status"`
		Attempt        int             `json:"attempt"`
		IdempotencyKey string          `json:"idempotency_key"`
		ResolvedInput  json.RawMessage `json:"resolved_input"`
		Output         json.RawMessage `json:"output"`
		Error          *string         `json:"error"`
	} `json:"steps"`
}

func createHTTPTask(t *testing.T, baseURL, body string) string {
	t.Helper()
	response, err := http.Post(baseURL+"/tasks", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create task status = %d", response.StatusCode)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode created task: %v", err)
	}
	return created.ID
}

func waitForDatabaseCondition(t *testing.T, database testDatabase, taskID string, within time.Duration, condition string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var ready bool
		query := `SELECT ` + condition + ` FROM tasks WHERE id = $1`
		if err := database.pool.QueryRow(context.Background(), query, taskID).Scan(&ready); err != nil {
			t.Fatalf("check database condition: %v", err)
		}
		if ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("database condition did not become true before deadline")
}

func startWorkerProcess(t *testing.T, binary, databaseURL, workerID string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	command := exec.Command(binary, "--id", workerID, "--concurrency", "1")
	command.Env = append(os.Environ(),
		"RUNSTATE_DATABASE_URL="+databaseURL,
		"RUNSTATE_LEASE_DURATION=350ms",
		"RUNSTATE_HEARTBEAT_INTERVAL=75ms",
		"RUNSTATE_POLL_INTERVAL=15ms",
		"RUNSTATE_CANCELLATION_INTERVAL=25ms",
	)
	logs := &bytes.Buffer{}
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", workerID, err)
	}
	return command, logs
}

func waitForTask(t *testing.T, baseURL, taskID string, within time.Duration, ready func(taskView) bool) taskView {
	t.Helper()
	deadline := time.Now().Add(within)
	var latest taskView
	for time.Now().Before(deadline) {
		response, err := http.Get(baseURL + "/tasks/" + taskID)
		if err == nil {
			latest = taskView{}
			decodeErr := json.NewDecoder(response.Body).Decode(&latest)
			response.Body.Close()
			if decodeErr == nil && response.StatusCode == http.StatusOK && ready(latest) {
				return latest
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("task did not reach expected state; latest=%+v", latest))
	return taskView{}
}
