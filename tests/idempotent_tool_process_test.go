package tests_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/faketool"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
)

func TestBusinessRetryAndLostRuntimeCommitResponseKeepToolRequestIdentity(t *testing.T) {
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	toolServer := httptestServer(t, faketool.New(database.pool))
	finishCommits := 0
	durableStore := store.New(database.pool, store.Options{AfterCommit: func(operation string) error {
		if operation == "finish_step" {
			finishCommits++
		}
		if operation == "finish_step" && finishCommits == 2 {
			return errors.New("controlled lost response")
		}
		return nil
	}})
	created, err := durableStore.CreateTask(context.Background(), task.Definition{TaskTimeoutSeconds: 60, Steps: []task.StepDefinition{{
		Type: "fake_tool", Input: json.RawMessage(`{"value":"stable"}`), TimeoutSeconds: 5, MaxAttempts: 3,
	}}})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim, err := durableStore.ClaimTask(context.Background(), "identity-worker-a")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	first, err := durableStore.StartStep(context.Background(), claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start first attempt: %v", err)
	}
	firstResult := invokeTool(t, toolServer, first.IdempotencyKey, first.ResolvedInput)
	if err := durableStore.FinishStep(context.Background(), claim.Token(), first, store.StepOutcome{Error: "controlled temporary response loss", FailureClass: store.FailureTemporary}); err != nil {
		t.Fatalf("finish retryable attempt: %v", err)
	}
	if _, err := database.pool.Exec(context.Background(), `UPDATE tasks SET retry_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	if woke, err := durableStore.WakeDueRetry(context.Background()); err != nil || !woke {
		t.Fatalf("wake retry: woke=%v err=%v", woke, err)
	}
	claim, err = durableStore.ClaimTask(context.Background(), "identity-worker-b")
	if err != nil {
		t.Fatalf("claim retry: %v", err)
	}
	second, err := durableStore.StartStep(context.Background(), claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start second attempt: %v", err)
	}
	if second.IdempotencyKey != first.IdempotencyKey || !bytes.Equal(second.ResolvedInput, first.ResolvedInput) {
		t.Fatalf("request identity drifted: first=%+v second=%+v", first, second)
	}
	secondResult := invokeTool(t, toolServer, second.IdempotencyKey, second.ResolvedInput)
	if !bytes.Equal(firstResult, secondResult) {
		t.Fatalf("tool result changed: first=%s second=%s", firstResult, secondResult)
	}
	if err := durableStore.FinishStep(context.Background(), claim.Token(), second, store.StepOutcome{Output: secondResult}); err != nil {
		t.Fatalf("finish after lost commit response: %v", err)
	}
	loaded, err := durableStore.LoadTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("load completed task: %v", err)
	}
	effect := waitForToolEffect(t, toolServer, first.IdempotencyKey, time.Second, 1)
	if loaded.Status != task.StatusSucceeded || loaded.Steps[0].Attempt != 2 || effect.RequestCount != 2 {
		t.Fatalf("identity recovery task=%+v effect=%+v", loaded, effect)
	}
}

func invokeTool(t *testing.T, baseURL, key string, payload json.RawMessage) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(map[string]any{"idempotency_key": key, "payload": payload})
	if err != nil {
		t.Fatalf("encode invocation: %v", err)
	}
	response, err := http.Post(baseURL+"/invoke", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("invoke tool: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("invoke status = %d", response.StatusCode)
	}
	var result json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode invocation: %v", err)
	}
	return result
}

func TestFakeToolAtomicallyDeduplicatesConcurrentRequestsAndRejectsConflicts(t *testing.T) {
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptestServer(t, faketool.New(database.pool))
	const key = "concurrent-key"
	const body = `{"idempotency_key":"concurrent-key","payload":{"value":"once"}}`
	statuses := make(chan int, 8)
	for range 8 {
		go func() {
			response, err := http.Post(server+"/invoke", "application/json", bytes.NewBufferString(body))
			if err != nil {
				statuses <- 0
				return
			}
			response.Body.Close()
			statuses <- response.StatusCode
		}()
	}
	for range 8 {
		if status := <-statuses; status != http.StatusOK {
			t.Fatalf("concurrent invoke status = %d", status)
		}
	}
	effect := waitForToolEffect(t, server, key, time.Second, 1)
	if effect.RequestCount != 8 || string(effect.Result) != `{"value": "once"}` && string(effect.Result) != `{"value":"once"}` {
		t.Fatalf("deduplicated effect = %+v", effect)
	}
	response, err := http.Post(server+"/invoke", "application/json", bytes.NewBufferString(`{"idempotency_key":"concurrent-key","payload":{"value":"different"}}`))
	if err != nil {
		t.Fatalf("conflicting invoke: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("conflicting invoke status = %d", response.StatusCode)
	}
	after := waitForToolEffect(t, server, key, time.Second, 1)
	if after.RequestCount != 8 {
		t.Fatalf("conflict changed effect = %+v", after)
	}
}

func TestCompiledWorkerRecoversIdempotentToolAfterEffectWasCommitted(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	toolBinary := os.Getenv("RUNSTATE_FAKE_TOOL_BINARY")
	if workerBinary == "" || toolBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY and RUNSTATE_FAKE_TOOL_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	tool, toolURL, toolLogs := startFakeToolProcess(t, toolBinary, database.url)
	t.Cleanup(func() { stopProcess(t, tool) })
	server := httptestServer(t, api.New(store.New(database.pool, store.Options{})))
	taskID := createHTTPTask(t, server, `{"steps":[{"type":"echo","input":{"value":"stable"}},{"type":"fake_tool","input":{"value":{"$ref":"previous_output"},"block_first_response":true},"max_attempts":3}]}`)

	workerA, workerALogs := startWorkerWithTool(t, workerBinary, database.url, toolURL, "tool-worker-a")
	running := waitForTask(t, server, taskID, 4*time.Second, func(view taskView) bool {
		return view.Status == "RUNNING" && view.Steps[1].Status == "RUNNING"
	})
	key := running.Steps[1].IdempotencyKey
	waitForToolEffect(t, toolURL, key, 3*time.Second, 1)
	stopProcess(t, workerA)

	workerB, workerBLogs := startWorkerWithTool(t, workerBinary, database.url, toolURL, "tool-worker-b")
	t.Cleanup(func() { stopProcess(t, workerB) })
	finished := waitForTask(t, server, taskID, 5*time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
	effect := waitForToolEffect(t, toolURL, key, time.Second, 1)
	if finished.CurrentStep != 2 || finished.Steps[0].Attempt != 1 || finished.Steps[1].Attempt != 2 || effect.RequestCount != 2 {
		t.Fatalf("recovery facts task=%+v effect=%+v workerA=%s workerB=%s tool=%s", finished, effect, workerALogs, workerBLogs, toolLogs)
	}
	var succeededEvents int
	if err := database.pool.QueryRow(context.Background(), `SELECT count(*) FROM task_events WHERE task_id = $1 AND event_type = 'STEP_SUCCEEDED' AND payload->>'step_id' = $2`, taskID, running.Steps[1].ID).Scan(&succeededEvents); err != nil {
		t.Fatalf("count successful tool events: %v", err)
	}
	if succeededEvents != 1 {
		t.Fatalf("successful tool events = %d, want 1", succeededEvents)
	}
	if !bytes.Equal(finished.Steps[1].ResolvedInput, effect.Payload) {
		t.Fatalf("runtime input = %s, tool payload = %s", finished.Steps[1].ResolvedInput, effect.Payload)
	}
	if !bytes.Equal(finished.Steps[1].Output, effect.Result) {
		t.Fatalf("HTTP task result = %s, tool result = %s", finished.Steps[1].Output, effect.Result)
	}
}

func TestCompiledWorkerRetriesLostToolResponseWithSameIdentity(t *testing.T) {
	workerBinary := os.Getenv("RUNSTATE_WORKER_BINARY")
	schedulerBinary := os.Getenv("RUNSTATE_SCHEDULER_BINARY")
	toolBinary := os.Getenv("RUNSTATE_FAKE_TOOL_BINARY")
	if workerBinary == "" || schedulerBinary == "" || toolBinary == "" {
		t.Skip("RUNSTATE_WORKER_BINARY, RUNSTATE_SCHEDULER_BINARY and RUNSTATE_FAKE_TOOL_BINARY are required")
	}
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	tool, toolURL, toolLogs := startFakeToolProcess(t, toolBinary, database.url)
	t.Cleanup(func() { stopProcess(t, tool) })
	server := httptestServer(t, api.New(store.New(database.pool, store.Options{})))
	taskID := createHTTPTask(t, server, `{"steps":[{"type":"fake_tool","input":{"value":"lost response","block_first_response":true},"timeout_seconds":1,"max_attempts":3}]}`)
	worker, workerLogs := startWorkerWithTool(t, workerBinary, database.url, toolURL, "response-loss-worker")
	scheduler, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, worker); stopProcess(t, scheduler) })
	waiting := waitForTask(t, server, taskID, 4*time.Second, func(view taskView) bool { return view.Status == "RETRY_WAIT" })
	key := waiting.Steps[0].IdempotencyKey
	input := append([]byte(nil), waiting.Steps[0].ResolvedInput...)
	finished := waitForTask(t, server, taskID, 5*time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
	effect := waitForToolEffect(t, toolURL, key, time.Second, 1)
	if finished.Steps[0].Attempt != 2 || finished.Steps[0].IdempotencyKey != key || !bytes.Equal(finished.Steps[0].ResolvedInput, input) || effect.RequestCount != 2 {
		t.Fatalf("lost response recovery task=%+v effect=%+v worker=%s scheduler=%s tool=%s", finished, effect, workerLogs, schedulerLogs, toolLogs)
	}
}

type toolEffect struct {
	Key          string          `json:"key"`
	Payload      json.RawMessage `json:"payload"`
	Result       json.RawMessage `json:"result"`
	EffectCount  int             `json:"effect_count"`
	RequestCount int             `json:"request_count"`
}

func waitForToolEffect(t *testing.T, baseURL, key string, within time.Duration, count int) toolEffect {
	t.Helper()
	deadline := time.Now().Add(within)
	var effect toolEffect
	for time.Now().Before(deadline) {
		response, err := http.Get(baseURL + "/effects/" + key)
		if err == nil {
			decodeErr := json.NewDecoder(response.Body).Decode(&effect)
			response.Body.Close()
			if decodeErr == nil && response.StatusCode == http.StatusOK && effect.EffectCount == count {
				return effect
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("tool effect %q did not reach count %d; latest=%+v", key, count, effect)
	return toolEffect{}
}

func startFakeToolProcess(t *testing.T, binary, databaseURL string) (*exec.Cmd, string, *bytes.Buffer) {
	t.Helper()
	address := reserveAddress(t)
	command := exec.Command(binary)
	command.Env = append(os.Environ(), "RUNSTATE_DATABASE_URL="+databaseURL, "RUNSTATE_FAKE_TOOL_ADDR="+address)
	logs := &bytes.Buffer{}
	command.Stdout, command.Stderr = logs, logs
	if err := command.Start(); err != nil {
		t.Fatalf("start fake tool: %v", err)
	}
	baseURL := "http://" + address
	waitForHTTP(t, baseURL+"/healthz", 2*time.Second)
	return command, baseURL, logs
}

func startWorkerWithTool(t *testing.T, binary, databaseURL, toolURL, workerID string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	command := exec.Command(binary, "--id", workerID, "--concurrency", "1")
	command.Env = append(os.Environ(), "RUNSTATE_DATABASE_URL="+databaseURL, "RUNSTATE_FAKE_TOOL_URL="+toolURL,
		"RUNSTATE_LEASE_DURATION=350ms", "RUNSTATE_HEARTBEAT_INTERVAL=75ms", "RUNSTATE_POLL_INTERVAL=15ms")
	logs := &bytes.Buffer{}
	command.Stdout, command.Stderr = logs, logs
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", workerID, err)
	}
	return command, logs
}

func httptestServer(t *testing.T, handler http.Handler) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server.URL
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func waitForHTTP(t *testing.T, url string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		response, err := http.Get(url)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("endpoint %s did not become ready", url))
}
