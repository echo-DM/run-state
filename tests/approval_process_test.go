package tests_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
)

func TestHTTPTaskAcceptsApprovalStep(t *testing.T) {
	database := isolatedTestDatabase(t)
	if err := migrations.Run(context.Background(), database.pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	t.Cleanup(server.Close)

	response, err := http.Post(server.URL+"/tasks", "application/json", bytes.NewBufferString(
		`{"steps":[{"type":"approval","input":{}},{"type":"echo","input":{"value":"after approval"}}]}`,
	))
	if err != nil {
		t.Fatalf("create approval task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create approval task status = %d, want 201", response.StatusCode)
	}
	var created struct {
		Status string `json:"status"`
		Steps  []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"steps"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode created task: %v", err)
	}
	if created.Status != "RUNNABLE" || len(created.Steps) != 2 || created.Steps[0].Type != "approval" || created.Steps[0].Status != "PENDING" {
		t.Fatalf("created task = %+v, want runnable task with a pending approval step", created)
	}
}

func TestCompiledWorkerResumesAfterApprovalAndOldReplayCannotDecideNextStep(t *testing.T) {
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
	t.Cleanup(func() { server.Close() })
	taskID := createHTTPTask(t, server.URL,
		`{"steps":[{"type":"approval","input":{}},{"type":"echo","input":{"value":"step two"}},{"type":"approval","input":{}},{"type":"echo","input":{"value":"complete"}}]}`)
	worker, logs := startWorkerProcess(t, workerBinary, database.url, "approval-before-restart")
	workerActive := true
	var scheduler *exec.Cmd
	schedulerActive := false
	t.Cleanup(func() {
		if workerActive {
			stopProcess(t, worker)
		}
		if schedulerActive {
			stopProcess(t, scheduler)
		}
	})
	firstWaiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "WAITING_APPROVAL" && len(view.Steps) == 4 && view.Steps[0].Status == "WAITING_APPROVAL"
	})
	firstStepID := firstWaiting.Steps[0].ID
	firstStartedAt := firstWaiting.Steps[0].StartedAt
	scheduler, _ = startSchedulerProcess(t, schedulerBinary, database.url)
	schedulerActive = true
	stopProcess(t, worker)
	workerActive = false
	stopProcess(t, scheduler)
	schedulerActive = false
	server.Close()
	server = httptest.NewServer(api.New(store.New(database.pool, store.Options{})))
	scheduler, _ = startSchedulerProcess(t, schedulerBinary, database.url)
	schedulerActive = true
	worker, logs = startWorkerProcess(t, workerBinary, database.url, "approval-after-restart")
	workerActive = true

	status, firstDecision := postApprovalDecision(t, server.URL, taskID, "approve", firstStepID)
	if status != http.StatusOK || firstDecision.Decision != "approved" || firstDecision.TaskStatus != "RUNNABLE" {
		t.Fatalf("first approval = status %d, result %+v; logs=%s", status, firstDecision, logs)
	}
	secondWaiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "WAITING_APPROVAL" && view.CurrentStep == 2 && view.Steps[2].Status == "WAITING_APPROVAL"
	})
	if firstStartedAt == nil || secondWaiting.Steps[0].ApprovalDecision == nil || *secondWaiting.Steps[0].ApprovalDecision != "approved" || secondWaiting.Steps[0].FinishedAt == nil {
		t.Fatalf("first approval timing or decision was not persisted: %+v", secondWaiting.Steps[0])
	}
	status, replay := postApprovalDecision(t, server.URL, taskID, "approve", firstStepID)
	if status != http.StatusOK || replay.Decision != "approved" || replay.TaskStatus != "WAITING_APPROVAL" {
		t.Fatalf("old approval replay = status %d, result %+v", status, replay)
	}
	current := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return true })
	if current.Steps[2].Status != "WAITING_APPROVAL" || current.Steps[2].ApprovalDecision != nil || current.CurrentStep != 2 {
		t.Fatalf("replaying step 0 changed the second approval: %+v", current)
	}
	assertEventCount(t, server.URL, taskID, "TASK_APPROVED", 1)

	status, secondDecision := postApprovalDecision(t, server.URL, taskID, "approve", current.Steps[2].ID)
	if status != http.StatusOK || secondDecision.TaskStatus != "RUNNABLE" {
		t.Fatalf("second approval = status %d, result %+v", status, secondDecision)
	}
	finished := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
	if finished.Steps[3].Status != "SUCCEEDED" || string(finished.Steps[3].Output) != `{"value":"complete"}` {
		t.Fatalf("task after both approvals = %+v", finished)
	}
	assertEventCount(t, server.URL, taskID, "TASK_APPROVED", 2)
}

type approvalAPIResult struct {
	Decision   string `json:"decision"`
	TaskStatus string `json:"task_status"`
}

func postApprovalDecision(t *testing.T, baseURL, taskID, action, stepID string) (int, approvalAPIResult) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"step_id": stepID})
	if err != nil {
		t.Fatalf("encode approval request: %v", err)
	}
	response, err := http.Post(baseURL+"/tasks/"+taskID+"/"+action, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", action, err)
	}
	defer response.Body.Close()
	var result approvalAPIResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode %s response: %v", action, err)
	}
	return response.StatusCode, result
}

func TestHTTPRejectsApprovalWithValidationAndPersistsReplayableRejection(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"approval","input":{}},{"type":"echo","input":{"value":"must not run"}}]}`)
	runnable := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "RUNNABLE" })
	stepID := runnable.Steps[0].ID

	if status := postRawApproval(t, server.URL, taskID, "approve", `{}`); status != http.StatusBadRequest {
		t.Fatalf("approval without step_id status = %d, want 400", status)
	}
	if status, _ := postApprovalDecision(t, server.URL, "missing-task", "approve", stepID); status != http.StatusNotFound {
		t.Fatalf("approval for missing task status = %d, want 404", status)
	}
	if status, _ := postApprovalDecision(t, server.URL, taskID, "approve", "unrelated-step"); status != http.StatusNotFound {
		t.Fatalf("approval for nonexistent step status = %d, want 404", status)
	}
	if status, _ := postApprovalDecision(t, server.URL, taskID, "approve", stepID); status != http.StatusConflict {
		t.Fatalf("approval before task is waiting status = %d, want 409", status)
	}

	worker, _ := startWorkerProcess(t, workerBinary, database.url, "approval-reject-worker")
	t.Cleanup(func() { stopProcess(t, worker) })
	waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "WAITING_APPROVAL" && view.Steps[0].Status == "WAITING_APPROVAL"
	})
	status, rejected := postApprovalDecision(t, server.URL, taskID, "reject", stepID)
	if status != http.StatusOK || rejected.Decision != "rejected" || rejected.TaskStatus != "FAILED" {
		t.Fatalf("rejection = status %d, result %+v", status, rejected)
	}
	failed := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "FAILED" })
	if failed.Steps[0].Status != "FAILED" || failed.Steps[0].ApprovalDecision == nil || *failed.Steps[0].ApprovalDecision != "rejected" || failed.Steps[0].Error == nil || *failed.Steps[0].Error != "approval_rejected" || failed.Steps[0].FinishedAt == nil || failed.Steps[1].Status != "PENDING" {
		t.Fatalf("rejected task state = %+v", failed)
	}
	status, replay := postApprovalDecision(t, server.URL, taskID, "reject", stepID)
	if status != http.StatusOK || replay.Decision != "rejected" || replay.TaskStatus != "FAILED" {
		t.Fatalf("rejection replay = status %d, result %+v", status, replay)
	}
	if status, _ := postApprovalDecision(t, server.URL, taskID, "approve", stepID); status != http.StatusConflict {
		t.Fatalf("opposite approval replay status = %d, want 409", status)
	}
	assertEventCount(t, server.URL, taskID, "TASK_REJECTED", 1)
	assertEventCount(t, server.URL, taskID, "TASK_FAILED", 1)
}

func postRawApproval(t *testing.T, baseURL, taskID, action, body string) int {
	t.Helper()
	response, err := http.Post(baseURL+"/tasks/"+taskID+"/"+action, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post raw %s: %v", action, err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestApprovingLastStepSucceedsAndIgnoresStepTimeout(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":8,"steps":[{"type":"approval","input":{},"timeout_seconds":1,"max_attempts":1}]}`)
	worker, _ := startWorkerProcess(t, workerBinary, database.url, "last-approval-worker")
	t.Cleanup(func() { stopProcess(t, worker) })
	waiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "WAITING_APPROVAL" && view.Steps[0].Status == "WAITING_APPROVAL"
	})
	waitForDatabaseCondition(t, database, taskID, 3*time.Second, `EXISTS (SELECT 1 FROM task_steps WHERE task_id = $1 AND step_index = 0 AND status = 'WAITING_APPROVAL' AND started_at + interval '1 second' <= clock_timestamp())`)
	stillWaiting := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "WAITING_APPROVAL" })
	if stillWaiting.Steps[0].Attempt != 0 || waiting.DeadlineAt == nil {
		t.Fatalf("approval step consumed an attempt or lost task deadline: %+v", stillWaiting)
	}
	status, decision := postApprovalDecision(t, server.URL, taskID, "approve", stillWaiting.Steps[0].ID)
	if status != http.StatusOK || decision.Decision != "approved" || decision.TaskStatus != "SUCCEEDED" {
		t.Fatalf("last-step approval = status %d, result %+v", status, decision)
	}
	finished := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
	if finished.CurrentStep != 1 || finished.WorkerID != nil || finished.Steps[0].Status != "SUCCEEDED" || finished.Steps[0].Attempt != 0 || finished.Steps[0].ApprovalDecision == nil || *finished.Steps[0].ApprovalDecision != "approved" || finished.Steps[0].FinishedAt == nil {
		t.Fatalf("last approval completion = %+v", finished)
	}
	status, replay := postApprovalDecision(t, server.URL, taskID, "approve", finished.Steps[0].ID)
	if status != http.StatusOK || replay.Decision != "approved" || replay.TaskStatus != "SUCCEEDED" {
		t.Fatalf("terminal approval replay = status %d, result %+v", status, replay)
	}
	assertEventCount(t, server.URL, taskID, "TASK_APPROVED", 1)
	assertEventCount(t, server.URL, taskID, "TASK_SUCCEEDED", 1)
}

func TestCancellationRacesApprovalWithoutRevivingTask(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"approval","input":{}},{"type":"echo","input":{"value":"cancel or wait"}}]}`)
	worker, _ := startWorkerProcess(t, workerBinary, database.url, "approval-cancel-race")
	workerActive := true
	t.Cleanup(func() {
		if workerActive {
			stopProcess(t, worker)
		}
	})
	waiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "WAITING_APPROVAL" })
	stepID := waiting.Steps[0].ID
	stopProcess(t, worker)
	workerActive = false

	approvalBody, err := json.Marshal(map[string]string{"step_id": stepID})
	if err != nil {
		t.Fatalf("encode race approval: %v", err)
	}
	start := make(chan struct{})
	type responseResult struct {
		kind   string
		status int
		err    error
	}
	results := make(chan responseResult, 2)
	var requests sync.WaitGroup
	requests.Add(2)
	go func() {
		defer requests.Done()
		<-start
		response, requestErr := http.Post(server.URL+"/tasks/"+taskID+"/approve", "application/json", bytes.NewReader(approvalBody))
		if requestErr != nil {
			results <- responseResult{kind: "approval", err: requestErr}
			return
		}
		response.Body.Close()
		results <- responseResult{kind: "approval", status: response.StatusCode}
	}()
	go func() {
		defer requests.Done()
		<-start
		response, requestErr := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", bytes.NewReader(nil))
		if requestErr != nil {
			results <- responseResult{kind: "cancel", err: requestErr}
			return
		}
		response.Body.Close()
		results <- responseResult{kind: "cancel", status: response.StatusCode}
	}()
	close(start)
	requests.Wait()
	close(results)
	var approvalStatus, cancelStatus int
	for result := range results {
		if result.err != nil {
			t.Fatalf("approval/cancel race request: %v", result.err)
		}
		if result.kind == "approval" {
			approvalStatus = result.status
		} else {
			cancelStatus = result.status
		}
	}
	if (approvalStatus != http.StatusOK && approvalStatus != http.StatusConflict) || cancelStatus != http.StatusOK {
		t.Fatalf("approval/cancel race statuses = approval %d, cancel %d", approvalStatus, cancelStatus)
	}
	finished := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "CANCELLED" })
	if finished.WorkerID != nil || (finished.Steps[1].Status != "PENDING" && finished.Steps[1].Status != "FAILED") {
		t.Fatalf("approval/cancel race state = %+v", finished)
	}
	if approvalStatus == http.StatusOK {
		if finished.Steps[0].Status != "SUCCEEDED" || finished.Steps[0].ApprovalDecision == nil || *finished.Steps[0].ApprovalDecision != "approved" || finished.Steps[1].Status != "FAILED" {
			t.Fatalf("approval won but later cancellation was not preserved: %+v", finished)
		}
	} else if finished.Steps[0].Status != "FAILED" || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "user_cancelled" {
		t.Fatalf("cancellation won but late approval changed the waiting step: %+v", finished)
	}
	assertEventCount(t, server.URL, taskID, "TASK_CANCELLED", 1)
	var approvedEvents int
	if err := database.pool.QueryRow(context.Background(), `SELECT count(*) FROM task_events WHERE task_id = $1 AND event_type = 'TASK_APPROVED'`, taskID).Scan(&approvedEvents); err != nil {
		t.Fatalf("count approval race events: %v", err)
	}
	if (approvedEvents != 0 && approvedEvents != 1) || (approvalStatus == http.StatusOK && approvedEvents != 1) || (approvalStatus == http.StatusConflict && approvedEvents != 0) {
		t.Fatalf("approval event count = %d for approval status %d", approvedEvents, approvalStatus)
	}
}

func TestSchedulerTimesOutPersistentApprovalWait(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":30,"steps":[{"type":"approval","input":{},"timeout_seconds":30}]}`)
	worker, _ := startWorkerProcess(t, workerBinary, database.url, "approval-deadline-worker")
	workerActive := true
	t.Cleanup(func() {
		if workerActive {
			stopProcess(t, worker)
		}
	})
	waiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "WAITING_APPROVAL" })
	stepID := waiting.Steps[0].ID
	stopProcess(t, worker)
	workerActive = false
	var expiredDeadline time.Time
	if err := database.pool.QueryRow(context.Background(), `
		UPDATE tasks SET deadline_at = clock_timestamp() - interval '1 millisecond'
		WHERE id = $1 RETURNING deadline_at`, taskID).Scan(&expiredDeadline); err != nil {
		t.Fatalf("expire waiting task deadline: %v", err)
	}
	if status, _ := postApprovalDecision(t, server.URL, taskID, "approve", stepID); status != http.StatusConflict {
		t.Fatalf("approval after deadline status = %d, want 409", status)
	}
	stillWaiting := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "WAITING_APPROVAL" })
	if stillWaiting.Steps[0].ApprovalDecision != nil || stillWaiting.CurrentStep != 0 {
		t.Fatalf("late approval changed the expired task before scheduler handling: %+v", stillWaiting)
	}
	scheduler, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, scheduler) })
	finished := waitForTask(t, server.URL, taskID, 5*time.Second, func(view taskView) bool { return view.Status == "TIMED_OUT" })
	if finished.DeadlineAt == nil || !expiredDeadline.Equal(*finished.DeadlineAt) || finished.WorkerID != nil || finished.Steps[0].Attempt != 0 || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "task_timeout" || finished.Steps[0].FinishedAt == nil {
		t.Fatalf("approval deadline result = %+v; scheduler logs=%s", finished, schedulerLogs)
	}
	if status, _ := postApprovalDecision(t, server.URL, taskID, "approve", stepID); status != http.StatusConflict {
		t.Fatalf("late approval after timeout status = %d, want 409", status)
	}
	assertEventCount(t, server.URL, taskID, "TASK_TIMED_OUT", 1)
	assertEventCount(t, server.URL, taskID, "TASK_APPROVED", 0)
}

func TestHTTPApprovalRacesDeadlineAndScheduler(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"task_timeout_seconds":30,"steps":[{"type":"approval","input":{}}]}`)
	worker, _ := startWorkerProcess(t, workerBinary, database.url, "approval-deadline-race")
	workerActive := true
	t.Cleanup(func() {
		if workerActive {
			stopProcess(t, worker)
		}
	})
	waiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool {
		return view.Status == "WAITING_APPROVAL" && view.Steps[0].Status == "WAITING_APPROVAL"
	})
	stopProcess(t, worker)
	workerActive = false

	ctx := context.Background()
	blocker, err := database.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin task row blocker: %v", err)
	}
	defer blocker.Rollback(ctx)
	var lockedID string
	if err := blocker.QueryRow(ctx, `SELECT id FROM tasks WHERE id = $1 FOR UPDATE`, taskID).Scan(&lockedID); err != nil {
		t.Fatalf("lock approval task: %v", err)
	}
	var deadline time.Time
	if err := blocker.QueryRow(ctx, `UPDATE tasks SET deadline_at = clock_timestamp() + interval '750 milliseconds' WHERE id = $1 RETURNING deadline_at`, taskID).Scan(&deadline); err != nil {
		t.Fatalf("set near-term approval deadline: %v", err)
	}

	body, err := json.Marshal(map[string]string{"step_id": waiting.Steps[0].ID})
	if err != nil {
		t.Fatalf("encode approval request: %v", err)
	}
	type httpResult struct {
		status int
		err    error
	}
	approvalDone := make(chan httpResult, 1)
	go func() {
		response, requestErr := http.Post(server.URL+"/tasks/"+taskID+"/approve", "application/json", bytes.NewReader(body))
		if requestErr != nil {
			approvalDone <- httpResult{err: requestErr}
			return
		}
		response.Body.Close()
		approvalDone <- httpResult{status: response.StatusCode}
	}()
	waitForTaskRowLockWait(t, database, 2*time.Second)
	scheduler, schedulerLogs := startSchedulerProcess(t, schedulerBinary, database.url)
	t.Cleanup(func() { stopProcess(t, scheduler) })
	waitForTransactionDeadline(t, 3*time.Second, func() (bool, error) {
		var due bool
		err := blocker.QueryRow(ctx, `SELECT $1::timestamptz <= clock_timestamp()`, deadline).Scan(&due)
		return due, err
	})
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release task row blocker: %v", err)
	}
	select {
	case result := <-approvalDone:
		if result.err != nil {
			t.Fatalf("approval request during deadline race: %v", result.err)
		}
		if result.status != http.StatusConflict {
			t.Fatalf("approval request after deadline returned %d, want 409", result.status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("approval request remained blocked after releasing task row")
	}
	finished := waitForTask(t, server.URL, taskID, 5*time.Second, func(view taskView) bool { return view.Status == "TIMED_OUT" })
	if finished.Steps[0].ApprovalDecision != nil || finished.Steps[0].Status != "FAILED" || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "task_timeout" {
		t.Fatalf("deadline race changed approval decision or terminal result: %+v; scheduler logs=%s", finished, schedulerLogs)
	}
	assertEventCount(t, server.URL, taskID, "TASK_TIMED_OUT", 1)
	assertEventCount(t, server.URL, taskID, "TASK_APPROVED", 0)
}

func TestCancelDuringApprovalWaitEndsImmediately(t *testing.T) {
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
	taskID := createHTTPTask(t, server.URL, `{"steps":[{"type":"approval","input":{}},{"type":"echo","input":{"value":"leave pending"}}]}`)
	worker, _ := startWorkerProcess(t, workerBinary, database.url, "approval-cancel-worker")
	t.Cleanup(func() { stopProcess(t, worker) })
	waiting := waitForTask(t, server.URL, taskID, 3*time.Second, func(view taskView) bool { return view.Status == "WAITING_APPROVAL" })
	response, err := http.Post(server.URL+"/tasks/"+taskID+"/cancel", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("cancel approval wait: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("cancel approval wait status = %d, want 200", response.StatusCode)
	}
	finished := waitForTask(t, server.URL, taskID, time.Second, func(view taskView) bool { return view.Status == "CANCELLED" })
	if finished.WorkerID != nil || finished.Steps[0].Attempt != 0 || finished.Steps[0].Status != "FAILED" || finished.Steps[0].Error == nil || *finished.Steps[0].Error != "user_cancelled" || finished.Steps[1].Status != "PENDING" {
		t.Fatalf("cancelled approval wait = %+v", finished)
	}
	if status, _ := postApprovalDecision(t, server.URL, taskID, "approve", waiting.Steps[0].ID); status != http.StatusConflict {
		t.Fatalf("late approval after cancellation status = %d, want 409", status)
	}
	assertEventCount(t, server.URL, taskID, "TASK_CANCELLED", 1)
	assertEventCount(t, server.URL, taskID, "TASK_APPROVED", 0)
}

func waitForTaskRowLockWait(t *testing.T, database testDatabase, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var waiting bool
		err := database.pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE state = 'active' AND wait_event_type = 'Lock'
				  AND query LIKE '%lease_version%'
				  AND query LIKE '%FROM tasks WHERE id = $1 FOR UPDATE%'
			)`).Scan(&waiting)
		if err != nil {
			t.Fatalf("check approval task row lock wait: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("approval transaction did not wait on the locked task row")
}

func waitForTransactionDeadline(t *testing.T, within time.Duration, checkDue func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		due, err := checkDue()
		if err != nil {
			t.Fatalf("check approval deadline: %v", err)
		}
		if due {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("approval deadline did not become due before timeout")
}

func TestCompiledWorkerWaitsForApprovalAndReleasesItsSlot(t *testing.T) {
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
	approvalID := createHTTPTask(t, server.URL, `{"steps":[{"type":"approval","input":{}},{"type":"echo","input":{"value":"after approval"}}]}`)
	worker, logs := startWorkerProcess(t, workerBinary, database.url, "approval-worker")
	t.Cleanup(func() { stopProcess(t, worker) })

	waiting := waitForTask(t, server.URL, approvalID, 3*time.Second, func(view taskView) bool {
		return view.Status == "WAITING_APPROVAL" && view.Steps[0].Status == "WAITING_APPROVAL"
	})
	if waiting.WorkerID != nil || waiting.Steps[0].Attempt != 0 || waiting.Steps[0].StartedAt == nil {
		t.Fatalf("approval wait did not release ownership without consuming an attempt: %+v; logs=%s", waiting, logs)
	}

	otherID := createHTTPTask(t, server.URL, `{"steps":[{"type":"echo","input":{"value":"slot is free"}}]}`)
	waitForTask(t, server.URL, otherID, 2*time.Second, func(view taskView) bool { return view.Status == "SUCCEEDED" })
	assertEventCount(t, server.URL, approvalID, "TASK_WAITING_APPROVAL", 1)
}
