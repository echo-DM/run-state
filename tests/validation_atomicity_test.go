package tests_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestInvalidWorkflowDefinitionsAreRejectedWithoutPersistence(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(pool, store.Options{})))
	t.Cleanup(server.Close)

	oneHundredOne := make([]string, 101)
	for index := range oneHundredOne {
		oneHundredOne[index] = `{"type":"echo","input":{"value":1}}`
	}
	cases := map[string]string{
		"empty sequence":             `{"steps":[]}`,
		"too many steps":             `{"steps":[` + strings.Join(oneHundredOne, ",") + `]}`,
		"unknown type":               `{"steps":[{"type":"missing","input":{}}]}`,
		"scheduled not implemented":  `{"run_at":null,"steps":[{"type":"echo","input":{"value":1}}]}`,
		"invalid normal input":       `{"steps":[{"type":"sleep","input":{"duration":"soon"}}]}`,
		"first step previous output": `{"steps":[{"type":"echo","input":{"value":{"$ref":"previous_output"}}}]}`,
		"unknown input reference":    `{"steps":[{"type":"echo","input":{"value":{"$ref":"future"}}}]}`,
		"zero step timeout":          `{"steps":[{"type":"echo","input":{"value":1},"timeout_seconds":0}]}`,
		"zero max attempts":          `{"steps":[{"type":"echo","input":{"value":1},"max_attempts":0}]}`,
		"zero task timeout":          `{"task_timeout_seconds":0,"steps":[{"type":"echo","input":{"value":1}}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			response, err := http.Post(server.URL+"/tasks", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatalf("post invalid workflow: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", response.StatusCode)
			}
		})
	}
	oversized := append([]byte(`{"steps":[{"type":"echo","input":{"value":"`), bytes.Repeat([]byte("x"), 1<<20)...)
	oversized = append(oversized, []byte(`"}}]}`)...)
	response, err := http.Post(server.URL+"/tasks", "application/json", bytes.NewReader(oversized))
	if err != nil {
		t.Fatalf("post oversized workflow: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized status = %d, want 400", response.StatusCode)
	}
	assertTableCount(t, pool, "tasks", 0)
}

func TestCreationAndCompletionRollBackWhenEventWriteFails(t *testing.T) {
	pool := isolatedTestPool(t)
	ctx := context.Background()
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	database := store.New(pool, store.Options{LeaseDuration: 5 * time.Second})
	definition := task.Definition{TaskTimeoutSeconds: 60, Steps: []task.StepDefinition{{
		Type: "echo", Input: json.RawMessage(`{"value":"atomic"}`), TimeoutSeconds: 5, MaxAttempts: 3,
	}}}

	installRejectingEventTrigger(t, pool, "TASK_CREATED")
	if _, err := database.CreateTask(ctx, definition); err == nil {
		t.Fatal("creation succeeded while TASK_CREATED event was rejected")
	}
	assertTableCount(t, pool, "tasks", 0)
	assertTableCount(t, pool, "task_steps", 0)
	dropRejectingEventTrigger(t, pool)

	created, err := database.CreateTask(ctx, definition)
	if err != nil {
		t.Fatalf("create task after removing trigger: %v", err)
	}
	claim, err := database.ClaimTask(ctx, "worker-a")
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	attempt, err := database.StartStep(ctx, claim.Token(), created.Steps[0].ID)
	if err != nil {
		t.Fatalf("start step: %v", err)
	}
	installRejectingEventTrigger(t, pool, "STEP_SUCCEEDED")
	if err := database.FinishStep(ctx, claim.Token(), attempt, store.StepOutcome{Output: json.RawMessage(`{"value":"atomic"}`)}); err == nil {
		t.Fatal("finish succeeded while STEP_SUCCEEDED event was rejected")
	}
	afterFailure, err := database.LoadTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("load rolled back task: %v", err)
	}
	if afterFailure.Status != "RUNNING" || afterFailure.CurrentStep != 0 || afterFailure.Steps[0].Status != "RUNNING" || afterFailure.Steps[0].Output != nil {
		t.Fatalf("partial finish escaped rollback: task=%+v steps=%+v", afterFailure, afterFailure.Steps)
	}
	dropRejectingEventTrigger(t, pool)
	if err := database.FinishStep(ctx, claim.Token(), attempt, store.StepOutcome{Output: json.RawMessage(`{"value":"atomic"}`)}); err != nil {
		t.Fatalf("finish after removing trigger: %v", err)
	}
}

func installRejectingEventTrigger(t *testing.T, pool *pgxpool.Pool, eventType string) {
	t.Helper()
	statement := fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION reject_selected_event() RETURNS trigger AS $trigger$
		BEGIN
			IF NEW.event_type = %s THEN
				RAISE EXCEPTION 'injected event failure';
			END IF;
			RETURN NEW;
		END;
		$trigger$ LANGUAGE plpgsql;
		DROP TRIGGER IF EXISTS reject_selected_event_trigger ON task_events;
		CREATE TRIGGER reject_selected_event_trigger BEFORE INSERT ON task_events
		FOR EACH ROW EXECUTE FUNCTION reject_selected_event();`, quoteLiteral(eventType))
	if _, err := pool.Exec(context.Background(), statement); err != nil {
		t.Fatalf("install rejecting event trigger: %v", err)
	}
}

func dropRejectingEventTrigger(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_selected_event_trigger ON task_events`); err != nil {
		t.Fatalf("drop rejecting event trigger: %v", err)
	}
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func assertTableCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s rows = %d, want %d", table, got, want)
	}
}
