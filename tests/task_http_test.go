package tests_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
	"github.com/dimen61/runstate/internal/task"
	"github.com/jackc/pgx/v5/pgxpool"
)

var schemaSequence atomic.Uint64

func TestUserCreatesAndReadsFixedTask(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}

	server := httptest.NewServer(api.New(store.New(pool, store.Options{})))
	t.Cleanup(server.Close)

	body := []byte(`{"steps":[{"type":"echo","input":{"value":"first"}},{"type":"sleep","input":{"duration":"1ms"}}]}`)
	response, err := http.Post(server.URL+"/tasks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", response.StatusCode, http.StatusCreated)
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode created task: %v", err)
	}

	response, err = http.Get(server.URL + "/tasks/" + created.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, want %d", response.StatusCode, http.StatusOK)
	}

	var got struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		CurrentStep int    `json:"current_step"`
		Steps       []struct {
			Index  int    `json:"index"`
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"steps"`
	}
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode task: %v", err)
	}

	if got.ID != created.ID || got.Status != "RUNNABLE" || got.CurrentStep != 0 {
		t.Fatalf("task = %+v, want created RUNNABLE task at step 0", got)
	}
	if len(got.Steps) != 2 || got.Steps[0].Index != 0 || got.Steps[0].Type != "echo" || got.Steps[0].Status != "PENDING" || got.Steps[1].Index != 1 || got.Steps[1].Type != "sleep" || got.Steps[1].Status != "PENDING" {
		t.Fatalf("steps = %+v, want ordered pending echo and sleep steps", got.Steps)
	}
}

func TestUserCreatesFutureTaskWithExplicitTimezone(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(pool, store.Options{})))
	t.Cleanup(server.Close)

	runAt := time.Now().UTC().Add(2 * time.Minute).In(time.FixedZone("UTC+08", 8*60*60))
	body, err := json.Marshal(map[string]any{
		"run_at": runAt.Format(time.RFC3339Nano),
		"steps":  []map[string]any{{"type": "echo", "input": map[string]any{"value": "scheduled"}}},
	})
	if err != nil {
		t.Fatalf("encode scheduled task: %v", err)
	}
	response, err := http.Post(server.URL+"/tasks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create scheduled task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	var created task.Task
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode scheduled task: %v", err)
	}
	if created.Status != task.StatusScheduled || !created.RunAt.Equal(runAt) {
		t.Fatalf("created task status/run_at = %s/%s, want SCHEDULED/%s", created.Status, created.RunAt, runAt)
	}
	if created.FirstStartedAt != nil || created.DeadlineAt != nil || created.LeaseVersion != 0 {
		t.Fatalf("scheduled task began before its run_at: %+v", created)
	}
	eventsResponse, err := http.Get(server.URL + "/tasks/" + created.ID + "/events")
	if err != nil {
		t.Fatalf("get scheduled task events: %v", err)
	}
	defer eventsResponse.Body.Close()
	var events []task.Event
	if err := json.NewDecoder(eventsResponse.Body).Decode(&events); err != nil {
		t.Fatalf("decode scheduled task events: %v", err)
	}
	scheduledEvents := 0
	for _, event := range events {
		if event.Type == "TASK_SCHEDULED" {
			scheduledEvents++
		}
	}
	if scheduledEvents != 1 {
		t.Fatalf("TASK_SCHEDULED events = %d, want 1", scheduledEvents)
	}
}

func TestUserCreatesPastTaskRunnableWithoutStartingItsTimeout(t *testing.T) {
	pool := isolatedTestPool(t)
	if err := migrations.Run(context.Background(), pool); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}
	server := httptest.NewServer(api.New(store.New(pool, store.Options{})))
	t.Cleanup(server.Close)

	const runAtValue = "2001-02-03T04:05:06-05:00"
	response, err := http.Post(server.URL+"/tasks", "application/json", bytes.NewBufferString(
		`{"run_at":"`+runAtValue+`","steps":[{"type":"echo","input":{"value":"past"}}]}`,
	))
	if err != nil {
		t.Fatalf("create past task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	var created task.Task
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode past task: %v", err)
	}
	expectedRunAt, err := time.Parse(time.RFC3339Nano, runAtValue)
	if err != nil {
		t.Fatalf("parse expected run_at: %v", err)
	}
	if created.Status != task.StatusRunnable || !created.RunAt.Equal(expectedRunAt) {
		t.Fatalf("created past task status/run_at = %s/%s, want RUNNABLE/%s", created.Status, created.RunAt, expectedRunAt)
	}
	if created.FirstStartedAt != nil || created.DeadlineAt != nil || created.LeaseVersion != 0 {
		t.Fatalf("past run_at started task timeout before a claim: %+v", created)
	}
}

func isolatedTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return isolatedTestDatabase(t).pool
}

type testDatabase struct {
	pool *pgxpool.Pool
	url  string
}

func isolatedTestDatabase(t *testing.T) testDatabase {
	t.Helper()
	dsn := os.Getenv("RUNSTATE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("RUNSTATE_TEST_DATABASE_URL is required for PostgreSQL tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(admin.Close)
	var databaseName string
	if err := admin.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatalf("identify test database: %v", err)
	}
	if databaseName == "runstate_dev" {
		t.Fatal("RUNSTATE_TEST_DATABASE_URL must not point at runstate_dev")
	}

	schema := fmt.Sprintf("test_%d_%d", os.Getpid(), schemaSequence.Add(1))
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Fatalf("drop stale test schema: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open isolated test schema: %v", err)
	}
	t.Cleanup(pool.Close)
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse test database URL for subprocess: %v", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	parsedURL.RawQuery = query.Encode()
	return testDatabase{pool: pool, url: parsedURL.String()}
}
