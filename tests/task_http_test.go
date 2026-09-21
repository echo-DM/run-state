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

	"github.com/dimen61/runstate/internal/api"
	"github.com/dimen61/runstate/internal/migrations"
	"github.com/dimen61/runstate/internal/store"
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
