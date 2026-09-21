package task

import (
	"encoding/json"
	"time"
)

const (
	StatusRunnable  = "RUNNABLE"
	StatusRunning   = "RUNNING"
	StatusRetryWait = "RETRY_WAIT"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusCancelled = "CANCELLED"
	StatusTimedOut  = "TIMED_OUT"

	StepPending   = "PENDING"
	StepRunning   = "RUNNING"
	StepSucceeded = "SUCCEEDED"
	StepFailed    = "FAILED"
)

type Definition struct {
	TenantID           *string
	TaskTimeoutSeconds int
	Steps              []StepDefinition
}

type StepDefinition struct {
	Type           string
	Input          json.RawMessage
	TimeoutSeconds int
	MaxAttempts    int
}

type Task struct {
	ID                 string          `json:"id"`
	TenantID           *string         `json:"tenant_id,omitempty"`
	Status             string          `json:"status"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
	RunAt              time.Time       `json:"run_at"`
	RetryAt            *time.Time      `json:"retry_at,omitempty"`
	WorkerID           *string         `json:"worker_id,omitempty"`
	LeaseVersion       int64           `json:"lease_version"`
	LeaseExpiresAt     *time.Time      `json:"lease_expires_at,omitempty"`
	CancelRequested    bool            `json:"cancel_requested"`
	CurrentStep        int             `json:"current_step"`
	Checkpoint         json.RawMessage `json:"checkpoint"`
	TaskTimeoutSeconds int             `json:"task_timeout_seconds"`
	FirstStartedAt     *time.Time      `json:"first_started_at,omitempty"`
	DeadlineAt         *time.Time      `json:"deadline_at,omitempty"`
	Steps              []Step          `json:"steps"`
}

type Step struct {
	ID             string          `json:"id"`
	TaskID         string          `json:"task_id"`
	Index          int             `json:"index"`
	Type           string          `json:"type"`
	Status         string          `json:"status"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
	Input          json.RawMessage `json:"input"`
	ResolvedInput  json.RawMessage `json:"resolved_input,omitempty"`
	Output         json.RawMessage `json:"output,omitempty"`
	Error          *string         `json:"error,omitempty"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

type Event struct {
	ID        int64           `json:"id"`
	TaskID    string          `json:"task_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}
