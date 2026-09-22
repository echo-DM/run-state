package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dimen61/runstate/internal/id"
	"github.com/dimen61/runstate/internal/task"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrNoTask           = errors.New("no task available")
	ErrLeaseLost        = errors.New("lease lost")
	ErrConflict         = errors.New("conflict")
	ErrCancelled        = errors.New("task cancelled")
	ErrDeadlineExceeded = errors.New("task deadline exceeded")
)

const maxJSONBytes = 1 << 20

type LeaseToken struct {
	TaskID       string
	WorkerID     string
	LeaseVersion int64
}

type Claim struct {
	LeaseToken
	LeaseExpiresAt time.Time
	DeadlineAt     time.Time
}

func (claim Claim) Token() LeaseToken {
	return claim.LeaseToken
}

type StepAttempt struct {
	StepID         string
	TaskID         string
	StepIndex      int
	StepType       string
	Attempt        int
	MaxAttempts    int
	IdempotencyKey string
	ResolvedInput  json.RawMessage
	TimeoutSeconds int
}

type StepOutcome struct {
	Output       json.RawMessage
	Checkpoint   json.RawMessage
	Error        string
	FailureClass FailureClass
}

type FailureClass string

type CancelResult string

type ApprovalResult struct {
	Decision   string `json:"decision"`
	TaskStatus string `json:"task_status"`
}

const (
	CancelledSynchronously CancelResult = "cancelled"
	CancellationRequested  CancelResult = "cancellation_requested"
)

const (
	FailurePermanent FailureClass = "permanent"
	FailureTemporary FailureClass = "temporary"
)

type Options struct {
	LeaseDuration time.Duration
	// AfterCommit injects a lost commit response in PostgreSQL integration tests.
	// Runtime processes leave it nil.
	AfterCommit func(operation string) error
}

type Store struct {
	pool          *pgxpool.Pool
	leaseDuration time.Duration
	afterCommit   func(operation string) error
}

func New(pool *pgxpool.Pool, options Options) *Store {
	leaseDuration := options.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	return &Store{pool: pool, leaseDuration: leaseDuration, afterCommit: options.AfterCommit}
}

func (s *Store) CreateTask(ctx context.Context, definition task.Definition) (task.Task, error) {
	taskID, err := id.New()
	if err != nil {
		return task.Task{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return task.Task{}, fmt.Errorf("begin create task: %w", err)
	}
	defer tx.Rollback(ctx)

	var created task.Task
	var runAt any
	if definition.RunAt != nil {
		runAt = *definition.RunAt
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO tasks (id, tenant_id, status, run_at, task_timeout_seconds)
		SELECT $1, $2,
		       CASE WHEN $3::timestamptz > database_time.now THEN 'SCHEDULED' ELSE 'RUNNABLE' END,
		       COALESCE($3::timestamptz, database_time.now), $4
		FROM (SELECT clock_timestamp() AS now) AS database_time
		RETURNING id, tenant_id, status, created_at, updated_at, run_at, retry_at, worker_id,
		          lease_version, lease_expires_at, cancel_requested, current_step,
		          checkpoint, task_timeout_seconds, first_started_at, deadline_at`,
		taskID, definition.TenantID, runAt, definition.TaskTimeoutSeconds,
	).Scan(&created.ID, &created.TenantID, &created.Status, &created.CreatedAt, &created.UpdatedAt,
		&created.RunAt, &created.RetryAt, &created.WorkerID, &created.LeaseVersion, &created.LeaseExpiresAt,
		&created.CancelRequested, &created.CurrentStep, &created.Checkpoint,
		&created.TaskTimeoutSeconds, &created.FirstStartedAt, &created.DeadlineAt)
	if err != nil {
		return task.Task{}, fmt.Errorf("insert task: %w", err)
	}

	for index, definition := range definition.Steps {
		stepID, err := id.New()
		if err != nil {
			return task.Task{}, err
		}
		key := taskID + ":" + stepID
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_steps (
				id, task_id, step_index, step_type, status, max_attempts,
				idempotency_key, input, timeout_seconds
			) VALUES ($1, $2, $3, $4, 'PENDING', $5, $6, $7, $8)`,
			stepID, taskID, index, definition.Type, definition.MaxAttempts,
			key, definition.Input, definition.TimeoutSeconds,
		); err != nil {
			return task.Task{}, fmt.Errorf("insert step %d: %w", index, err)
		}
	}
	payload, _ := json.Marshal(map[string]any{"step_count": len(definition.Steps)})
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'TASK_CREATED', $2)`, taskID, payload); err != nil {
		return task.Task{}, fmt.Errorf("insert task created event: %w", err)
	}
	if created.Status == task.StatusScheduled {
		scheduledPayload, _ := json.Marshal(map[string]any{"run_at": created.RunAt})
		if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'TASK_SCHEDULED', $2)`, taskID, scheduledPayload); err != nil {
			return task.Task{}, fmt.Errorf("insert task scheduled event: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return task.Task{}, fmt.Errorf("commit create task: %w", err)
	}
	return s.LoadTask(ctx, taskID)
}

func (s *Store) LoadTask(ctx context.Context, taskID string) (task.Task, error) {
	var loaded task.Task
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, status, created_at, updated_at, run_at, retry_at, worker_id,
		       lease_version, lease_expires_at, cancel_requested, current_step,
		       checkpoint, task_timeout_seconds, first_started_at, deadline_at
		FROM tasks WHERE id = $1`, taskID,
	).Scan(&loaded.ID, &loaded.TenantID, &loaded.Status, &loaded.CreatedAt, &loaded.UpdatedAt,
		&loaded.RunAt, &loaded.RetryAt, &loaded.WorkerID, &loaded.LeaseVersion, &loaded.LeaseExpiresAt,
		&loaded.CancelRequested, &loaded.CurrentStep, &loaded.Checkpoint,
		&loaded.TaskTimeoutSeconds, &loaded.FirstStartedAt, &loaded.DeadlineAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return task.Task{}, ErrNotFound
	}
	if err != nil {
		return task.Task{}, fmt.Errorf("load task: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, task_id, step_index, step_type, status, attempt, max_attempts,
		       idempotency_key, input, resolved_input, output, error, approval_decision,
		       timeout_seconds, started_at, finished_at
		FROM task_steps WHERE task_id = $1 ORDER BY step_index`, taskID)
	if err != nil {
		return task.Task{}, fmt.Errorf("load task steps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var step task.Step
		if err := rows.Scan(&step.ID, &step.TaskID, &step.Index, &step.Type, &step.Status,
			&step.Attempt, &step.MaxAttempts, &step.IdempotencyKey, &step.Input,
			&step.ResolvedInput, &step.Output, &step.Error, &step.ApprovalDecision, &step.TimeoutSeconds,
			&step.StartedAt, &step.FinishedAt); err != nil {
			return task.Task{}, fmt.Errorf("scan task step: %w", err)
		}
		loaded.Steps = append(loaded.Steps, step)
	}
	if err := rows.Err(); err != nil {
		return task.Task{}, fmt.Errorf("read task steps: %w", err)
	}
	return loaded, nil
}

func (s *Store) ClaimTask(ctx context.Context, workerID string) (Claim, error) {
	if workerID == "" {
		return Claim{}, errors.New("worker id is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Claim{}, fmt.Errorf("begin claim: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return Claim{}, err
	}

	var taskID, previousStatus string
	var previousWorkerID *string
	var previousVersion int64
	var currentStep int
	err = tx.QueryRow(ctx, `
		SELECT id, status, worker_id, lease_version, current_step
		FROM tasks
		WHERE (
		        (status = 'RUNNABLE' AND run_at <= clock_timestamp())
		        OR (status = 'RUNNING' AND lease_expires_at <= clock_timestamp())
		      )
		  AND cancel_requested = false
		  AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`).Scan(&taskID, &previousStatus, &previousWorkerID, &previousVersion, &currentStep)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, ErrNoTask
	}
	if err != nil {
		return Claim{}, fmt.Errorf("select claim candidate: %w", err)
	}
	if previousStatus == task.StatusRunning {
		if err := s.recoverExpiredTask(ctx, tx, taskID, currentStep, previousWorkerID, previousVersion); err != nil {
			if errors.Is(err, errRecoveryExhausted) {
				if commitErr := tx.Commit(ctx); commitErr != nil {
					return Claim{}, fmt.Errorf("commit exhausted recovery: %w", commitErr)
				}
				return Claim{}, ErrNoTask
			}
			return Claim{}, err
		}
	}

	leaseMicroseconds := s.leaseDuration.Microseconds()
	var claim Claim
	claim.LeaseToken = LeaseToken{TaskID: taskID, WorkerID: workerID}
	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET status = 'RUNNING',
		    worker_id = $2,
		    lease_version = lease_version + 1,
		    lease_expires_at = clock_timestamp() + ($3 * interval '1 microsecond'),
		    first_started_at = COALESCE(first_started_at, statement_timestamp()),
		    deadline_at = COALESCE(deadline_at, statement_timestamp() + (task_timeout_seconds * interval '1 second')),
		    updated_at = clock_timestamp()
		WHERE id = $1
		RETURNING lease_version, lease_expires_at, deadline_at`, taskID, workerID, leaseMicroseconds,
	).Scan(&claim.LeaseVersion, &claim.LeaseExpiresAt, &claim.DeadlineAt)
	if err != nil {
		return Claim{}, fmt.Errorf("assign lease: %w", err)
	}
	payload, _ := json.Marshal(map[string]any{"worker_id": workerID, "lease_version": claim.LeaseVersion})
	if previousStatus == task.StatusRunning {
		recoveredPayload, _ := json.Marshal(map[string]any{
			"previous_worker_id": previousWorkerID, "previous_lease_version": previousVersion,
			"worker_id": workerID, "lease_version": claim.LeaseVersion,
		})
		if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'TASK_RECOVERED', $2)`, taskID, recoveredPayload); err != nil {
			return Claim{}, fmt.Errorf("insert recovered event: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'TASK_CLAIMED', $2)`, taskID, payload); err != nil {
		return Claim{}, fmt.Errorf("insert claimed event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Claim{}, fmt.Errorf("commit claim: %w", err)
	}
	return claim, nil
}

var errRecoveryExhausted = errors.New("recovery attempt budget exhausted")

type controlOutcome struct {
	status    string
	reason    string
	eventType string
}

var (
	cancelledOutcome = controlOutcome{status: task.StatusCancelled, reason: "user_cancelled", eventType: "TASK_CANCELLED"}
	timedOutOutcome  = controlOutcome{status: task.StatusTimedOut, reason: "task_timeout", eventType: "TASK_TIMED_OUT"}
)

func (s *Store) recoverExpiredTask(ctx context.Context, tx pgx.Tx, taskID string, currentStep int, previousWorkerID *string, previousVersion int64) error {
	payload, _ := json.Marshal(map[string]any{
		"worker_id": previousWorkerID, "lease_version": previousVersion,
	})
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'LEASE_EXPIRED', $2)`, taskID, payload); err != nil {
		return fmt.Errorf("insert lease expired event: %w", err)
	}

	var stepID, stepStatus string
	var attempt, maxAttempts int
	err := tx.QueryRow(ctx, `
		SELECT id, status, attempt, max_attempts
		FROM task_steps
		WHERE task_id = $1 AND step_index = $2
		FOR UPDATE`, taskID, currentStep,
	).Scan(&stepID, &stepStatus, &attempt, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err := tx.Exec(ctx, `
			UPDATE tasks SET status = 'SUCCEEDED', worker_id = NULL,
			lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE id = $1`, taskID); err != nil {
			return fmt.Errorf("converge recovered task: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type) VALUES ($1, 'TASK_SUCCEEDED')`, taskID); err != nil {
			return fmt.Errorf("insert recovered success event: %w", err)
		}
		return errRecoveryExhausted
	}
	if err != nil {
		return fmt.Errorf("lock recovery step: %w", err)
	}
	if stepStatus != task.StepRunning {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_steps
		SET status = 'FAILED', error = 'worker_lost', finished_at = clock_timestamp()
		WHERE task_id = $1 AND id = $2`, taskID, stepID); err != nil {
		return fmt.Errorf("abandon running step: %w", err)
	}
	stepPayload, _ := json.Marshal(map[string]any{
		"step_id": stepID, "step_index": currentStep, "attempt": attempt,
		"error": "worker_lost", "worker_id": previousWorkerID, "lease_version": previousVersion,
	})
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'STEP_FAILED', $2)`, taskID, stepPayload); err != nil {
		return fmt.Errorf("insert abandoned step event: %w", err)
	}
	if attempt < maxAttempts {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'FAILED', worker_id = NULL, lease_expires_at = NULL,
		    updated_at = clock_timestamp()
		WHERE id = $1`, taskID); err != nil {
		return fmt.Errorf("fail exhausted recovered task: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'TASK_FAILED', $2)`, taskID, stepPayload); err != nil {
		return fmt.Errorf("insert exhausted recovery event: %w", err)
	}
	return errRecoveryExhausted
}

func (s *Store) LoadEvents(ctx context.Context, taskID string) ([]task.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, task_id, event_type, payload, created_at
		FROM task_events WHERE task_id = $1 ORDER BY id`, taskID)
	if err != nil {
		return nil, fmt.Errorf("load task events: %w", err)
	}
	defer rows.Close()
	var events []task.Event
	for rows.Next() {
		var event task.Event
		if err := rows.Scan(&event.ID, &event.TaskID, &event.Type, &event.Payload, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan task event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read task events: %w", err)
	}
	return events, nil
}

func (s *Store) RenewLease(ctx context.Context, lease LeaseToken) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin renew lease: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return err
	}
	if _, err := lockOwnedTask(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET lease_expires_at = clock_timestamp() + ($2 * interval '1 microsecond'),
		    updated_at = clock_timestamp()
		WHERE id = $1`, lease.TaskID, s.leaseDuration.Microseconds()); err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit lease renewal: %w", err)
	}
	return nil
}

func (s *Store) StartStep(ctx context.Context, lease LeaseToken, stepID string) (StepAttempt, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return StepAttempt{}, fmt.Errorf("begin start step: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return StepAttempt{}, err
	}
	locked, err := lockOwnedTask(ctx, tx, lease)
	if err != nil {
		return StepAttempt{}, err
	}

	var attempt StepAttempt
	attempt.TaskID = lease.TaskID
	var configuredInput, persistedResolvedInput json.RawMessage
	var status string
	err = tx.QueryRow(ctx, `
		SELECT id, step_index, step_type, attempt, max_attempts, status,
		       idempotency_key, timeout_seconds, input, resolved_input
		FROM task_steps
		WHERE task_id = $1 AND id = $2
		FOR UPDATE`, lease.TaskID, stepID,
	).Scan(&attempt.StepID, &attempt.StepIndex, &attempt.StepType, &attempt.Attempt,
		&attempt.MaxAttempts, &status, &attempt.IdempotencyKey, &attempt.TimeoutSeconds,
		&configuredInput, &persistedResolvedInput)
	if errors.Is(err, pgx.ErrNoRows) {
		return StepAttempt{}, ErrConflict
	}
	if err != nil {
		return StepAttempt{}, fmt.Errorf("lock step: %w", err)
	}

	if attempt.StepType == task.StepTypeApproval {
		return StepAttempt{}, ErrConflict
	}
	if attempt.StepIndex != locked.currentStep || (status != task.StepPending && status != task.StepFailed) || attempt.Attempt >= attempt.MaxAttempts {
		return StepAttempt{}, ErrConflict
	}
	var unfinishedPredecessors bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM task_steps
			WHERE task_id = $1 AND step_index < $2 AND status <> 'SUCCEEDED'
		)`, lease.TaskID, attempt.StepIndex).Scan(&unfinishedPredecessors); err != nil {
		return StepAttempt{}, fmt.Errorf("check step predecessors: %w", err)
	}
	if unfinishedPredecessors {
		return StepAttempt{}, ErrConflict
	}
	resolvedInput := persistedResolvedInput
	if len(resolvedInput) == 0 {
		var previousOutput json.RawMessage
		if attempt.StepIndex > 0 {
			if err := tx.QueryRow(ctx, `
				SELECT output FROM task_steps
				WHERE task_id = $1 AND step_index = $2 AND status = 'SUCCEEDED'`,
				lease.TaskID, attempt.StepIndex-1,
			).Scan(&previousOutput); err != nil {
				return StepAttempt{}, fmt.Errorf("load previous step output: %w", err)
			}
		}
		resolvedInput, err = task.ResolveInput(configuredInput, previousOutput, locked.checkpoint)
		if err != nil {
			return StepAttempt{}, fmt.Errorf("resolve step input: %w", err)
		}
	}
	err = tx.QueryRow(ctx, `
		UPDATE task_steps
		SET status = 'RUNNING',
		    attempt = attempt + 1,
		    resolved_input = $3,
		    error = NULL,
		    started_at = clock_timestamp(),
		    finished_at = NULL
		WHERE task_id = $1 AND id = $2
		RETURNING attempt, resolved_input`, lease.TaskID, stepID, resolvedInput,
	).Scan(&attempt.Attempt, &attempt.ResolvedInput)
	if err != nil {
		return StepAttempt{}, fmt.Errorf("start step: %w", err)
	}
	payload, _ := json.Marshal(map[string]any{
		"step_id": stepID, "step_index": attempt.StepIndex, "attempt": attempt.Attempt,
		"worker_id": lease.WorkerID, "lease_version": lease.LeaseVersion,
	})
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'STEP_STARTED', $2)`, lease.TaskID, payload); err != nil {
		return StepAttempt{}, fmt.Errorf("insert step started event: %w", err)
	}
	if err := s.commitWithFactCheck(ctx, tx, "start_step", func(confirmCtx context.Context) (bool, error) {
		return s.stepStartPersisted(confirmCtx, attempt)
	}); err != nil {
		return StepAttempt{}, err
	}
	return attempt, nil
}

// WaitForApproval persists the current approval step and releases the worker's
// lease in one transaction. Approval is a control step and does not consume an
// execution attempt.
func (s *Store) WaitForApproval(ctx context.Context, lease LeaseToken, stepID string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin approval wait: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return err
	}
	locked, err := lockOwnedTask(ctx, tx, lease)
	if err != nil {
		return err
	}

	var stepIndex, attempt int
	var stepType, status string
	var configuredInput, persistedResolvedInput json.RawMessage
	err = tx.QueryRow(ctx, `
		SELECT step_index, step_type, status, attempt, input, resolved_input
		FROM task_steps WHERE task_id = $1 AND id = $2 FOR UPDATE`, lease.TaskID, stepID,
	).Scan(&stepIndex, &stepType, &status, &attempt, &configuredInput, &persistedResolvedInput)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrConflict
	}
	if err != nil {
		return fmt.Errorf("lock approval step: %w", err)
	}
	if stepType != task.StepTypeApproval || stepIndex != locked.currentStep || status != task.StepPending || attempt != 0 {
		return ErrConflict
	}
	var unfinishedPredecessors bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM task_steps
			WHERE task_id = $1 AND step_index < $2 AND status <> 'SUCCEEDED'
		)`, lease.TaskID, stepIndex).Scan(&unfinishedPredecessors); err != nil {
		return fmt.Errorf("check approval predecessors: %w", err)
	}
	if unfinishedPredecessors {
		return ErrConflict
	}

	resolvedInput := persistedResolvedInput
	if len(resolvedInput) == 0 {
		var previousOutput json.RawMessage
		if stepIndex > 0 {
			if err := tx.QueryRow(ctx, `
				SELECT output FROM task_steps
				WHERE task_id = $1 AND step_index = $2 AND status = 'SUCCEEDED'`,
				lease.TaskID, stepIndex-1,
			).Scan(&previousOutput); err != nil {
				return fmt.Errorf("load previous step output for approval: %w", err)
			}
		}
		resolvedInput, err = task.ResolveInput(configuredInput, previousOutput, locked.checkpoint)
		if err != nil {
			return fmt.Errorf("resolve approval input: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_steps
		SET status = 'WAITING_APPROVAL', resolved_input = $3, error = NULL,
		    started_at = clock_timestamp(), finished_at = NULL
		WHERE task_id = $1 AND id = $2`, lease.TaskID, stepID, resolvedInput); err != nil {
		return fmt.Errorf("persist approval wait: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'WAITING_APPROVAL', worker_id = NULL, lease_expires_at = NULL,
		    retry_at = NULL, updated_at = clock_timestamp()
		WHERE id = $1`, lease.TaskID); err != nil {
		return fmt.Errorf("release approval task lease: %w", err)
	}
	payload, _ := json.Marshal(map[string]any{"step_id": stepID, "step_index": stepIndex})
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_events (task_id, event_type, payload)
		VALUES ($1, 'TASK_WAITING_APPROVAL', $2)`, lease.TaskID, payload); err != nil {
		return fmt.Errorf("insert approval wait event: %w", err)
	}
	return s.commitWithFactCheck(ctx, tx, "wait_approval", func(confirmCtx context.Context) (bool, error) {
		var persisted bool
		err := s.pool.QueryRow(confirmCtx, `
			SELECT EXISTS (
				SELECT 1 FROM tasks task
				JOIN task_steps step ON step.task_id = task.id
				WHERE task.id = $1 AND task.status = 'WAITING_APPROVAL'
				  AND step.id = $2 AND step.status = 'WAITING_APPROVAL'
				  AND EXISTS (
					SELECT 1 FROM task_events event
					WHERE event.task_id = $1 AND event.event_type = 'TASK_WAITING_APPROVAL'
					  AND event.payload->>'step_id' = $2
				  )
			)`, lease.TaskID, stepID).Scan(&persisted)
		return persisted, err
	})
}

// DecideApproval records one decision for the task's current waiting approval
// step. A matching replay returns the original decision and current task state.
func (s *Store) DecideApproval(ctx context.Context, taskID, stepID, decision string) (ApprovalResult, error) {
	if decision != "approved" && decision != "rejected" {
		return ApprovalResult{}, ErrConflict
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ApprovalResult{}, fmt.Errorf("begin approval decision: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return ApprovalResult{}, err
	}
	locked, err := lockTask(ctx, tx, taskID)
	if err != nil {
		return ApprovalResult{}, err
	}

	var stepIndex int
	var stepType, stepStatus string
	var persistedDecision *string
	err = tx.QueryRow(ctx, `
		SELECT step_index, step_type, status, approval_decision
		FROM task_steps WHERE task_id = $1 AND id = $2 FOR UPDATE`, taskID, stepID,
	).Scan(&stepIndex, &stepType, &stepStatus, &persistedDecision)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApprovalResult{}, ErrNotFound
	}
	if err != nil {
		return ApprovalResult{}, fmt.Errorf("lock approval decision step: %w", err)
	}
	if persistedDecision != nil {
		if *persistedDecision != decision {
			return ApprovalResult{}, ErrConflict
		}
		return ApprovalResult{Decision: *persistedDecision, TaskStatus: locked.status}, nil
	}
	if locked.status != task.StatusWaitingApproval || stepType != task.StepTypeApproval ||
		stepStatus != task.StepWaitingApproval || stepIndex != locked.currentStep {
		return ApprovalResult{}, ErrConflict
	}
	if locked.cancelRequested || (locked.deadlineAt != nil && !locked.deadlineAt.After(locked.databaseNow)) {
		return ApprovalResult{}, ErrConflict
	}

	result := ApprovalResult{Decision: decision}
	payload, _ := json.Marshal(map[string]any{"step_id": stepID, "step_index": stepIndex, "decision": decision})
	if decision == "approved" {
		var stepCount int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_steps WHERE task_id = $1`, taskID).Scan(&stepCount); err != nil {
			return ApprovalResult{}, fmt.Errorf("count approval task steps: %w", err)
		}
		nextStep := stepIndex + 1
		result.TaskStatus = task.StatusRunnable
		if nextStep == stepCount {
			result.TaskStatus = task.StatusSucceeded
		}
		if _, err := tx.Exec(ctx, `
			UPDATE task_steps
			SET approval_decision = 'approved', status = 'SUCCEEDED', error = NULL,
			    finished_at = clock_timestamp()
			WHERE task_id = $1 AND id = $2`, taskID, stepID); err != nil {
			return ApprovalResult{}, fmt.Errorf("approve step: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tasks
			SET status = $2, current_step = $3, worker_id = NULL, lease_expires_at = NULL,
			    retry_at = NULL, updated_at = clock_timestamp()
			WHERE id = $1`, taskID, result.TaskStatus, nextStep); err != nil {
			return ApprovalResult{}, fmt.Errorf("advance approved task: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_events (task_id, event_type, payload)
			VALUES ($1, 'TASK_APPROVED', $2)`, taskID, payload); err != nil {
			return ApprovalResult{}, fmt.Errorf("insert approval event: %w", err)
		}
		if result.TaskStatus == task.StatusSucceeded {
			if _, err := tx.Exec(ctx, `
				INSERT INTO task_events (task_id, event_type, payload)
				VALUES ($1, 'TASK_SUCCEEDED', $2)`, taskID, payload); err != nil {
				return ApprovalResult{}, fmt.Errorf("insert approved task success event: %w", err)
			}
		}
	} else {
		result.TaskStatus = task.StatusFailed
		if _, err := tx.Exec(ctx, `
			UPDATE task_steps
			SET approval_decision = 'rejected', status = 'FAILED', error = 'approval_rejected',
			    finished_at = clock_timestamp()
			WHERE task_id = $1 AND id = $2`, taskID, stepID); err != nil {
			return ApprovalResult{}, fmt.Errorf("reject step: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tasks
			SET status = 'FAILED', worker_id = NULL, lease_expires_at = NULL,
			    retry_at = NULL, updated_at = clock_timestamp()
			WHERE id = $1`, taskID); err != nil {
			return ApprovalResult{}, fmt.Errorf("fail rejected task: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_events (task_id, event_type, payload)
			VALUES ($1, 'TASK_REJECTED', $2)`, taskID, payload); err != nil {
			return ApprovalResult{}, fmt.Errorf("insert rejection event: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_events (task_id, event_type, payload)
			VALUES ($1, 'TASK_FAILED', $2)`, taskID, payload); err != nil {
			return ApprovalResult{}, fmt.Errorf("insert rejected task failure event: %w", err)
		}
	}
	stepStatus = task.StepSucceeded
	eventType := "TASK_APPROVED"
	if decision == "rejected" {
		stepStatus = task.StepFailed
		eventType = "TASK_REJECTED"
	}
	if err := s.commitWithFactCheck(ctx, tx, "decide_approval", func(confirmCtx context.Context) (bool, error) {
		var persisted bool
		err := s.pool.QueryRow(confirmCtx, `
			SELECT EXISTS (
				SELECT 1 FROM task_steps step
				JOIN tasks task ON task.id = step.task_id
				WHERE step.task_id = $1 AND step.id = $2
				  AND step.approval_decision = $3 AND step.status = $4
				  AND (
					($3 = 'approved' AND task.current_step > step.step_index)
					OR ($3 = 'rejected' AND task.status = 'FAILED' AND task.current_step = step.step_index)
				  )
				  AND EXISTS (
					SELECT 1 FROM task_events event
					WHERE event.task_id = $1 AND event.event_type = $5
					  AND event.payload->>'step_id' = $2
					  AND event.payload->>'decision' = $3
				  )
			)
		`, taskID, stepID, decision, stepStatus, eventType).Scan(&persisted)
		return persisted, err
	}); err != nil {
		return ApprovalResult{}, err
	}
	return result, nil
}

func (s *Store) FinishStep(ctx context.Context, lease LeaseToken, attempt StepAttempt, outcome StepOutcome) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin finish step: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return err
	}
	locked, err := lockTask(ctx, tx, lease.TaskID)
	if err != nil {
		return err
	}

	if err := validateOwnership(locked, lease); err != nil {
		return err
	}

	var stepStatus string
	var persistedAttempt, persistedMaxAttempts, stepIndex int
	err = tx.QueryRow(ctx, `
		SELECT status, attempt, max_attempts, step_index FROM task_steps
		WHERE task_id = $1 AND id = $2 FOR UPDATE`, lease.TaskID, attempt.StepID,
	).Scan(&stepStatus, &persistedAttempt, &persistedMaxAttempts, &stepIndex)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrConflict
	}
	if err != nil {
		return fmt.Errorf("lock finishing step: %w", err)
	}
	if stepStatus != task.StepRunning || persistedAttempt != attempt.Attempt || stepIndex != locked.currentStep {
		return ErrConflict
	}
	attempt.MaxAttempts = persistedMaxAttempts
	if len(outcome.Output) > maxJSONBytes {
		outcome.Error = "output_too_large"
		outcome.Output = nil
	}
	if len(outcome.Checkpoint) > maxJSONBytes {
		outcome.Error = "checkpoint_too_large"
		outcome.Output = nil
		outcome.Checkpoint = nil
	}
	if outcome.Error != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE task_steps
			SET status = 'FAILED', output = NULL, error = $3, finished_at = clock_timestamp()
			WHERE task_id = $1 AND id = $2`, lease.TaskID, attempt.StepID, outcome.Error); err != nil {
			return fmt.Errorf("fail step: %w", err)
		}
		retrying := outcome.FailureClass == FailureTemporary && attempt.Attempt < attempt.MaxAttempts
		if retrying {
			delaySeconds := 1 << min(attempt.Attempt-1, 6)
			if delaySeconds > 60 {
				delaySeconds = 60
			}
			if _, err := tx.Exec(ctx, `
				UPDATE tasks
				SET status = 'RETRY_WAIT', retry_at = clock_timestamp() + ($2 * interval '1 second'),
				    worker_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
				WHERE id = $1`, lease.TaskID, delaySeconds); err != nil {
				return fmt.Errorf("wait to retry task: %w", err)
			}
		} else if _, err := tx.Exec(ctx, `
			UPDATE tasks
			SET status = 'FAILED', retry_at = NULL, worker_id = NULL, lease_expires_at = NULL,
			    updated_at = clock_timestamp()
			WHERE id = $1`, lease.TaskID); err != nil {
			return fmt.Errorf("fail task: %w", err)
		}
		if err := insertFinishEvents(ctx, tx, lease, attempt, "STEP_FAILED", outcome.Error, !retrying); err != nil {
			return err
		}
		if retrying {
			payload, _ := json.Marshal(map[string]any{"step_id": attempt.StepID, "attempt": attempt.Attempt})
			if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'TASK_RETRY_WAIT', $2)`, lease.TaskID, payload); err != nil {
				return fmt.Errorf("insert retry wait event: %w", err)
			}
		}
		return s.commitWithFactCheck(ctx, tx, "finish_step", func(confirmCtx context.Context) (bool, error) {
			return s.stepFinishPersisted(confirmCtx, attempt, outcome)
		})
	}
	if len(outcome.Output) == 0 || !json.Valid(outcome.Output) {
		return errors.New("step output must be valid JSON")
	}
	if len(outcome.Checkpoint) > 0 && !json.Valid(outcome.Checkpoint) {
		return errors.New("checkpoint must be valid JSON")
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_steps
		SET status = 'SUCCEEDED', output = $3, error = NULL, finished_at = clock_timestamp()
		WHERE task_id = $1 AND id = $2`, lease.TaskID, attempt.StepID, outcome.Output); err != nil {
		return fmt.Errorf("succeed step: %w", err)
	}
	var stepCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_steps WHERE task_id = $1`, lease.TaskID).Scan(&stepCount); err != nil {
		return fmt.Errorf("count task steps: %w", err)
	}
	nextStep := stepIndex + 1
	terminal := nextStep == stepCount
	if terminal {
		if _, err := tx.Exec(ctx, `
			UPDATE tasks
			SET status = 'SUCCEEDED', current_step = $2,
			    checkpoint = COALESCE($3, checkpoint), worker_id = NULL,
			    lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE id = $1`, lease.TaskID, nextStep, nullableJSON(outcome.Checkpoint)); err != nil {
			return fmt.Errorf("complete task: %w", err)
		}
	} else if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET current_step = $2, checkpoint = COALESCE($3, checkpoint), updated_at = clock_timestamp()
		WHERE id = $1`, lease.TaskID, nextStep, nullableJSON(outcome.Checkpoint)); err != nil {
		return fmt.Errorf("advance task: %w", err)
	}
	if err := insertFinishEvents(ctx, tx, lease, attempt, "STEP_SUCCEEDED", "", terminal); err != nil {
		return err
	}
	return s.commitWithFactCheck(ctx, tx, "finish_step", func(confirmCtx context.Context) (bool, error) {
		return s.stepFinishPersisted(confirmCtx, attempt, outcome)
	})
}

// TimeoutOwnedTask lets the current fenced owner persist the task deadline
// transition using a control context that is independent of execution.
func (s *Store) TimeoutOwnedTask(ctx context.Context, lease LeaseToken) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin owned timeout: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return err
	}
	locked, err := lockTask(ctx, tx, lease.TaskID)
	if err != nil {
		return err
	}
	if locked.status == task.StatusTimedOut {
		return nil
	}
	if !ownsActiveLease(locked, lease) {
		return ErrLeaseLost
	}
	if locked.cancelRequested {
		return ErrCancelled
	}
	if locked.deadlineAt == nil || locked.deadlineAt.After(locked.databaseNow) {
		return ErrConflict
	}
	if err := finishControlledTask(ctx, tx, lease.TaskID, locked.currentStep, timedOutOutcome); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit owned timeout: %w", err)
	}
	return nil
}

// CancelTask records a running cancellation request or atomically terminates a
// non-running task. A repeated cancellation is idempotent.
func (s *Store) CancelTask(ctx context.Context, taskID string) (CancelResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("begin cancellation: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return "", err
	}
	locked, err := lockTask(ctx, tx, taskID)
	if err != nil {
		return "", err
	}
	if locked.status == task.StatusCancelled {
		return CancelledSynchronously, nil
	}
	if isTerminalStatus(locked.status) {
		return "", ErrConflict
	}
	if locked.status == task.StatusRunning {
		if _, err := tx.Exec(ctx, `UPDATE tasks SET cancel_requested = true, updated_at = clock_timestamp() WHERE id = $1`, taskID); err != nil {
			return "", fmt.Errorf("record cancellation request: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("commit cancellation request: %w", err)
		}
		return CancellationRequested, nil
	}
	if err := finishControlledTask(ctx, tx, taskID, locked.currentStep, cancelledOutcome); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit synchronous cancellation: %w", err)
	}
	return CancelledSynchronously, nil
}

// CancelOwnedTask lets the currently fenced owner turn its persisted request
// into the terminal cancellation using an execution-independent context.
func (s *Store) CancelOwnedTask(ctx context.Context, lease LeaseToken) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin owned cancellation: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return err
	}
	locked, err := lockTask(ctx, tx, lease.TaskID)
	if err != nil {
		return err
	}
	if locked.status == task.StatusCancelled {
		return nil
	}
	if !ownsActiveLease(locked, lease) {
		return ErrLeaseLost
	}
	if !locked.cancelRequested {
		return ErrConflict
	}
	if err := finishControlledTask(ctx, tx, lease.TaskID, locked.currentStep, cancelledOutcome); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit owned cancellation: %w", err)
	}
	return nil
}

// CancellationRequested checks the current owner's durable control state
// without renewing its lease.
func (s *Store) CancellationRequested(ctx context.Context, lease LeaseToken) (bool, error) {
	var locked lockedTask
	err := s.pool.QueryRow(ctx, `
		SELECT status, worker_id, lease_version, lease_expires_at, cancel_requested,
		       deadline_at, clock_timestamp(), current_step, checkpoint
		FROM tasks WHERE id = $1`, lease.TaskID,
	).Scan(&locked.status, &locked.workerID, &locked.leaseVersion, &locked.leaseExpiresAt,
		&locked.cancelRequested, &locked.deadlineAt, &locked.databaseNow, &locked.currentStep,
		&locked.checkpoint)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("check cancellation: %w", err)
	}
	if !ownsActiveLease(locked, lease) {
		return false, ErrLeaseLost
	}
	return locked.cancelRequested, nil
}

func isTerminalStatus(status string) bool {
	switch status {
	case task.StatusSucceeded, task.StatusFailed, task.StatusCancelled, task.StatusTimedOut:
		return true
	default:
		return false
	}
}

// ProcessDueControl performs one scheduler transition. Candidate ordering is
// cancellation, task deadline, then scheduled or retry wake-up.
func (s *Store) ProcessDueControl(ctx context.Context) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin due control: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return false, err
	}
	var taskID, status string
	var currentStep int
	var cancelRequested bool
	var deadlineAt, retryAt *time.Time
	var runAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, status, current_step, cancel_requested, deadline_at, retry_at, run_at
		FROM tasks
		WHERE status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
		  AND (cancel_requested
		       OR (deadline_at IS NOT NULL AND deadline_at <= clock_timestamp())
		       OR (status = 'RETRY_WAIT' AND retry_at <= clock_timestamp())
		       OR (status = 'SCHEDULED' AND run_at <= clock_timestamp()))
		ORDER BY CASE
			WHEN cancel_requested THEN 0
			WHEN deadline_at IS NOT NULL AND deadline_at <= clock_timestamp() THEN 1
			ELSE 2
		END,
		CASE WHEN status = 'RETRY_WAIT' THEN retry_at ELSE run_at END, id
		FOR UPDATE SKIP LOCKED LIMIT 1`,
	).Scan(&taskID, &status, &currentStep, &cancelRequested, &deadlineAt, &retryAt, &runAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select due control: %w", err)
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return false, fmt.Errorf("read database time after locking due task: %w", err)
	}
	switch {
	case cancelRequested:
		if err := finishControlledTask(ctx, tx, taskID, currentStep, cancelledOutcome); err != nil {
			return false, err
		}
	case deadlineAt != nil && !deadlineAt.After(databaseNow):
		if err := finishControlledTask(ctx, tx, taskID, currentStep, timedOutOutcome); err != nil {
			return false, err
		}
	case status == task.StatusRetryWait && retryAt != nil && !retryAt.After(databaseNow):
		if err := wakeRetryTask(ctx, tx, taskID); err != nil {
			return false, err
		}
	case status == task.StatusScheduled && !runAt.After(databaseNow):
		if err := wakeScheduledTask(ctx, tx, taskID, runAt); err != nil {
			return false, err
		}
	default:
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit due control: %w", err)
	}
	return true, nil
}

func wakeScheduledTask(ctx context.Context, tx pgx.Tx, taskID string, runAt time.Time) error {
	result, err := tx.Exec(ctx, `
		UPDATE tasks SET status = 'RUNNABLE', updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'SCHEDULED'`, taskID)
	if err != nil {
		return fmt.Errorf("wake scheduled task: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrConflict
	}
	payload, _ := json.Marshal(map[string]any{"run_at": runAt, "source": "scheduled"})
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_events (task_id, event_type, payload)
		VALUES ($1, 'TASK_RUNNABLE', $2)`, taskID, payload); err != nil {
		return fmt.Errorf("insert scheduled ready event: %w", err)
	}
	return nil
}

func finishControlledTask(ctx context.Context, tx pgx.Tx, taskID string, currentStep int, outcome controlOutcome) error {
	var stepID, stepStatus string
	var attempt int
	err := tx.QueryRow(ctx, `
		SELECT id, status, attempt FROM task_steps
		WHERE task_id = $1 AND step_index = $2 FOR UPDATE`, taskID, currentStep,
	).Scan(&stepID, &stepStatus, &attempt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock controlled step: %w", err)
	}
	if err == nil && stepStatus != task.StepSucceeded {
		if _, err := tx.Exec(ctx, `
			UPDATE task_steps SET status = 'FAILED', output = NULL, error = $3,
			finished_at = clock_timestamp() WHERE task_id = $1 AND id = $2`, taskID, stepID, outcome.reason); err != nil {
			return fmt.Errorf("finish controlled step: %w", err)
		}
		payload, _ := json.Marshal(map[string]any{
			"step_id": stepID, "step_index": currentStep, "attempt": attempt, "error": outcome.reason,
		})
		if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, 'STEP_FAILED', $2)`, taskID, payload); err != nil {
			return fmt.Errorf("insert controlled step event: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET status = $2, retry_at = NULL, worker_id = NULL,
		lease_expires_at = NULL, updated_at = clock_timestamp() WHERE id = $1`, taskID, outcome.status); err != nil {
		return fmt.Errorf("finish controlled task: %w", err)
	}
	payload, _ := json.Marshal(map[string]any{"step_index": currentStep, "error": outcome.reason})
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, $2, $3)`, taskID, outcome.eventType, payload); err != nil {
		return fmt.Errorf("insert %s event: %w", outcome.eventType, err)
	}
	return nil
}

func (s *Store) WakeDueRetry(ctx context.Context) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin retry wake: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setTransactionLimits(ctx, tx); err != nil {
		return false, err
	}
	var taskID string
	err = tx.QueryRow(ctx, `
		SELECT id FROM tasks
		WHERE status = 'RETRY_WAIT' AND retry_at <= clock_timestamp()
		  AND cancel_requested = false
		  AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
		ORDER BY retry_at, id
		FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select due retry: %w", err)
	}
	if err := wakeRetryTask(ctx, tx, taskID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit retry wake: %w", err)
	}
	return true, nil
}

func wakeRetryTask(ctx context.Context, tx pgx.Tx, taskID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET status = 'RUNNABLE', retry_at = NULL, updated_at = clock_timestamp()
		WHERE id = $1`, taskID); err != nil {
		return fmt.Errorf("wake due retry: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type) VALUES ($1, 'TASK_RETRY_READY')`, taskID); err != nil {
		return fmt.Errorf("insert retry ready event: %w", err)
	}
	return nil
}

func (s *Store) commitWithFactCheck(ctx context.Context, tx pgx.Tx, operation string, confirm func(context.Context) (bool, error)) error {
	commitErr := tx.Commit(ctx)
	if commitErr == nil && s.afterCommit != nil {
		commitErr = s.afterCommit(operation)
	}
	if commitErr == nil {
		return nil
	}
	confirmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	committed, confirmErr := confirm(confirmCtx)
	if committed {
		return nil
	}
	if confirmErr != nil {
		return fmt.Errorf("%s commit result unknown after %v; confirm persisted fact: %w", operation, commitErr, confirmErr)
	}
	return fmt.Errorf("commit %s: %w", operation, commitErr)
}

func (s *Store) stepStartPersisted(ctx context.Context, attempt StepAttempt) (bool, error) {
	var persisted bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM task_steps step
			WHERE step.task_id = $1 AND step.id = $2
			  AND step.status = 'RUNNING' AND step.attempt = $3
			  AND step.resolved_input = $4::jsonb
			  AND EXISTS (
				SELECT 1 FROM task_events event
				WHERE event.task_id = $1 AND event.event_type = 'STEP_STARTED'
				  AND event.payload->>'step_id' = $2
				  AND (event.payload->>'attempt')::integer = $3
			  )
		)`, attempt.TaskID, attempt.StepID, attempt.Attempt, attempt.ResolvedInput).Scan(&persisted)
	if err != nil {
		return false, err
	}
	return persisted, nil
}

func (s *Store) stepFinishPersisted(ctx context.Context, attempt StepAttempt, outcome StepOutcome) (bool, error) {
	stepStatus := task.StepSucceeded
	eventType := "STEP_SUCCEEDED"
	taskStatus := task.StatusRunning
	if outcome.Error != "" {
		stepStatus = task.StepFailed
		eventType = "STEP_FAILED"
		if outcome.FailureClass == FailureTemporary && attempt.Attempt < attempt.MaxAttempts {
			taskStatus = task.StatusRetryWait
		} else {
			taskStatus = task.StatusFailed
		}
	}
	var persisted bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM task_steps step
			JOIN tasks task ON task.id = step.task_id
			WHERE step.task_id = $1 AND step.id = $2
			  AND step.status = $4 AND step.attempt = $3
			  AND (
				($4 = 'SUCCEEDED' AND step.output = $5::jsonb AND task.current_step > step.step_index)
				OR ($4 = 'FAILED' AND step.error = $6 AND task.status = $7)
			  )
			  AND EXISTS (
				SELECT 1 FROM task_events event
				WHERE event.task_id = $1 AND event.event_type = $8
				  AND event.payload->>'step_id' = $2
				  AND (event.payload->>'attempt')::integer = $3
			  )
		)`, attempt.TaskID, attempt.StepID, attempt.Attempt, stepStatus,
		nullableJSON(outcome.Output), outcome.Error, taskStatus, eventType).Scan(&persisted)
	if err != nil {
		return false, err
	}
	return persisted, nil
}

type lockedTask struct {
	status          string
	workerID        *string
	leaseVersion    int64
	leaseExpiresAt  *time.Time
	cancelRequested bool
	deadlineAt      *time.Time
	databaseNow     time.Time
	currentStep     int
	checkpoint      json.RawMessage
}

func lockTask(ctx context.Context, tx pgx.Tx, taskID string) (lockedTask, error) {
	var locked lockedTask
	err := tx.QueryRow(ctx, `
		SELECT status, worker_id, lease_version, lease_expires_at, cancel_requested,
		       deadline_at, current_step, checkpoint
		FROM tasks WHERE id = $1 FOR UPDATE`, taskID,
	).Scan(&locked.status, &locked.workerID, &locked.leaseVersion, &locked.leaseExpiresAt,
		&locked.cancelRequested, &locked.deadlineAt, &locked.currentStep, &locked.checkpoint)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedTask{}, ErrNotFound
	}
	if err != nil {
		return lockedTask{}, fmt.Errorf("lock task: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&locked.databaseNow); err != nil {
		return lockedTask{}, fmt.Errorf("read time after locking task: %w", err)
	}
	return locked, nil
}

func lockOwnedTask(ctx context.Context, tx pgx.Tx, lease LeaseToken) (lockedTask, error) {
	locked, err := lockTask(ctx, tx, lease.TaskID)
	if err != nil {
		return lockedTask{}, err
	}
	if err := validateOwnership(locked, lease); err != nil {
		return lockedTask{}, err
	}
	return locked, nil
}

func validateOwnership(locked lockedTask, lease LeaseToken) error {
	if !ownsActiveLease(locked, lease) {
		return ErrLeaseLost
	}
	if locked.cancelRequested {
		return ErrCancelled
	}
	if locked.deadlineAt != nil && !locked.deadlineAt.After(locked.databaseNow) {
		return ErrDeadlineExceeded
	}
	return nil
}

func ownsActiveLease(locked lockedTask, lease LeaseToken) bool {
	return locked.status == task.StatusRunning && locked.workerID != nil && *locked.workerID == lease.WorkerID &&
		locked.leaseVersion == lease.LeaseVersion && locked.leaseExpiresAt != nil && locked.leaseExpiresAt.After(locked.databaseNow)
}

func insertFinishEvents(ctx context.Context, tx pgx.Tx, lease LeaseToken, attempt StepAttempt, stepEvent, stepError string, taskTerminal bool) error {
	payload, _ := json.Marshal(map[string]any{
		"step_id": attempt.StepID, "step_index": attempt.StepIndex, "attempt": attempt.Attempt,
		"worker_id": lease.WorkerID, "lease_version": lease.LeaseVersion, "error": stepError,
	})
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, $2, $3)`, lease.TaskID, stepEvent, payload); err != nil {
		return fmt.Errorf("insert %s event: %w", stepEvent, err)
	}
	if taskTerminal {
		taskEvent := "TASK_SUCCEEDED"
		if stepEvent == "STEP_FAILED" {
			taskEvent = "TASK_FAILED"
		}
		if _, err := tx.Exec(ctx, `INSERT INTO task_events (task_id, event_type, payload) VALUES ($1, $2, $3)`, lease.TaskID, taskEvent, payload); err != nil {
			return fmt.Errorf("insert %s event: %w", taskEvent, err)
		}
	}
	return nil
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func setTransactionLimits(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '2s'"); err != nil {
		return fmt.Errorf("set lock timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '5s'"); err != nil {
		return fmt.Errorf("set statement timeout: %w", err)
	}
	return nil
}
