CREATE TABLE IF NOT EXISTS tasks (
    id text PRIMARY KEY,
    tenant_id text,
    status text NOT NULL CHECK (status IN ('SCHEDULED', 'RUNNABLE', 'RUNNING', 'RETRY_WAIT', 'WAITING_APPROVAL', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    run_at timestamptz NOT NULL,
    worker_id text,
    lease_version bigint NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
    lease_expires_at timestamptz,
    retry_at timestamptz,
    cancel_requested boolean NOT NULL DEFAULT false,
    current_step integer NOT NULL DEFAULT 0 CHECK (current_step >= 0),
    checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
    task_timeout_seconds integer NOT NULL CHECK (task_timeout_seconds > 0),
    first_started_at timestamptz,
    deadline_at timestamptz,
    CONSTRAINT task_ownership_matches_status CHECK (
        (status = 'RUNNING' AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'RUNNING' AND worker_id IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS task_steps (
    id text PRIMARY KEY,
    task_id text NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    step_index integer NOT NULL CHECK (step_index >= 0),
    step_type text NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'WAITING_APPROVAL', 'SUCCEEDED', 'FAILED')),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    idempotency_key text NOT NULL UNIQUE CHECK (idempotency_key <> ''),
    input jsonb NOT NULL,
    resolved_input jsonb,
    output jsonb,
    error text,
    approval_decision text CHECK (approval_decision IN ('approved', 'rejected')),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds > 0),
    started_at timestamptz,
    finished_at timestamptz,
    UNIQUE (task_id, step_index)
);

CREATE TABLE IF NOT EXISTS task_events (
    id bigserial PRIMARY KEY,
    task_id text NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS tasks_runnable_idx ON tasks (run_at, created_at) WHERE status = 'RUNNABLE';
CREATE INDEX IF NOT EXISTS tasks_lease_idx ON tasks (lease_expires_at, created_at) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS tasks_retry_idx ON tasks (retry_at) WHERE status = 'RETRY_WAIT';
CREATE INDEX IF NOT EXISTS tasks_deadline_idx ON tasks (deadline_at) WHERE status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT');
CREATE INDEX IF NOT EXISTS task_steps_task_idx ON task_steps (task_id, step_index);
CREATE INDEX IF NOT EXISTS task_events_task_idx ON task_events (task_id, id);

CREATE OR REPLACE FUNCTION reject_step_definition_change() RETURNS trigger AS $$
BEGIN
    IF NEW.task_id IS DISTINCT FROM OLD.task_id
       OR NEW.step_index IS DISTINCT FROM OLD.step_index
       OR NEW.step_type IS DISTINCT FROM OLD.step_type
       OR NEW.max_attempts IS DISTINCT FROM OLD.max_attempts
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.input IS DISTINCT FROM OLD.input
       OR NEW.timeout_seconds IS DISTINCT FROM OLD.timeout_seconds THEN
        RAISE EXCEPTION 'task step definition is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS task_step_definition_immutable ON task_steps;
CREATE TRIGGER task_step_definition_immutable
BEFORE UPDATE ON task_steps
FOR EACH ROW EXECUTE FUNCTION reject_step_definition_change();
