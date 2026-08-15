CREATE SCHEMA IF NOT EXISTS pgtask;

CREATE TABLE pgtask.tasks (
    id uuid PRIMARY KEY,
    queue_name text NOT NULL,
    task_name text NOT NULL,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(headers) = 'object'),
    state text NOT NULL DEFAULT 'pending' CHECK (
        state IN ('pending', 'running', 'waiting', 'succeeded', 'failed', 'cancelled')
    ),
    priority smallint NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    lease_token uuid,
    lease_owner uuid,
    lease_expires_at timestamptz,
    cancel_requested_at timestamptz,
    idempotency_key text,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    completed_at timestamptz,
    result jsonb,
    error jsonb,
    CHECK (
        (state = 'running' AND lease_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (state <> 'running')
    )
);

CREATE UNIQUE INDEX tasks_idempotency_key_idx
    ON pgtask.tasks (queue_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX tasks_claim_idx
    ON pgtask.tasks (queue_name, priority DESC, run_at, id)
    WHERE state = 'pending';

CREATE INDEX tasks_expired_lease_idx
    ON pgtask.tasks (lease_expires_at, id)
    WHERE state = 'running';

CREATE TABLE pgtask.attempts (
    task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    attempt integer NOT NULL CHECK (attempt > 0),
    lease_token uuid NOT NULL,
    worker_id uuid NOT NULL,
    state text NOT NULL DEFAULT 'running' CHECK (
        state IN ('running', 'succeeded', 'failed', 'lost', 'cancelled')
    ),
    started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    finished_at timestamptz,
    error jsonb,
    PRIMARY KEY (task_id, attempt)
);

CREATE OR REPLACE FUNCTION pgtask.enqueue(
    p_task_name text,
    p_payload jsonb,
    p_queue_name text DEFAULT 'default',
    p_handler_version integer DEFAULT 1,
    p_run_at timestamptz DEFAULT NULL,
    p_priority smallint DEFAULT 0,
    p_max_attempts integer DEFAULT 5,
    p_idempotency_key text DEFAULT NULL,
    p_headers jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(task_id uuid, created boolean)
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_id uuid;
BEGIN
    IF p_task_name IS NULL OR p_task_name = '' OR octet_length(p_task_name) > 255
        OR p_task_name !~ '^[A-Za-z0-9._:-]+$'
    THEN
        RAISE EXCEPTION 'invalid task name' USING ERRCODE = '22023';
    END IF;
    IF p_queue_name IS NULL OR p_queue_name = '' OR octet_length(p_queue_name) > 128
        OR p_queue_name !~ '^[A-Za-z0-9._:-]+$'
    THEN
        RAISE EXCEPTION 'invalid queue name' USING ERRCODE = '22023';
    END IF;
    IF p_handler_version IS NULL OR p_handler_version <= 0 THEN
        RAISE EXCEPTION 'handler version must be positive' USING ERRCODE = '22023';
    END IF;
    IF p_max_attempts IS NULL OR p_max_attempts <= 0 THEN
        RAISE EXCEPTION 'max attempts must be positive' USING ERRCODE = '22023';
    END IF;
    IF p_payload IS NULL THEN
        RAISE EXCEPTION 'payload must not be null' USING ERRCODE = '22023';
    END IF;
    IF p_headers IS NULL OR jsonb_typeof(p_headers) <> 'object' THEN
        RAISE EXCEPTION 'headers must be a JSON object' USING ERRCODE = '22023';
    END IF;

    INSERT INTO pgtask.tasks (
        id,
        queue_name,
        task_name,
        handler_version,
        payload,
        headers,
        priority,
        run_at,
        max_attempts,
        idempotency_key
    )
    VALUES (
        gen_random_uuid(),
        p_queue_name,
        p_task_name,
        p_handler_version,
        p_payload,
        p_headers,
        p_priority,
        COALESCE(p_run_at, statement_timestamp()),
        p_max_attempts,
        p_idempotency_key
    )
    ON CONFLICT (queue_name, idempotency_key) WHERE idempotency_key IS NOT NULL
    DO NOTHING
    RETURNING id INTO inserted_id;

    IF inserted_id IS NOT NULL THEN
        PERFORM pg_notify('pgtask_ready', p_queue_name);
        RETURN QUERY SELECT inserted_id, true;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT tasks.id, false
    FROM pgtask.tasks
    WHERE tasks.queue_name = p_queue_name
        AND tasks.idempotency_key = p_idempotency_key;
END;
$$;
