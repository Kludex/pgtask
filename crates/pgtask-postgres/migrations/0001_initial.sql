-- Queue kernel

CREATE SCHEMA IF NOT EXISTS pgtask;

CREATE TABLE pgtask.queues (
    name text PRIMARY KEY,
    terminal_retention_seconds bigint NOT NULL DEFAULT 604800 CHECK (terminal_retention_seconds >= 0),
    paused_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    idempotency_retention_seconds bigint NOT NULL DEFAULT 2592000 CHECK (idempotency_retention_seconds >= 0),
    max_outstanding_tasks bigint CHECK (max_outstanding_tasks > 0),
    starvation_timeout_seconds bigint NOT NULL DEFAULT 300 CHECK (starvation_timeout_seconds >= 0),
    capacity_outstanding_tasks bigint NOT NULL DEFAULT 0 CHECK (capacity_outstanding_tasks >= 0),
    CHECK (name <> '' AND octet_length(name) <= 128 AND name ~ '^[A-Za-z0-9._:-]+$')
);

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
    schedule_id uuid,
    scheduled_for timestamptz,
    parent_task_id uuid REFERENCES pgtask.tasks (id) ON DELETE SET NULL,
    retry_kind text,
    retry_base_delay_milliseconds bigint,
    retry_factor integer,
    retry_max_delay_milliseconds bigint,
    CONSTRAINT tasks_queue_name_fkey FOREIGN KEY (queue_name) REFERENCES pgtask.queues (name),
    CHECK (
        (state = 'running' AND lease_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (state <> 'running')
    ),
    CONSTRAINT tasks_payload_size_check CHECK (octet_length(payload::text) <= 1048576),
    CONSTRAINT tasks_headers_size_check CHECK (octet_length(headers::text) <= 65536),
    CONSTRAINT tasks_result_size_check CHECK (result IS NULL OR octet_length(result::text) <= 1048576),
    CONSTRAINT tasks_error_size_check CHECK (error IS NULL OR octet_length(error::text) <= 262144),
    CONSTRAINT tasks_schedule_occurrence_check CHECK (
        (schedule_id IS NULL AND scheduled_for IS NULL)
        OR (schedule_id IS NOT NULL AND scheduled_for IS NOT NULL)
    ),
    CONSTRAINT tasks_parent_check CHECK (parent_task_id IS NULL OR parent_task_id <> id),
    CONSTRAINT tasks_retry_policy_check CHECK (
        (
            retry_kind IS NULL
            AND retry_base_delay_milliseconds IS NULL
            AND retry_factor IS NULL
            AND retry_max_delay_milliseconds IS NULL
        )
        OR (
            retry_kind = 'never'
            AND retry_base_delay_milliseconds IS NULL
            AND retry_factor IS NULL
            AND retry_max_delay_milliseconds IS NULL
        )
        OR (
            retry_kind = 'fixed'
            AND retry_base_delay_milliseconds >= 0
            AND retry_factor IS NULL
            AND retry_max_delay_milliseconds IS NULL
        )
        OR (
            retry_kind = 'exponential'
            AND retry_base_delay_milliseconds >= 0
            AND retry_factor > 0
            AND retry_max_delay_milliseconds >= retry_base_delay_milliseconds
        )
    )
)
WITH (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 1000
);

CREATE INDEX tasks_claim_idx
    ON pgtask.tasks (queue_name, priority DESC, run_at, id)
    WHERE state = 'pending';

CREATE INDEX tasks_expired_lease_idx
    ON pgtask.tasks (queue_name, lease_expires_at, id)
    WHERE state = 'running';

CREATE UNIQUE INDEX tasks_schedule_occurrence_idx
    ON pgtask.tasks (schedule_id, scheduled_for)
    WHERE schedule_id IS NOT NULL;

CREATE INDEX tasks_parent_idx ON pgtask.tasks (parent_task_id, id) WHERE parent_task_id IS NOT NULL;

CREATE INDEX tasks_pending_capability_idx
    ON pgtask.tasks (queue_name, task_name, handler_version, priority DESC, run_at, id)
    WHERE state = 'pending';

CREATE INDEX tasks_outstanding_idx
    ON pgtask.tasks (queue_name, id)
    WHERE state IN ('pending', 'running', 'waiting');

CREATE INDEX tasks_oldest_ready_idx
    ON pgtask.tasks (queue_name, run_at, id)
    WHERE state = 'pending';

CREATE TABLE pgtask.attempts (
    task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    attempt integer NOT NULL CHECK (attempt > 0),
    lease_token uuid NOT NULL,
    worker_id uuid NOT NULL,
    state text NOT NULL DEFAULT 'running' CONSTRAINT attempts_state_check CHECK (
        state IN ('running', 'succeeded', 'failed', 'lost', 'cancelled', 'suspended')
    ),
    started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    finished_at timestamptz,
    error jsonb,
    CONSTRAINT attempts_error_size_check CHECK (error IS NULL OR octet_length(error::text) <= 262144),
    PRIMARY KEY (task_id, attempt)
);

-- Workers

CREATE TABLE pgtask.workers (
    id uuid PRIMARY KEY,
    queue_name text NOT NULL REFERENCES pgtask.queues (name),
    version text NOT NULL,
    draining boolean NOT NULL DEFAULT false,
    started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX workers_expiry_idx ON pgtask.workers (expires_at, id);

CREATE TABLE pgtask.worker_capabilities (
    worker_id uuid NOT NULL REFERENCES pgtask.workers (id) ON DELETE CASCADE,
    task_name text NOT NULL,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    PRIMARY KEY (worker_id, task_name, handler_version)
);

CREATE TABLE pgtask.handler_policies (
    queue_name text NOT NULL REFERENCES pgtask.queues (name) ON DELETE CASCADE,
    task_name text NOT NULL,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    retry_kind text NOT NULL,
    retry_base_delay_milliseconds bigint,
    retry_factor integer,
    retry_max_delay_milliseconds bigint,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK (
        (
            retry_kind = 'never'
            AND retry_base_delay_milliseconds IS NULL
            AND retry_factor IS NULL
            AND retry_max_delay_milliseconds IS NULL
        )
        OR (
            retry_kind = 'fixed'
            AND retry_base_delay_milliseconds >= 0
            AND retry_factor IS NULL
            AND retry_max_delay_milliseconds IS NULL
        )
        OR (
            retry_kind = 'exponential'
            AND retry_base_delay_milliseconds >= 0
            AND retry_factor > 0
            AND retry_max_delay_milliseconds >= retry_base_delay_milliseconds
        )
    ),
    PRIMARY KEY (queue_name, task_name, handler_version)
);

-- Checkpoints

CREATE TABLE pgtask.checkpoints (
    task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    step_name text NOT NULL CHECK (
        step_name <> ''
        AND octet_length(step_name) <= 255
        AND step_name ~ '^[A-Za-z0-9._:-]+$'
    ),
    occurrence integer NOT NULL CHECK (occurrence >= 0),
    value jsonb NOT NULL CHECK (octet_length(value::text) <= 1048576),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (task_id, handler_version, step_name, occurrence)
);

-- Schedules

CREATE TABLE pgtask.schedules (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (
        name <> '' AND octet_length(name) <= 255 AND name ~ '^[A-Za-z0-9._:-]+$'
    ),
    kind text NOT NULL CHECK (kind IN ('interval', 'cron')),
    interval_milliseconds bigint CHECK (interval_milliseconds > 0),
    cron_expression text,
    misfire_policy text NOT NULL CHECK (misfire_policy IN ('skip', 'latest', 'catch_up')),
    catch_up_limit integer CHECK (catch_up_limit > 0 AND catch_up_limit <= 65535),
    queue_name text NOT NULL REFERENCES pgtask.queues (name),
    task_name text NOT NULL CHECK (
        task_name <> '' AND octet_length(task_name) <= 255 AND task_name ~ '^[A-Za-z0-9._:-]+$'
    ),
    handler_version integer NOT NULL CHECK (handler_version > 0),
    payload jsonb NOT NULL CHECK (octet_length(payload::text) <= 1048576),
    headers jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (
        jsonb_typeof(headers) = 'object' AND octet_length(headers::text) <= 65536
    ),
    priority smallint NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    next_run_at timestamptz NOT NULL,
    paused_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK (
        (kind = 'interval' AND interval_milliseconds IS NOT NULL AND cron_expression IS NULL)
        OR (kind = 'cron' AND interval_milliseconds IS NULL AND cron_expression IS NOT NULL)
    ),
    CHECK (
        (misfire_policy = 'catch_up' AND catch_up_limit IS NOT NULL)
        OR (misfire_policy <> 'catch_up' AND catch_up_limit IS NULL)
    )
);

CREATE INDEX schedules_due_idx
    ON pgtask.schedules (next_run_at, id)
    WHERE paused_at IS NULL;

-- Signals

CREATE TABLE pgtask.signals (
    task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    signal_name text NOT NULL CHECK (
        signal_name <> ''
        AND octet_length(signal_name) <= 255
        AND signal_name ~ '^[A-Za-z0-9._:-]+$'
    ),
    occurrence integer NOT NULL CHECK (occurrence >= 0),
    value jsonb NOT NULL CHECK (octet_length(value::text) <= 1048576),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (task_id, signal_name, occurrence)
);

CREATE TABLE pgtask.waits (
    task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    step_name text NOT NULL CHECK (
        step_name <> ''
        AND octet_length(step_name) <= 255
        AND step_name ~ '^[A-Za-z0-9._:-]+$'
    ),
    occurrence integer NOT NULL CHECK (occurrence >= 0),
    signal_name text NOT NULL CHECK (
        signal_name <> ''
        AND octet_length(signal_name) <= 255
        AND signal_name ~ '^[A-Za-z0-9._:-]+$'
    ),
    signal_occurrence integer NOT NULL CHECK (signal_occurrence >= 0),
    timeout_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    resolved_at timestamptz,
    outcome text CHECK (outcome IN ('signal', 'timeout')),
    PRIMARY KEY (task_id, handler_version, step_name, occurrence)
);

CREATE UNIQUE INDEX waits_active_task_idx
    ON pgtask.waits (task_id)
    WHERE resolved_at IS NULL;

CREATE INDEX waits_timeout_idx
    ON pgtask.waits (timeout_at, task_id)
    WHERE resolved_at IS NULL AND timeout_at IS NOT NULL;

-- Task results

CREATE TABLE pgtask.result_waits (
    task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    step_name text NOT NULL CHECK (
        step_name <> ''
        AND octet_length(step_name) <= 255
        AND step_name ~ '^[A-Za-z0-9._:-]+$'
    ),
    occurrence integer NOT NULL CHECK (occurrence >= 0),
    result_task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    resolved_at timestamptz,
    outcome text CONSTRAINT result_waits_outcome_check CHECK (
        outcome IN ('succeeded', 'failed', 'cancelled', 'timeout')
    ),
    timeout_at timestamptz,
    CHECK (task_id <> result_task_id),
    PRIMARY KEY (task_id, handler_version, step_name, occurrence)
);

CREATE UNIQUE INDEX result_waits_active_task_idx
    ON pgtask.result_waits (task_id)
    WHERE resolved_at IS NULL;

CREATE INDEX result_waits_target_idx
    ON pgtask.result_waits (result_task_id, task_id)
    WHERE resolved_at IS NULL;

CREATE INDEX result_waits_timeout_idx
    ON pgtask.result_waits (timeout_at, task_id)
    WHERE resolved_at IS NULL AND timeout_at IS NOT NULL;

-- Idempotency keys

CREATE TABLE pgtask.idempotency_keys (
    queue_name text NOT NULL REFERENCES pgtask.queues (name) ON DELETE CASCADE,
    idempotency_key text NOT NULL,
    task_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    expires_at timestamptz,
    PRIMARY KEY (queue_name, idempotency_key)
);

CREATE INDEX idempotency_keys_expiry_idx
    ON pgtask.idempotency_keys (queue_name, expires_at, idempotency_key)
    WHERE expires_at IS NOT NULL;

-- Administrator audit

CREATE TABLE pgtask.administrator_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor text NOT NULL CHECK (actor <> '' AND octet_length(actor) <= 255),
    action text NOT NULL CHECK (action IN ('task.cancel', 'task.retry', 'schedule.pause', 'schedule.resume')),
    task_id uuid,
    schedule_id uuid,
    occurred_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK ((task_id IS NOT NULL)::integer + (schedule_id IS NOT NULL)::integer = 1)
);

CREATE INDEX administrator_audit_task_idx
    ON pgtask.administrator_audit (task_id, occurred_at DESC)
    WHERE task_id IS NOT NULL;

CREATE INDEX administrator_audit_schedule_idx
    ON pgtask.administrator_audit (schedule_id, occurred_at DESC)
    WHERE schedule_id IS NOT NULL;

-- Views

CREATE VIEW pgtask.task_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.tasks;

CREATE VIEW pgtask.attempt_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.attempts;

CREATE VIEW pgtask.queue_overview WITH (security_barrier = true) AS
SELECT
    queues.name,
    queues.terminal_retention_seconds,
    queues.paused_at,
    queues.created_at,
    queues.updated_at,
    count(tasks.id) FILTER (WHERE tasks.state = 'pending') AS pending_count,
    count(tasks.id) FILTER (WHERE tasks.state = 'running') AS running_count,
    count(tasks.id) FILTER (WHERE tasks.state = 'waiting') AS waiting_count,
    count(tasks.id) FILTER (WHERE tasks.state IN ('succeeded', 'failed', 'cancelled')) AS terminal_count,
    count(tasks.id) FILTER (
        WHERE tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
            AND queues.paused_at IS NULL
    ) AS ready_count,
    count(tasks.id) FILTER (
        WHERE tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
            AND queues.paused_at IS NULL
            AND EXISTS (
                SELECT 1
                FROM pgtask.workers
                JOIN pgtask.worker_capabilities ON worker_capabilities.worker_id = workers.id
                WHERE workers.queue_name = tasks.queue_name
                    AND workers.draining = false
                    AND workers.expires_at > statement_timestamp()
                    AND worker_capabilities.task_name = tasks.task_name
                    AND worker_capabilities.handler_version = tasks.handler_version
            )
    ) AS routable_count,
    count(tasks.id) FILTER (
        WHERE tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
            AND queues.paused_at IS NULL
            AND NOT EXISTS (
                SELECT 1
                FROM pgtask.workers
                JOIN pgtask.worker_capabilities ON worker_capabilities.worker_id = workers.id
                WHERE workers.queue_name = tasks.queue_name
                    AND workers.draining = false
                    AND workers.expires_at > statement_timestamp()
                    AND worker_capabilities.task_name = tasks.task_name
                    AND worker_capabilities.handler_version = tasks.handler_version
            )
    ) AS unroutable_count,
    queues.idempotency_retention_seconds,
    queues.max_outstanding_tasks,
    queues.starvation_timeout_seconds,
    count(tasks.id) FILTER (WHERE tasks.state IN ('pending', 'running', 'waiting')) AS outstanding_count
FROM pgtask.queues
LEFT JOIN pgtask.tasks ON tasks.queue_name = queues.name
GROUP BY queues.name;

CREATE VIEW pgtask.schedule_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.schedules;

CREATE VIEW pgtask.schedule_occurrence_view WITH (security_barrier = true) AS
SELECT schedule_id, scheduled_for, id AS task_id, state, created_at, completed_at
FROM pgtask.tasks
WHERE schedule_id IS NOT NULL;

CREATE VIEW pgtask.signal_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.signals;

CREATE VIEW pgtask.wait_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.waits;

CREATE VIEW pgtask.result_wait_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.result_waits;

CREATE VIEW pgtask.checkpoint_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.checkpoints;

CREATE VIEW pgtask.worker_view WITH (security_barrier = true) AS
SELECT workers.*, workers.expires_at > statement_timestamp() AS live
FROM pgtask.workers;

CREATE VIEW pgtask.worker_capability_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.worker_capabilities;

CREATE VIEW pgtask.handler_policy_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.handler_policies;

CREATE VIEW pgtask.administrator_audit_view AS
SELECT id, actor, action, task_id, schedule_id, occurred_at
FROM pgtask.administrator_audit;

-- Queue configuration

CREATE FUNCTION pgtask.put_queue(
    p_name text,
    p_terminal_retention_seconds bigint,
    p_idempotency_retention_seconds bigint,
    p_max_outstanding_tasks bigint,
    p_starvation_timeout_seconds bigint
)
RETURNS SETOF pgtask.queues
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    current_capacity bigint;
    current_maximum bigint;
    initial_capacity bigint := 0;
    queue_exists boolean;
BEGIN
    SELECT queues.max_outstanding_tasks, queues.capacity_outstanding_tasks
    INTO current_maximum, current_capacity
    FROM pgtask.queues
    WHERE queues.name = p_name
    FOR UPDATE;
    queue_exists := FOUND;

    IF p_max_outstanding_tasks IS NULL THEN
        initial_capacity := 0;
    ELSIF queue_exists AND current_maximum IS NOT NULL THEN
        initial_capacity := current_capacity;
    ELSE
        SELECT count(*)
        INTO initial_capacity
        FROM pgtask.tasks
        WHERE tasks.queue_name = p_name
            AND tasks.state IN ('pending', 'running', 'waiting');
    END IF;

    RETURN QUERY
    INSERT INTO pgtask.queues (
        name,
        terminal_retention_seconds,
        idempotency_retention_seconds,
        max_outstanding_tasks,
        starvation_timeout_seconds,
        capacity_outstanding_tasks
    )
    VALUES (
        p_name,
        p_terminal_retention_seconds,
        p_idempotency_retention_seconds,
        p_max_outstanding_tasks,
        p_starvation_timeout_seconds,
        initial_capacity
    )
    ON CONFLICT (name) DO UPDATE
    SET terminal_retention_seconds = EXCLUDED.terminal_retention_seconds,
        idempotency_retention_seconds = EXCLUDED.idempotency_retention_seconds,
        max_outstanding_tasks = EXCLUDED.max_outstanding_tasks,
        starvation_timeout_seconds = EXCLUDED.starvation_timeout_seconds,
        capacity_outstanding_tasks = EXCLUDED.capacity_outstanding_tasks,
        updated_at = statement_timestamp()
    RETURNING queues.*;
END;
$$;

CREATE FUNCTION pgtask.set_queue_paused(p_name text, p_paused boolean)
RETURNS SETOF pgtask.queues
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    RETURN QUERY
    UPDATE pgtask.queues
    SET paused_at = CASE WHEN p_paused THEN COALESCE(paused_at, statement_timestamp()) ELSE NULL END,
        updated_at = statement_timestamp()
    WHERE name = p_name
    RETURNING queues.*;
    IF FOUND AND NOT p_paused THEN
        PERFORM pg_notify('pgtask_ready', p_name);
    END IF;
END;
$$;

-- Enqueue

CREATE FUNCTION pgtask.enqueue(
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
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    candidate_id uuid := gen_random_uuid();
    reserved_id uuid;
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

    INSERT INTO pgtask.queues (name)
    VALUES (p_queue_name)
    ON CONFLICT DO NOTHING;

    IF p_idempotency_key IS NOT NULL THEN
        INSERT INTO pgtask.idempotency_keys (queue_name, idempotency_key, task_id)
        VALUES (p_queue_name, p_idempotency_key, candidate_id)
        ON CONFLICT (queue_name, idempotency_key) DO UPDATE
        SET task_id = EXCLUDED.task_id,
            created_at = statement_timestamp(),
            expires_at = NULL
        WHERE idempotency_keys.expires_at IS NOT NULL
            AND idempotency_keys.expires_at <= statement_timestamp()
        RETURNING idempotency_keys.task_id INTO reserved_id;

        IF reserved_id IS NULL THEN
            SELECT idempotency_keys.task_id
            INTO reserved_id
            FROM pgtask.idempotency_keys
            WHERE idempotency_keys.queue_name = p_queue_name
                AND idempotency_keys.idempotency_key = p_idempotency_key;
            RETURN QUERY SELECT reserved_id, false;
            RETURN;
        END IF;
    ELSE
        reserved_id := candidate_id;
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
        reserved_id,
        p_queue_name,
        p_task_name,
        p_handler_version,
        p_payload,
        p_headers,
        p_priority,
        COALESCE(p_run_at, statement_timestamp()),
        p_max_attempts,
        p_idempotency_key
    );

    PERFORM pg_notify('pgtask_ready', p_queue_name);
    RETURN QUERY SELECT reserved_id, true;
END;
$$;

CREATE FUNCTION pgtask.enqueue_many(p_tasks jsonb)
RETURNS TABLE(request_index bigint, task_id uuid, created boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF p_tasks IS NULL OR jsonb_typeof(p_tasks) <> 'array' THEN
        RAISE EXCEPTION 'tasks must be a JSON array' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT items.ordinality - 1, enqueued.task_id, enqueued.created
    FROM jsonb_array_elements(p_tasks) WITH ORDINALITY AS items(task, ordinality)
    CROSS JOIN LATERAL pgtask.enqueue(
        p_task_name => items.task->>'task_name',
        p_payload => items.task->'payload',
        p_queue_name => COALESCE(items.task->>'queue_name', 'default'),
        p_handler_version => COALESCE((items.task->>'handler_version')::integer, 1),
        p_run_at => (items.task->>'run_at')::timestamptz,
        p_priority => COALESCE((items.task->>'priority')::smallint, 0::smallint),
        p_max_attempts => COALESCE((items.task->>'max_attempts')::integer, 5),
        p_idempotency_key => items.task->>'idempotency_key',
        p_headers => COALESCE(items.task->'headers', '{}'::jsonb)
    ) AS enqueued
    ORDER BY items.ordinality;
END;
$$;

CREATE FUNCTION pgtask.task_result(p_task_id uuid)
RETURNS TABLE(state text, result jsonb, error jsonb, completed_at timestamptz)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT tasks.state, tasks.result, tasks.error, tasks.completed_at
    FROM pgtask.tasks
    WHERE tasks.id = p_task_id;
$$;

-- Worker protocol

CREATE FUNCTION pgtask.claim(
    p_queue_name text,
    p_worker_id uuid,
    p_task_names text[],
    p_handler_versions integer[],
    p_limit integer,
    p_lease_milliseconds bigint
)
RETURNS SETOF pgtask.tasks
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH queue_config AS MATERIALIZED (
        SELECT queues.starvation_timeout_seconds
        FROM pgtask.queues
        WHERE queues.name = p_queue_name AND queues.paused_at IS NULL
    ),
    starved AS MATERIALIZED (
        SELECT
            tasks.id,
            handler_policies.retry_kind,
            handler_policies.retry_base_delay_milliseconds,
            handler_policies.retry_factor,
            handler_policies.retry_max_delay_milliseconds,
            0 AS claim_order
        FROM pgtask.tasks
        CROSS JOIN queue_config
        LEFT JOIN pgtask.handler_policies
            ON handler_policies.queue_name = tasks.queue_name
            AND handler_policies.task_name = tasks.task_name
            AND handler_policies.handler_version = tasks.handler_version
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
                - (queue_config.starvation_timeout_seconds * interval '1 second')
            AND tasks.attempt < tasks.max_attempts
            AND EXISTS (
                SELECT 1
                FROM unnest(p_task_names, p_handler_versions) AS handlers(task_name, handler_version)
                WHERE handlers.task_name = tasks.task_name
                    AND handlers.handler_version = tasks.handler_version
            )
        ORDER BY tasks.run_at, tasks.id
        FOR NO KEY UPDATE OF tasks SKIP LOCKED
        LIMIT CASE WHEN p_limit > 0 THEN 1 ELSE 0 END
    ),
    priority_candidates AS MATERIALIZED (
        SELECT
            tasks.id,
            handler_policies.retry_kind,
            handler_policies.retry_base_delay_milliseconds,
            handler_policies.retry_factor,
            handler_policies.retry_max_delay_milliseconds,
            1 AS claim_order
        FROM pgtask.tasks
        CROSS JOIN queue_config
        LEFT JOIN pgtask.handler_policies
            ON handler_policies.queue_name = tasks.queue_name
            AND handler_policies.task_name = tasks.task_name
            AND handler_policies.handler_version = tasks.handler_version
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
            AND tasks.attempt < tasks.max_attempts
            AND NOT EXISTS (SELECT 1 FROM starved WHERE starved.id = tasks.id)
            AND EXISTS (
                SELECT 1
                FROM unnest(p_task_names, p_handler_versions) AS handlers(task_name, handler_version)
                WHERE handlers.task_name = tasks.task_name
                    AND handlers.handler_version = tasks.handler_version
            )
        ORDER BY tasks.priority DESC, tasks.run_at, tasks.id
        FOR NO KEY UPDATE OF tasks SKIP LOCKED
        LIMIT GREATEST(p_limit - (SELECT count(*)::integer FROM starved), 0)
    ),
    candidates AS MATERIALIZED (
        SELECT * FROM starved
        UNION ALL
        SELECT * FROM priority_candidates
    ),
    claimed AS (
        UPDATE pgtask.tasks AS tasks
        SET state = 'running',
            attempt = tasks.attempt + 1,
            lease_token = gen_random_uuid(),
            lease_owner = p_worker_id,
            lease_expires_at = statement_timestamp() + (p_lease_milliseconds * interval '1 millisecond'),
            updated_at = statement_timestamp(),
            retry_kind = COALESCE(tasks.retry_kind, candidates.retry_kind),
            retry_base_delay_milliseconds = COALESCE(
                tasks.retry_base_delay_milliseconds,
                candidates.retry_base_delay_milliseconds
            ),
            retry_factor = COALESCE(tasks.retry_factor, candidates.retry_factor),
            retry_max_delay_milliseconds = COALESCE(
                tasks.retry_max_delay_milliseconds,
                candidates.retry_max_delay_milliseconds
            )
        FROM candidates
        WHERE tasks.id = candidates.id
        RETURNING tasks.*
    ),
    inserted_attempts AS (
        INSERT INTO pgtask.attempts (task_id, attempt, lease_token, worker_id)
        SELECT id, attempt, lease_token, lease_owner
        FROM claimed
    )
    SELECT claimed.*
    FROM claimed
    JOIN candidates ON candidates.id = claimed.id
    ORDER BY candidates.claim_order, claimed.priority DESC, claimed.run_at, claimed.id;
$$;

CREATE FUNCTION pgtask.renew_leases(
    p_task_ids uuid[],
    p_attempts integer[],
    p_lease_tokens uuid[],
    p_lease_milliseconds bigint
)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH requested AS (
        SELECT *
        FROM unnest(p_task_ids, p_attempts, p_lease_tokens) AS leases(task_id, attempt, lease_token)
    )
    UPDATE pgtask.tasks
    SET lease_expires_at = statement_timestamp() + (p_lease_milliseconds * interval '1 millisecond'),
        updated_at = statement_timestamp()
    FROM requested
    WHERE tasks.id = requested.task_id
        AND tasks.state = 'running'
        AND tasks.attempt = requested.attempt
        AND tasks.lease_token = requested.lease_token
        AND tasks.cancel_requested_at IS NULL
    RETURNING tasks.id;
$$;

CREATE FUNCTION pgtask.complete_task(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_result jsonb
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH completed AS (
        UPDATE pgtask.tasks
        SET state = 'succeeded',
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = statement_timestamp(),
            updated_at = statement_timestamp(),
            result = p_result,
            error = NULL
        WHERE id = p_task_id AND state = 'running' AND attempt = p_attempt AND lease_token = p_lease_token
        RETURNING id, attempt
    ),
    completed_attempt AS (
        UPDATE pgtask.attempts AS attempts
        SET state = 'succeeded', finished_at = statement_timestamp(), error = NULL
        FROM completed
        WHERE attempts.task_id = completed.id AND attempts.attempt = completed.attempt
    )
    SELECT EXISTS(SELECT 1 FROM completed);
$$;

CREATE FUNCTION pgtask.fail_task(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_error jsonb,
    p_retry_milliseconds bigint
)
RETURNS text
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH failed AS (
        UPDATE pgtask.tasks
        SET state = CASE
                WHEN p_retry_milliseconds IS NOT NULL AND attempt < max_attempts THEN 'pending'
                ELSE 'failed'
            END,
            run_at = CASE
                WHEN p_retry_milliseconds IS NOT NULL
                    THEN statement_timestamp() + (p_retry_milliseconds * interval '1 millisecond')
                ELSE run_at
            END,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = CASE
                WHEN p_retry_milliseconds IS NOT NULL AND attempt < max_attempts THEN NULL
                ELSE statement_timestamp()
            END,
            updated_at = statement_timestamp(),
            error = p_error
        WHERE id = p_task_id AND state = 'running' AND attempt = p_attempt AND lease_token = p_lease_token
        RETURNING id, attempt, state
    ),
    failed_attempt AS (
        UPDATE pgtask.attempts AS attempts
        SET state = 'failed', finished_at = statement_timestamp(), error = p_error
        FROM failed
        WHERE attempts.task_id = failed.id AND attempts.attempt = failed.attempt
    )
    SELECT state FROM failed;
$$;

CREATE FUNCTION pgtask.recover_expired(p_queue_name text, p_limit integer)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH expired AS (
        SELECT tasks.id
        FROM pgtask.tasks
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state = 'running'
            AND tasks.lease_expires_at <= statement_timestamp()
        ORDER BY tasks.lease_expires_at, tasks.id
        FOR NO KEY UPDATE OF tasks SKIP LOCKED
        LIMIT p_limit
    ),
    recovered AS (
        UPDATE pgtask.tasks AS tasks
        SET state = CASE WHEN tasks.attempt < tasks.max_attempts THEN 'pending' ELSE 'failed' END,
            run_at = CASE WHEN tasks.attempt < tasks.max_attempts THEN statement_timestamp() ELSE tasks.run_at END,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = CASE WHEN tasks.attempt < tasks.max_attempts THEN NULL ELSE statement_timestamp() END,
            updated_at = statement_timestamp(),
            error = jsonb_build_object('type', 'lease_expired')
        FROM expired
        WHERE tasks.id = expired.id
        RETURNING tasks.id, tasks.attempt
    ),
    lost_attempts AS (
        UPDATE pgtask.attempts AS attempts
        SET state = 'lost', finished_at = statement_timestamp(), error = jsonb_build_object('type', 'lease_expired')
        FROM recovered
        WHERE attempts.task_id = recovered.id AND attempts.attempt = recovered.attempt
    )
    SELECT count(*) FROM recovered;
$$;

CREATE FUNCTION pgtask.suspend_task(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_wake_at timestamptz,
    p_delay_milliseconds bigint
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target_handler_version integer;
    target_queue_name text;
    target_wake_at timestamptz;
BEGIN
    IF (p_wake_at IS NULL) = (p_delay_milliseconds IS NULL) THEN
        RAISE EXCEPTION 'provide exactly one wake time or delay' USING ERRCODE = '22023';
    END IF;
    IF p_delay_milliseconds IS NOT NULL AND p_delay_milliseconds < 0 THEN
        RAISE EXCEPTION 'sleep delay must not be negative' USING ERRCODE = '22023';
    END IF;

    SELECT handler_version, queue_name
    INTO target_handler_version, target_queue_name
    FROM pgtask.tasks
    WHERE id = p_task_id
        AND state = 'running'
        AND attempt = p_attempt
        AND lease_token = p_lease_token
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    target_wake_at = COALESCE(
        p_wake_at,
        statement_timestamp() + (p_delay_milliseconds * interval '1 millisecond')
    );

    INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
    VALUES (
        p_task_id,
        target_handler_version,
        p_step_name,
        p_occurrence,
        jsonb_build_object('wake_at', target_wake_at)
    )
    ON CONFLICT (task_id, handler_version, step_name, occurrence) DO NOTHING;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    UPDATE pgtask.tasks
    SET state = 'pending',
        run_at = target_wake_at,
        lease_token = NULL,
        lease_owner = NULL,
        lease_expires_at = NULL,
        updated_at = statement_timestamp()
    WHERE id = p_task_id;

    UPDATE pgtask.attempts
    SET state = 'suspended', finished_at = statement_timestamp()
    WHERE task_id = p_task_id AND attempt = p_attempt;

    PERFORM pg_notify('pgtask_ready', target_queue_name);
    RETURN target_wake_at;
END;
$$;

CREATE FUNCTION pgtask.spawn_task(
    p_parent_task_id uuid,
    p_parent_attempt integer,
    p_parent_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_task_name text,
    p_payload jsonb,
    p_queue_name text,
    p_handler_version integer,
    p_run_at timestamptz,
    p_priority smallint,
    p_max_attempts integer,
    p_headers jsonb
)
RETURNS TABLE(task_id uuid, created boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    parent_handler_version integer;
    checkpoint_value jsonb;
    child_task_id uuid;
    child_created boolean;
    existing_parent uuid;
BEGIN
    SELECT tasks.handler_version
    INTO parent_handler_version
    FROM pgtask.tasks
    WHERE tasks.id = p_parent_task_id
        AND tasks.state = 'running'
        AND tasks.attempt = p_parent_attempt
        AND tasks.lease_token = p_parent_lease_token
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT checkpoints.value
    INTO checkpoint_value
    FROM pgtask.checkpoints
    WHERE checkpoints.task_id = p_parent_task_id
        AND checkpoints.handler_version = parent_handler_version
        AND checkpoints.step_name = p_step_name
        AND checkpoints.occurrence = p_occurrence;

    IF FOUND THEN
        child_task_id = (checkpoint_value->>'task_id')::uuid;
        UPDATE pgtask.tasks
        SET parent_task_id = p_parent_task_id
        WHERE id = child_task_id AND parent_task_id IS NULL;
        SELECT parent_task_id INTO existing_parent FROM pgtask.tasks WHERE id = child_task_id;
        IF existing_parent IS DISTINCT FROM p_parent_task_id THEN
            RAISE EXCEPTION 'spawn checkpoint does not reference a child of this task' USING ERRCODE = '23514';
        END IF;
        RETURN QUERY SELECT child_task_id, false;
        RETURN;
    END IF;

    SELECT enqueued.task_id, enqueued.created
    INTO child_task_id, child_created
    FROM pgtask.enqueue(
        p_task_name => p_task_name,
        p_payload => p_payload,
        p_queue_name => p_queue_name,
        p_handler_version => p_handler_version,
        p_run_at => p_run_at,
        p_priority => p_priority,
        p_max_attempts => p_max_attempts,
        p_idempotency_key => format(
            'pgtask:spawn:%s:%s:%s:%s',
            p_parent_task_id,
            parent_handler_version,
            p_step_name,
            p_occurrence
        ),
        p_headers => p_headers
    ) AS enqueued;

    IF EXISTS (
        WITH RECURSIVE ancestors AS (
            SELECT tasks.id, tasks.parent_task_id
            FROM pgtask.tasks
            WHERE tasks.id = p_parent_task_id
            UNION ALL
            SELECT tasks.id, tasks.parent_task_id
            FROM pgtask.tasks
            JOIN ancestors ON tasks.id = ancestors.parent_task_id
        )
        SELECT 1 FROM ancestors WHERE ancestors.id = child_task_id
    ) THEN
        RAISE EXCEPTION 'spawning this task would create an ownership cycle' USING ERRCODE = '23514';
    END IF;

    UPDATE pgtask.tasks
    SET parent_task_id = p_parent_task_id
    WHERE id = child_task_id AND parent_task_id IS NULL;
    SELECT parent_task_id INTO existing_parent FROM pgtask.tasks WHERE id = child_task_id;
    IF existing_parent IS DISTINCT FROM p_parent_task_id THEN
        RAISE EXCEPTION 'spawned task already belongs to another parent' USING ERRCODE = '23514';
    END IF;

    INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
    VALUES (
        p_parent_task_id,
        parent_handler_version,
        p_step_name,
        p_occurrence,
        jsonb_build_object('task_id', child_task_id)
    );
    RETURN QUERY SELECT child_task_id, child_created;
END;
$$;

-- Worker registry

CREATE FUNCTION pgtask.register_worker(
    p_worker_id uuid,
    p_queue_name text,
    p_version text,
    p_task_names text[],
    p_handler_versions integer[],
    p_retry_kinds text[],
    p_retry_base_delay_milliseconds bigint[],
    p_retry_factors integer[],
    p_retry_max_delay_milliseconds bigint[],
    p_ttl_milliseconds bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF cardinality(p_task_names) = 0
        OR cardinality(p_task_names) <> cardinality(p_handler_versions)
        OR cardinality(p_task_names) <> cardinality(p_retry_kinds)
        OR cardinality(p_task_names) <> cardinality(p_retry_base_delay_milliseconds)
        OR cardinality(p_task_names) <> cardinality(p_retry_factors)
        OR cardinality(p_task_names) <> cardinality(p_retry_max_delay_milliseconds)
    THEN
        RAISE EXCEPTION 'worker capabilities and retry policies must be nonempty and aligned'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM unnest(p_task_names, p_handler_versions) AS capabilities(task_name, handler_version)
        GROUP BY capabilities.task_name, capabilities.handler_version
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'worker capabilities must be unique' USING ERRCODE = '22023';
    END IF;
    IF p_ttl_milliseconds <= 0 THEN
        RAISE EXCEPTION 'worker ttl must be positive' USING ERRCODE = '22023';
    END IF;

    INSERT INTO pgtask.queues (name)
    VALUES (p_queue_name)
    ON CONFLICT DO NOTHING;

    INSERT INTO pgtask.workers (id, queue_name, version, expires_at)
    VALUES (
        p_worker_id,
        p_queue_name,
        p_version,
        statement_timestamp() + (p_ttl_milliseconds * interval '1 millisecond')
    )
    ON CONFLICT (id) DO UPDATE
    SET queue_name = EXCLUDED.queue_name,
        version = EXCLUDED.version,
        draining = false,
        heartbeat_at = statement_timestamp(),
        expires_at = EXCLUDED.expires_at;

    DELETE FROM pgtask.worker_capabilities WHERE worker_id = p_worker_id;
    INSERT INTO pgtask.worker_capabilities (worker_id, task_name, handler_version)
    SELECT p_worker_id, capabilities.task_name, capabilities.handler_version
    FROM unnest(p_task_names, p_handler_versions) AS capabilities(task_name, handler_version);

    IF EXISTS (
        SELECT 1
        FROM unnest(
            p_task_names,
            p_handler_versions,
            p_retry_kinds,
            p_retry_base_delay_milliseconds,
            p_retry_factors,
            p_retry_max_delay_milliseconds
        ) AS requested(
            task_name,
            handler_version,
            retry_kind,
            retry_base_delay_milliseconds,
            retry_factor,
            retry_max_delay_milliseconds
        )
        JOIN pgtask.handler_policies
            ON handler_policies.queue_name = p_queue_name
            AND handler_policies.task_name = requested.task_name
            AND handler_policies.handler_version = requested.handler_version
        WHERE ROW(
            handler_policies.retry_kind,
            handler_policies.retry_base_delay_milliseconds,
            handler_policies.retry_factor,
            handler_policies.retry_max_delay_milliseconds
        ) IS DISTINCT FROM ROW(
            requested.retry_kind,
            requested.retry_base_delay_milliseconds,
            requested.retry_factor,
            requested.retry_max_delay_milliseconds
        )
    ) THEN
        RAISE EXCEPTION 'retry policy is immutable for a queue, task name, and handler version'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO pgtask.handler_policies (
        queue_name,
        task_name,
        handler_version,
        retry_kind,
        retry_base_delay_milliseconds,
        retry_factor,
        retry_max_delay_milliseconds
    )
    SELECT
        p_queue_name,
        requested.task_name,
        requested.handler_version,
        requested.retry_kind,
        requested.retry_base_delay_milliseconds,
        requested.retry_factor,
        requested.retry_max_delay_milliseconds
    FROM unnest(
        p_task_names,
        p_handler_versions,
        p_retry_kinds,
        p_retry_base_delay_milliseconds,
        p_retry_factors,
        p_retry_max_delay_milliseconds
    ) AS requested(
        task_name,
        handler_version,
        retry_kind,
        retry_base_delay_milliseconds,
        retry_factor,
        retry_max_delay_milliseconds
    )
    ON CONFLICT DO NOTHING;
END;
$$;

CREATE FUNCTION pgtask.heartbeat_worker(
    p_worker_id uuid,
    p_ttl_milliseconds bigint,
    p_draining boolean
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH updated AS (
        UPDATE pgtask.workers
        SET draining = p_draining,
            heartbeat_at = statement_timestamp(),
            expires_at = statement_timestamp() + (p_ttl_milliseconds * interval '1 millisecond')
        WHERE id = p_worker_id AND p_ttl_milliseconds > 0
        RETURNING id
    )
    SELECT EXISTS(SELECT 1 FROM updated);
$$;

CREATE FUNCTION pgtask.queue_demand(
    p_queue_name text,
    p_task_names text[],
    p_handler_versions integer[]
)
RETURNS TABLE(ready_tasks bigint, capable_tasks bigint, unroutable_tasks bigint)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF cardinality(p_task_names) = 0 OR cardinality(p_task_names) <> cardinality(p_handler_versions) THEN
        RAISE EXCEPTION 'worker capabilities must be nonempty and aligned' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT
        count(tasks.id),
        count(tasks.id) FILTER (
            WHERE EXISTS (
                SELECT 1
                FROM unnest(p_task_names, p_handler_versions) AS capability(task_name, handler_version)
                WHERE capability.task_name = tasks.task_name
                    AND capability.handler_version = tasks.handler_version
            )
        ),
        count(tasks.id) FILTER (
            WHERE NOT EXISTS (
                SELECT 1
                FROM pgtask.workers
                JOIN pgtask.worker_capabilities ON worker_capabilities.worker_id = workers.id
                WHERE workers.queue_name = tasks.queue_name
                    AND workers.draining = false
                    AND workers.expires_at > statement_timestamp()
                    AND worker_capabilities.task_name = tasks.task_name
                    AND worker_capabilities.handler_version = tasks.handler_version
            )
        )
    FROM pgtask.tasks
    JOIN pgtask.queues ON queues.name = tasks.queue_name
    WHERE tasks.queue_name = p_queue_name
        AND tasks.state = 'pending'
        AND tasks.run_at <= statement_timestamp()
        AND queues.paused_at IS NULL;
END;
$$;

-- Checkpoint protocol

CREATE FUNCTION pgtask.commit_checkpoint(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_value jsonb
)
RETURNS SETOF pgtask.checkpoints
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH active AS (
        SELECT id, handler_version
        FROM pgtask.tasks
        WHERE id = p_task_id
            AND state = 'running'
            AND attempt = p_attempt
            AND lease_token = p_lease_token
    )
    INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
    SELECT id, handler_version, p_step_name, p_occurrence, p_value
    FROM active
    ON CONFLICT (task_id, handler_version, step_name, occurrence)
    DO UPDATE SET value = checkpoints.value
    RETURNING checkpoints.*;
$$;

CREATE FUNCTION pgtask.get_checkpoint(
    p_task_id uuid,
    p_handler_version integer,
    p_step_name text,
    p_occurrence integer
)
RETURNS SETOF pgtask.checkpoints
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT *
    FROM pgtask.checkpoints
    WHERE task_id = p_task_id
        AND handler_version = p_handler_version
        AND step_name = p_step_name
        AND occurrence = p_occurrence;
$$;

CREATE FUNCTION pgtask.get_task(p_task_id uuid)
RETURNS SETOF pgtask.tasks
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT * FROM pgtask.tasks WHERE id = p_task_id;
$$;

-- Schedule protocol

CREATE FUNCTION pgtask.put_schedule(
    p_id uuid,
    p_name text,
    p_kind text,
    p_interval_milliseconds bigint,
    p_cron_expression text,
    p_misfire_policy text,
    p_catch_up_limit integer,
    p_queue_name text,
    p_task_name text,
    p_handler_version integer,
    p_payload jsonb,
    p_headers jsonb,
    p_priority smallint,
    p_max_attempts integer,
    p_next_run_at timestamptz
)
RETURNS SETOF pgtask.schedules
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    INSERT INTO pgtask.queues (name) VALUES (p_queue_name) ON CONFLICT DO NOTHING;

    RETURN QUERY
    INSERT INTO pgtask.schedules (
        id, name, kind, interval_milliseconds, cron_expression, misfire_policy, catch_up_limit,
        queue_name, task_name, handler_version, payload, headers, priority, max_attempts, next_run_at
    )
    VALUES (
        p_id, p_name, p_kind, p_interval_milliseconds, p_cron_expression, p_misfire_policy, p_catch_up_limit,
        p_queue_name, p_task_name, p_handler_version, p_payload, p_headers, p_priority, p_max_attempts, p_next_run_at
    )
    ON CONFLICT (name) DO UPDATE
    SET kind = EXCLUDED.kind,
        interval_milliseconds = EXCLUDED.interval_milliseconds,
        cron_expression = EXCLUDED.cron_expression,
        misfire_policy = EXCLUDED.misfire_policy,
        catch_up_limit = EXCLUDED.catch_up_limit,
        queue_name = EXCLUDED.queue_name,
        task_name = EXCLUDED.task_name,
        handler_version = EXCLUDED.handler_version,
        payload = EXCLUDED.payload,
        headers = EXCLUDED.headers,
        priority = EXCLUDED.priority,
        max_attempts = EXCLUDED.max_attempts,
        next_run_at = EXCLUDED.next_run_at,
        updated_at = statement_timestamp()
    WHERE (schedules.kind, schedules.interval_milliseconds, schedules.cron_expression,
        schedules.misfire_policy, schedules.catch_up_limit, schedules.queue_name, schedules.task_name,
        schedules.handler_version, schedules.payload, schedules.headers, schedules.priority, schedules.max_attempts)
        IS DISTINCT FROM
        (EXCLUDED.kind, EXCLUDED.interval_milliseconds, EXCLUDED.cron_expression,
        EXCLUDED.misfire_policy, EXCLUDED.catch_up_limit, EXCLUDED.queue_name, EXCLUDED.task_name,
        EXCLUDED.handler_version, EXCLUDED.payload, EXCLUDED.headers, EXCLUDED.priority, EXCLUDED.max_attempts)
    RETURNING schedules.*;

    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM pgtask.schedules WHERE name = p_name;
    ELSE
        PERFORM pg_notify('pgtask_schedule', 'changed');
    END IF;
END;
$$;

CREATE FUNCTION pgtask.get_schedule(p_id uuid)
RETURNS SETOF pgtask.schedules
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT * FROM pgtask.schedules WHERE id = p_id;
$$;

CREATE FUNCTION pgtask.set_schedule_paused(p_id uuid, p_paused boolean)
RETURNS SETOF pgtask.schedules
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    RETURN QUERY
    UPDATE pgtask.schedules
    SET paused_at = CASE WHEN p_paused THEN COALESCE(paused_at, statement_timestamp()) ELSE NULL END,
        updated_at = statement_timestamp()
    WHERE id = p_id
    RETURNING schedules.*;
    IF FOUND THEN
        PERFORM pg_notify('pgtask_schedule', 'changed');
    END IF;
END;
$$;

CREATE FUNCTION pgtask.delete_schedule(p_id uuid)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    deleted boolean;
BEGIN
    WITH removed AS (
        DELETE FROM pgtask.schedules WHERE id = p_id RETURNING id
    )
    SELECT EXISTS(SELECT 1 FROM removed) INTO deleted;
    IF deleted THEN
        PERFORM pg_notify('pgtask_schedule', 'changed');
    END IF;
    RETURN deleted;
END;
$$;

CREATE FUNCTION pgtask.claim_due_schedules(p_limit integer)
RETURNS SETOF pgtask.schedules
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT *
    FROM pgtask.schedules
    WHERE paused_at IS NULL AND next_run_at <= statement_timestamp()
    ORDER BY next_run_at, id
    FOR NO KEY UPDATE SKIP LOCKED
    LIMIT p_limit;
$$;

CREATE FUNCTION pgtask.materialize_schedule(
    p_id uuid,
    p_expected_next_run_at timestamptz,
    p_occurrences timestamptz[],
    p_next_run_at timestamptz
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    allowed_count integer;
    capacity_outstanding bigint;
    maximum bigint;
    materialized bigint;
    occurrence_count integer := COALESCE(cardinality(p_occurrences), 0);
    target pgtask.schedules%ROWTYPE;
BEGIN
    SELECT schedules.*
    INTO target
    FROM pgtask.schedules
    WHERE schedules.id = p_id
        AND schedules.paused_at IS NULL
        AND schedules.next_run_at = p_expected_next_run_at
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    SELECT queues.max_outstanding_tasks, queues.capacity_outstanding_tasks
    INTO maximum, capacity_outstanding
    FROM pgtask.queues
    WHERE queues.name = target.queue_name
    FOR UPDATE;

    IF maximum IS NULL THEN
        allowed_count = occurrence_count;
    ELSE
        allowed_count = GREATEST(
            LEAST(occurrence_count::bigint, maximum - capacity_outstanding),
            0
        )::integer;
    END IF;

    UPDATE pgtask.schedules
    SET next_run_at = CASE
            WHEN allowed_count < occurrence_count THEN p_occurrences[allowed_count + 1]
            ELSE p_next_run_at
        END,
        updated_at = statement_timestamp()
    WHERE id = p_id;

    WITH inserted AS (
        INSERT INTO pgtask.tasks (
            id, queue_name, task_name, handler_version, payload, headers, priority, run_at,
            max_attempts, schedule_id, scheduled_for
        )
        SELECT
            gen_random_uuid(), target.queue_name, target.task_name, target.handler_version,
            target.payload, target.headers, target.priority, occurrence, target.max_attempts,
            target.id, occurrence
        FROM unnest(p_occurrences[1:allowed_count]) AS occurrence
        ON CONFLICT (schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL DO NOTHING
        RETURNING id
    )
    SELECT count(*) INTO materialized FROM inserted;

    IF materialized > 0 THEN
        PERFORM pg_notify('pgtask_ready', target.queue_name);
    END IF;
    RETURN materialized;
END;
$$;

CREATE FUNCTION pgtask.next_schedule_delay_milliseconds()
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT GREATEST(
        0,
        ceil(EXTRACT(epoch FROM (min(next_run_at) - statement_timestamp())) * 1000)::bigint
    )
    FROM pgtask.schedules
    WHERE paused_at IS NULL;
$$;

-- Signal protocol

CREATE FUNCTION pgtask.emit_signal(
    p_task_id uuid,
    p_signal_name text,
    p_occurrence integer,
    p_value jsonb
)
RETURNS SETOF pgtask.signals
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target pgtask.signals%ROWTYPE;
    queues text[];
    queue_name text;
BEGIN
    INSERT INTO pgtask.signals (task_id, signal_name, occurrence, value)
    VALUES (p_task_id, p_signal_name, p_occurrence, p_value)
    ON CONFLICT (task_id, signal_name, occurrence)
    DO UPDATE SET value = signals.value
    RETURNING * INTO target;

    WITH matching AS (
        SELECT waits.*
        FROM pgtask.waits
        WHERE waits.task_id = p_task_id
            AND waits.signal_name = p_signal_name
            AND waits.signal_occurrence = p_occurrence
            AND waits.resolved_at IS NULL
        FOR UPDATE
    ),
    checkpointed AS (
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        SELECT
            matching.task_id,
            matching.handler_version,
            matching.step_name,
            matching.occurrence,
            jsonb_build_object('outcome', 'signal', 'value', target.value)
        FROM matching
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value
        RETURNING task_id
    ),
    woken AS (
        UPDATE pgtask.tasks
        SET state = 'pending',
            run_at = statement_timestamp(),
            updated_at = statement_timestamp()
        FROM matching
        WHERE tasks.id = matching.task_id AND tasks.state = 'waiting'
        RETURNING tasks.queue_name
    ),
    resolved AS (
        UPDATE pgtask.waits
        SET resolved_at = statement_timestamp(), outcome = 'signal'
        FROM matching
        WHERE waits.task_id = matching.task_id
            AND waits.handler_version = matching.handler_version
            AND waits.step_name = matching.step_name
            AND waits.occurrence = matching.occurrence
    )
    SELECT array_agg(DISTINCT woken.queue_name) INTO queues FROM woken;

    FOREACH queue_name IN ARRAY COALESCE(queues, ARRAY[]::text[])
    LOOP
        PERFORM pg_notify('pgtask_ready', queue_name);
    END LOOP;
    RETURN NEXT target;
END;
$$;

CREATE FUNCTION pgtask.wait_for_signal(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_signal_name text,
    p_signal_occurrence integer,
    p_timeout_milliseconds bigint
)
RETURNS TABLE(status text, checkpoint jsonb)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target_handler_version integer;
    signal_value jsonb;
    checkpoint_value jsonb;
    timeout_at timestamptz;
BEGIN
    IF p_timeout_milliseconds IS NOT NULL AND p_timeout_milliseconds < 0 THEN
        RAISE EXCEPTION 'signal timeout must not be negative' USING ERRCODE = '22023';
    END IF;

    SELECT handler_version
    INTO target_handler_version
    FROM pgtask.tasks
    WHERE id = p_task_id
        AND state = 'running'
        AND attempt = p_attempt
        AND lease_token = p_lease_token
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT value
    INTO checkpoint_value
    FROM pgtask.checkpoints
    WHERE task_id = p_task_id
        AND handler_version = target_handler_version
        AND step_name = p_step_name
        AND occurrence = p_occurrence;

    IF FOUND THEN
        RETURN QUERY SELECT 'ready'::text, checkpoint_value;
        RETURN;
    END IF;

    SELECT value
    INTO signal_value
    FROM pgtask.signals
    WHERE task_id = p_task_id
        AND signal_name = p_signal_name
        AND occurrence = p_signal_occurrence;

    IF FOUND THEN
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        VALUES (
            p_task_id,
            target_handler_version,
            p_step_name,
            p_occurrence,
            jsonb_build_object('outcome', 'signal', 'value', signal_value)
        )
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value
        RETURNING value INTO checkpoint_value;
        RETURN QUERY SELECT 'ready'::text, checkpoint_value;
        RETURN;
    END IF;

    timeout_at = CASE
        WHEN p_timeout_milliseconds IS NULL THEN NULL
        ELSE statement_timestamp() + (p_timeout_milliseconds * interval '1 millisecond')
    END;

    INSERT INTO pgtask.waits (
        task_id, handler_version, step_name, occurrence, signal_name, signal_occurrence, timeout_at
    )
    VALUES (
        p_task_id, target_handler_version, p_step_name, p_occurrence, p_signal_name, p_signal_occurrence, timeout_at
    );

    UPDATE pgtask.tasks
    SET state = 'waiting',
        lease_token = NULL,
        lease_owner = NULL,
        lease_expires_at = NULL,
        updated_at = statement_timestamp()
    WHERE id = p_task_id;

    UPDATE pgtask.attempts
    SET state = 'suspended', finished_at = statement_timestamp()
    WHERE task_id = p_task_id AND attempt = p_attempt;

    PERFORM pg_notify('pgtask_wait', 'changed');
    RETURN QUERY SELECT 'waiting'::text, NULL::jsonb;
END;
$$;

CREATE FUNCTION pgtask.recover_wait_timeouts(p_limit integer)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    recovered bigint;
    queues text[];
    queue_name text;
BEGIN
    WITH due AS (
        SELECT waits.*
        FROM pgtask.waits
        WHERE waits.resolved_at IS NULL
            AND waits.timeout_at IS NOT NULL
            AND waits.timeout_at <= statement_timestamp()
        ORDER BY waits.timeout_at, waits.task_id
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    ),
    checkpointed AS (
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        SELECT
            due.task_id,
            due.handler_version,
            due.step_name,
            due.occurrence,
            jsonb_build_object('outcome', 'timeout')
        FROM due
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value
        RETURNING task_id
    ),
    woken AS (
        UPDATE pgtask.tasks
        SET state = 'pending',
            run_at = statement_timestamp(),
            updated_at = statement_timestamp()
        FROM due
        WHERE tasks.id = due.task_id AND tasks.state = 'waiting'
        RETURNING tasks.queue_name
    ),
    resolved AS (
        UPDATE pgtask.waits
        SET resolved_at = statement_timestamp(), outcome = 'timeout'
        FROM due
        WHERE waits.task_id = due.task_id
            AND waits.handler_version = due.handler_version
            AND waits.step_name = due.step_name
            AND waits.occurrence = due.occurrence
        RETURNING waits.task_id
    )
    SELECT count(DISTINCT resolved.task_id), array_agg(DISTINCT woken.queue_name)
    INTO recovered, queues
    FROM resolved
    LEFT JOIN woken ON true;

    FOREACH queue_name IN ARRAY COALESCE(queues, ARRAY[]::text[])
    LOOP
        PERFORM pg_notify('pgtask_ready', queue_name);
    END LOOP;
    RETURN recovered;
END;
$$;

CREATE FUNCTION pgtask.next_wait_delay_milliseconds()
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT GREATEST(
        0,
        ceil(EXTRACT(epoch FROM (min(deadlines.timeout_at) - statement_timestamp())) * 1000)::bigint
    )
    FROM (
        SELECT waits.timeout_at
        FROM pgtask.waits
        WHERE waits.resolved_at IS NULL AND waits.timeout_at IS NOT NULL
        UNION ALL
        SELECT result_waits.timeout_at
        FROM pgtask.result_waits
        WHERE result_waits.resolved_at IS NULL AND result_waits.timeout_at IS NOT NULL
    ) AS deadlines;
$$;

-- Result protocol

CREATE FUNCTION pgtask.wait_for_result(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_result_task_id uuid,
    p_timeout_milliseconds bigint
)
RETURNS TABLE(status text, checkpoint jsonb)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target_handler_version integer;
    result_state text;
    result_value jsonb;
    result_error jsonb;
    checkpoint_value jsonb;
BEGIN
    IF p_task_id = p_result_task_id THEN
        RAISE EXCEPTION 'a task cannot wait for its own result' USING ERRCODE = '22023';
    END IF;
    IF p_timeout_milliseconds IS NOT NULL AND p_timeout_milliseconds <= 0 THEN
        RAISE EXCEPTION 'result wait timeout must be positive' USING ERRCODE = '22023';
    END IF;

    SELECT handler_version
    INTO target_handler_version
    FROM pgtask.tasks
    WHERE id = p_task_id
        AND state = 'running'
        AND attempt = p_attempt
        AND lease_token = p_lease_token
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT value
    INTO checkpoint_value
    FROM pgtask.checkpoints
    WHERE task_id = p_task_id
        AND handler_version = target_handler_version
        AND step_name = p_step_name
        AND occurrence = p_occurrence;

    IF FOUND THEN
        RETURN QUERY SELECT 'ready'::text, checkpoint_value;
        RETURN;
    END IF;

    SELECT state, result, error
    INTO result_state, result_value, result_error
    FROM pgtask.tasks
    WHERE id = p_result_task_id AND parent_task_id = p_task_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'result task is not a direct child of this task' USING ERRCODE = '22023';
    END IF;

    IF result_state IN ('succeeded', 'failed', 'cancelled') THEN
        checkpoint_value = jsonb_build_object(
            'state', result_state,
            'result', result_value,
            'error', result_error
        );
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        VALUES (p_task_id, target_handler_version, p_step_name, p_occurrence, checkpoint_value)
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value
        RETURNING value INTO checkpoint_value;
        RETURN QUERY SELECT 'ready'::text, checkpoint_value;
        RETURN;
    END IF;

    INSERT INTO pgtask.result_waits (
        task_id, handler_version, step_name, occurrence, result_task_id, timeout_at
    )
    VALUES (
        p_task_id,
        target_handler_version,
        p_step_name,
        p_occurrence,
        p_result_task_id,
        CASE
            WHEN p_timeout_milliseconds IS NULL THEN NULL
            ELSE statement_timestamp() + (p_timeout_milliseconds * interval '1 millisecond')
        END
    );

    UPDATE pgtask.tasks
    SET state = 'waiting',
        lease_token = NULL,
        lease_owner = NULL,
        lease_expires_at = NULL,
        updated_at = statement_timestamp()
    WHERE id = p_task_id;

    UPDATE pgtask.attempts
    SET state = 'suspended', finished_at = statement_timestamp()
    WHERE task_id = p_task_id AND attempt = p_attempt;

    IF p_timeout_milliseconds IS NOT NULL THEN
        PERFORM pg_notify('pgtask_wait', 'changed');
    END IF;
    RETURN QUERY SELECT 'waiting'::text, NULL::jsonb;
END;
$$;

CREATE FUNCTION pgtask.resolve_task_result()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    queues text[];
    queue_name text;
BEGIN
    WITH matching AS (
        SELECT result_waits.*
        FROM pgtask.result_waits
        JOIN pgtask.tasks AS parents ON parents.id = result_waits.task_id
        WHERE result_waits.result_task_id = NEW.id
            AND result_waits.resolved_at IS NULL
            AND parents.state = 'waiting'
        FOR UPDATE OF result_waits
    ),
    checkpointed AS (
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        SELECT
            matching.task_id,
            matching.handler_version,
            matching.step_name,
            matching.occurrence,
            jsonb_build_object('state', NEW.state, 'result', NEW.result, 'error', NEW.error)
        FROM matching
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value
        RETURNING task_id
    ),
    woken AS (
        UPDATE pgtask.tasks
        SET state = 'pending',
            run_at = statement_timestamp(),
            updated_at = statement_timestamp()
        FROM matching
        WHERE tasks.id = matching.task_id AND tasks.state = 'waiting'
        RETURNING tasks.queue_name
    ),
    resolved AS (
        UPDATE pgtask.result_waits
        SET resolved_at = statement_timestamp(), outcome = NEW.state
        FROM matching
        WHERE result_waits.task_id = matching.task_id
            AND result_waits.handler_version = matching.handler_version
            AND result_waits.step_name = matching.step_name
            AND result_waits.occurrence = matching.occurrence
    )
    SELECT array_agg(DISTINCT woken.queue_name) INTO queues FROM woken;

    UPDATE pgtask.result_waits
    SET resolved_at = statement_timestamp(), outcome = 'cancelled'
    WHERE task_id = NEW.id AND resolved_at IS NULL;

    FOREACH queue_name IN ARRAY COALESCE(queues, ARRAY[]::text[])
    LOOP
        PERFORM pg_notify('pgtask_ready', queue_name);
    END LOOP;
    PERFORM pg_notify('pgtask_result', NEW.id::text);
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtask.recover_result_wait_timeouts(p_limit integer)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target pgtask.result_waits%ROWTYPE;
    parent_queue text;
    recovered bigint := 0;
BEGIN
    FOR target IN
        SELECT result_waits.*
        FROM pgtask.result_waits
        WHERE result_waits.resolved_at IS NULL
            AND result_waits.timeout_at IS NOT NULL
            AND result_waits.timeout_at <= statement_timestamp()
        ORDER BY result_waits.timeout_at, result_waits.task_id
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    LOOP
        parent_queue := NULL;
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        VALUES (
            target.task_id,
            target.handler_version,
            target.step_name,
            target.occurrence,
            jsonb_build_object('state', 'timeout', 'result', NULL, 'error', NULL)
        )
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value;

        UPDATE pgtask.result_waits
        SET resolved_at = statement_timestamp(), outcome = 'timeout'
        WHERE task_id = target.task_id
            AND handler_version = target.handler_version
            AND step_name = target.step_name
            AND occurrence = target.occurrence;

        UPDATE pgtask.tasks
        SET state = 'pending', run_at = statement_timestamp(), updated_at = statement_timestamp()
        WHERE id = target.task_id AND state = 'waiting'
        RETURNING queue_name INTO parent_queue;

        WITH cancelled AS (
            UPDATE pgtask.tasks
            SET state = 'cancelled',
                cancel_requested_at = statement_timestamp(),
                lease_token = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL,
                completed_at = statement_timestamp(),
                updated_at = statement_timestamp(),
                error = jsonb_build_object('type', 'result_wait_timeout', 'parent_task_id', target.task_id)
            WHERE id = target.result_task_id AND state IN ('pending', 'running', 'waiting')
            RETURNING id, attempt
        )
        UPDATE pgtask.attempts
        SET state = 'cancelled',
            finished_at = statement_timestamp(),
            error = jsonb_build_object('type', 'result_wait_timeout', 'parent_task_id', target.task_id)
        FROM cancelled
        WHERE attempts.task_id = cancelled.id
            AND attempts.attempt = cancelled.attempt
            AND attempts.state = 'running';

        IF parent_queue IS NOT NULL THEN
            PERFORM pg_notify(pgtask.ready_channel(parent_queue), parent_queue);
        END IF;
        recovered := recovered + 1;
    END LOOP;
    RETURN recovered;
END;
$$;

-- Retention

CREATE FUNCTION pgtask.delete_expired_terminal(p_queue_name text, p_limit integer)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH candidates AS (
        SELECT tasks.id
        FROM pgtask.tasks
        JOIN pgtask.queues ON queues.name = tasks.queue_name
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state IN ('succeeded', 'failed', 'cancelled')
            AND tasks.completed_at <= statement_timestamp()
                - (queues.terminal_retention_seconds * interval '1 second')
            AND NOT EXISTS (
                SELECT 1 FROM pgtask.tasks AS children WHERE children.parent_task_id = tasks.id
            )
        ORDER BY tasks.completed_at, tasks.id
        FOR UPDATE OF tasks SKIP LOCKED
        LIMIT p_limit
    ),
    deleted AS (
        DELETE FROM pgtask.tasks AS tasks
        USING candidates
        WHERE tasks.id = candidates.id
        RETURNING tasks.id
    )
    SELECT count(*) FROM deleted;
$$;

CREATE FUNCTION pgtask.delete_expired_idempotency_keys(p_queue_name text, p_limit integer)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH candidates AS (
        SELECT idempotency_keys.queue_name, idempotency_keys.idempotency_key
        FROM pgtask.idempotency_keys
        WHERE idempotency_keys.queue_name = p_queue_name
            AND idempotency_keys.expires_at <= statement_timestamp()
        ORDER BY idempotency_keys.expires_at, idempotency_keys.idempotency_key
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    ),
    deleted AS (
        DELETE FROM pgtask.idempotency_keys
        USING candidates
        WHERE idempotency_keys.queue_name = candidates.queue_name
            AND idempotency_keys.idempotency_key = candidates.idempotency_key
        RETURNING idempotency_keys.idempotency_key
    )
    SELECT count(*) FROM deleted;
$$;

CREATE FUNCTION pgtask.manage_idempotency_key()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF NEW.idempotency_key IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.state IN ('succeeded', 'failed', 'cancelled')
        AND OLD.state NOT IN ('succeeded', 'failed', 'cancelled')
    THEN
        UPDATE pgtask.idempotency_keys
        SET expires_at = COALESCE(NEW.completed_at, statement_timestamp())
            + (
                SELECT queues.idempotency_retention_seconds * interval '1 second'
                FROM pgtask.queues
                WHERE queues.name = NEW.queue_name
            )
        WHERE queue_name = NEW.queue_name
            AND idempotency_key = NEW.idempotency_key
            AND task_id = NEW.id;
    ELSIF NEW.state NOT IN ('succeeded', 'failed', 'cancelled')
        AND OLD.state IN ('succeeded', 'failed', 'cancelled')
    THEN
        UPDATE pgtask.idempotency_keys
        SET expires_at = NULL
        WHERE queue_name = NEW.queue_name
            AND idempotency_key = NEW.idempotency_key
            AND task_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtask.preserve_deleted_idempotency_key()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF OLD.idempotency_key IS NOT NULL THEN
        UPDATE pgtask.idempotency_keys
        SET expires_at = COALESCE(
            expires_at,
            statement_timestamp() + (
                SELECT queues.idempotency_retention_seconds * interval '1 second'
                FROM pgtask.queues
                WHERE queues.name = OLD.queue_name
            )
        )
        WHERE queue_name = OLD.queue_name
            AND idempotency_key = OLD.idempotency_key
            AND task_id = OLD.id;
    END IF;
    RETURN OLD;
END;
$$;

-- Administration

CREATE FUNCTION pgtask.cancel_task(p_task_id uuid)
RETURNS TABLE(queue_name text, task_name text)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH cancelled AS (
        UPDATE pgtask.tasks
        SET state = 'cancelled',
            cancel_requested_at = statement_timestamp(),
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = statement_timestamp(),
            updated_at = statement_timestamp(),
            error = jsonb_build_object('type', 'cancelled')
        WHERE id = p_task_id AND state IN ('pending', 'running', 'waiting')
        RETURNING id, queue_name, task_name, attempt
    ),
    cancelled_attempt AS (
        UPDATE pgtask.attempts AS attempts
        SET state = 'cancelled',
            finished_at = statement_timestamp(),
            error = jsonb_build_object('type', 'cancelled')
        FROM cancelled
        WHERE attempts.task_id = cancelled.id
            AND attempts.attempt = cancelled.attempt
            AND attempts.state = 'running'
    )
    SELECT cancelled.queue_name, cancelled.task_name FROM cancelled;
$$;

CREATE FUNCTION pgtask.admin_cancel_task(p_task_id uuid, p_actor text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    changed boolean;
BEGIN
    IF p_actor IS NULL OR p_actor = '' OR octet_length(p_actor) > 255 THEN
        RAISE EXCEPTION 'invalid administrator actor' USING ERRCODE = '22023';
    END IF;
    SELECT EXISTS(SELECT 1 FROM pgtask.cancel_task(p_task_id)) INTO changed;
    IF changed THEN
        INSERT INTO pgtask.administrator_audit (actor, action, task_id)
        VALUES (p_actor, 'task.cancel', p_task_id);
    END IF;
    RETURN changed;
END;
$$;

CREATE FUNCTION pgtask.admin_retry_task(p_task_id uuid, p_actor text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    task_queue text;
BEGIN
    IF p_actor IS NULL OR p_actor = '' OR octet_length(p_actor) > 255 THEN
        RAISE EXCEPTION 'invalid administrator actor' USING ERRCODE = '22023';
    END IF;
    UPDATE pgtask.tasks
    SET state = 'pending',
        run_at = statement_timestamp(),
        max_attempts = GREATEST(max_attempts, attempt + 1),
        lease_token = NULL,
        lease_owner = NULL,
        lease_expires_at = NULL,
        cancel_requested_at = NULL,
        completed_at = NULL,
        result = NULL,
        error = NULL,
        updated_at = statement_timestamp()
    WHERE id = p_task_id AND state IN ('failed', 'cancelled')
    RETURNING queue_name INTO task_queue;
    IF task_queue IS NULL THEN
        RETURN false;
    END IF;
    INSERT INTO pgtask.administrator_audit (actor, action, task_id)
    VALUES (p_actor, 'task.retry', p_task_id);
    PERFORM pg_notify('pgtask_ready', task_queue);
    RETURN true;
END;
$$;

CREATE FUNCTION pgtask.admin_set_schedule_paused(p_schedule_id uuid, p_paused boolean, p_actor text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    changed boolean;
BEGIN
    IF p_actor IS NULL OR p_actor = '' OR octet_length(p_actor) > 255 THEN
        RAISE EXCEPTION 'invalid administrator actor' USING ERRCODE = '22023';
    END IF;
    SELECT EXISTS(SELECT 1 FROM pgtask.set_schedule_paused(p_schedule_id, p_paused)) INTO changed;
    IF changed THEN
        INSERT INTO pgtask.administrator_audit (actor, action, schedule_id)
        VALUES (p_actor, CASE WHEN p_paused THEN 'schedule.pause' ELSE 'schedule.resume' END, p_schedule_id);
    END IF;
    RETURN changed;
END;
$$;

-- Notification shards

CREATE FUNCTION pgtask.ready_channel(p_queue_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT format(
        'pgtask_ready_%s',
        lpad(((hashtextextended(p_queue_name, 0) & 63)::integer)::text, 2, '0')
    );
$$;

CREATE FUNCTION pgtask.result_channel(p_task_id uuid)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT format(
        'pgtask_result_%s',
        lpad(((hashtextextended(p_task_id::text, 0) & 63)::integer)::text, 2, '0')
    );
$$;

CREATE FUNCTION pgtask.notify_ready_shard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF NEW.state = 'pending' AND (
        TG_OP = 'INSERT'
        OR OLD.state IS DISTINCT FROM NEW.state
        OR OLD.run_at IS DISTINCT FROM NEW.run_at
        OR OLD.queue_name IS DISTINCT FROM NEW.queue_name
    ) THEN
        PERFORM pg_notify(pgtask.ready_channel(NEW.queue_name), NEW.queue_name);
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtask.notify_result_shard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    PERFORM pg_notify(pgtask.result_channel(NEW.id), NEW.id::text);
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtask.notify_resumed_queue_shard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF OLD.paused_at IS NOT NULL AND NEW.paused_at IS NULL THEN
        PERFORM pg_notify(pgtask.ready_channel(NEW.name), NEW.name);
    END IF;
    RETURN NEW;
END;
$$;

-- Storage protocol

CREATE FUNCTION pgtask.storage_protocol_version()
RETURNS integer
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 1;
$$;

CREATE FUNCTION pgtask.storage_protocol_range()
RETURNS TABLE(minimum integer, maximum integer)
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 1, 1;
$$;

-- Worker idle timing

CREATE FUNCTION pgtask.next_task_delay_milliseconds(
    p_queue_name text,
    p_task_names text[],
    p_handler_versions integer[]
)
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT GREATEST(
        0,
        ceil(EXTRACT(epoch FROM (min(tasks.run_at) - statement_timestamp())) * 1000)::bigint
    )
    FROM pgtask.tasks
    WHERE tasks.queue_name = p_queue_name
        AND tasks.state = 'pending'
        AND tasks.attempt < tasks.max_attempts
        AND EXISTS (
            SELECT 1
            FROM pgtask.queues
            WHERE queues.name = tasks.queue_name AND queues.paused_at IS NULL
        )
        AND EXISTS (
            SELECT 1
            FROM unnest(p_task_names, p_handler_versions) AS handlers(task_name, handler_version)
            WHERE handlers.task_name = tasks.task_name
                AND handlers.handler_version = tasks.handler_version
        );
$$;

-- Table triggers

CREATE FUNCTION pgtask.ensure_queue_for_task()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO pgtask.queues (name)
    VALUES (NEW.queue_name)
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtask.snapshot_task_retry_policy()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF NEW.retry_kind IS NULL THEN
        SELECT
            handler_policies.retry_kind,
            handler_policies.retry_base_delay_milliseconds,
            handler_policies.retry_factor,
            handler_policies.retry_max_delay_milliseconds
        INTO
            NEW.retry_kind,
            NEW.retry_base_delay_milliseconds,
            NEW.retry_factor,
            NEW.retry_max_delay_milliseconds
        FROM pgtask.handler_policies
        WHERE handler_policies.queue_name = NEW.queue_name
            AND handler_policies.task_name = NEW.task_name
            AND handler_policies.handler_version = NEW.handler_version;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtask.enforce_queue_capacity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    maximum bigint;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.state IN ('pending', 'running', 'waiting')
            AND NEW.state IN ('succeeded', 'failed', 'cancelled')
        THEN
            UPDATE pgtask.queues
            SET capacity_outstanding_tasks = GREATEST(capacity_outstanding_tasks - 1, 0)
            WHERE name = NEW.queue_name AND max_outstanding_tasks IS NOT NULL;
            RETURN NEW;
        END IF;
        IF OLD.state IN ('pending', 'running', 'waiting')
            OR NEW.state IN ('succeeded', 'failed', 'cancelled')
        THEN
            RETURN NEW;
        END IF;
    ELSIF NEW.state IN ('succeeded', 'failed', 'cancelled') THEN
        RETURN NEW;
    END IF;

    UPDATE pgtask.queues
    SET capacity_outstanding_tasks = capacity_outstanding_tasks + 1
    WHERE name = NEW.queue_name
        AND max_outstanding_tasks IS NOT NULL
        AND capacity_outstanding_tasks < max_outstanding_tasks
    RETURNING max_outstanding_tasks INTO maximum;

    IF FOUND THEN
        RETURN NEW;
    END IF;

    SELECT queues.max_outstanding_tasks
    INTO maximum
    FROM pgtask.queues
    WHERE queues.name = NEW.queue_name;

    IF maximum IS NOT NULL THEN
        RAISE EXCEPTION 'queue % has reached its capacity of % outstanding tasks', NEW.queue_name, maximum
            USING ERRCODE = 'PT001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtask.cancel_owned_children()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    WITH cancelled AS (
        UPDATE pgtask.tasks
        SET state = 'cancelled',
            cancel_requested_at = statement_timestamp(),
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = statement_timestamp(),
            updated_at = statement_timestamp(),
            error = jsonb_build_object('type', 'parent_finished', 'parent_task_id', NEW.id, 'parent_state', NEW.state)
        WHERE parent_task_id = NEW.id AND state IN ('pending', 'running', 'waiting')
        RETURNING id, attempt
    )
    UPDATE pgtask.attempts
    SET state = 'cancelled',
        finished_at = statement_timestamp(),
        error = jsonb_build_object('type', 'parent_finished', 'parent_task_id', NEW.id, 'parent_state', NEW.state)
    FROM cancelled
    WHERE attempts.task_id = cancelled.id
        AND attempts.attempt = cancelled.attempt
        AND attempts.state = 'running';
    RETURN NEW;
END;
$$;

CREATE TRIGGER ensure_queue_for_task
BEFORE INSERT ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.ensure_queue_for_task();

CREATE TRIGGER tasks_resolve_result
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
WHEN (
    OLD.state NOT IN ('succeeded', 'failed', 'cancelled')
    AND NEW.state IN ('succeeded', 'failed', 'cancelled')
)
EXECUTE FUNCTION pgtask.resolve_task_result();

CREATE TRIGGER tasks_notify_ready_shard
AFTER INSERT OR UPDATE OF state, run_at, queue_name ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.notify_ready_shard();

CREATE TRIGGER tasks_notify_result_shard
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
WHEN (
    OLD.state NOT IN ('succeeded', 'failed', 'cancelled')
    AND NEW.state IN ('succeeded', 'failed', 'cancelled')
)
EXECUTE FUNCTION pgtask.notify_result_shard();

CREATE TRIGGER queues_notify_resumed_shard
AFTER UPDATE OF paused_at ON pgtask.queues
FOR EACH ROW
EXECUTE FUNCTION pgtask.notify_resumed_queue_shard();

CREATE TRIGGER tasks_cancel_owned_children
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
WHEN (
    OLD.state NOT IN ('succeeded', 'failed', 'cancelled')
    AND NEW.state IN ('succeeded', 'failed', 'cancelled')
)
EXECUTE FUNCTION pgtask.cancel_owned_children();

CREATE TRIGGER tasks_snapshot_retry_policy
BEFORE INSERT ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.snapshot_task_retry_policy();

CREATE TRIGGER tasks_manage_idempotency_key
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.manage_idempotency_key();

CREATE TRIGGER tasks_preserve_deleted_idempotency_key
AFTER DELETE ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.preserve_deleted_idempotency_key();

CREATE TRIGGER tasks_queue_capacity
BEFORE INSERT OR UPDATE OF state ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.enforce_queue_capacity();

-- Roles

CREATE FUNCTION pgtask.configure_grants(
    p_owner regrole,
    p_producer regrole,
    p_worker regrole,
    p_observer regrole,
    p_administrator regrole
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target regrole;
    readable constant text = 'pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, '
        'pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, '
        'pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.wait_view, pgtask.result_wait_view, '
        'pgtask.administrator_audit_view, pgtask.handler_policy_view';
BEGIN
    FOREACH target IN ARRAY ARRAY[p_owner, p_producer, p_worker, p_observer, p_administrator]
    LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA pgtask TO %s', target);
        EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.storage_protocol_range() TO %s', target);
    END LOOP;

    EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pgtask TO %s', p_owner);
    EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtask TO %s', p_owner);

    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.enqueue(text, jsonb, text, integer, timestamptz, smallint, integer, text, jsonb) TO %s',
        p_producer
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.enqueue_many(jsonb) TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.emit_signal(uuid, text, integer, jsonb) TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.task_result(uuid) TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.result_channel(uuid) TO %s', p_producer);

    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.claim(text, uuid, text[], integer[], integer, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.renew_leases(uuid[], integer[], uuid[], bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.complete_task(uuid, integer, uuid, jsonb) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.fail_task(uuid, integer, uuid, jsonb, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.recover_expired(text, integer) TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.register_worker(uuid, text, text, text[], integer[], text[], bigint[], integer[], bigint[], bigint) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.commit_checkpoint(uuid, integer, uuid, text, integer, jsonb) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_task(uuid) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.claim_due_schedules(integer) TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.materialize_schedule(uuid, timestamptz, timestamptz[], timestamptz) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.next_schedule_delay_milliseconds() TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.next_task_delay_milliseconds(text, text[], integer[]) TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.suspend_task(uuid, integer, uuid, text, integer, timestamptz, bigint) TO %s',
        p_worker
    );
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.wait_for_signal(uuid, integer, uuid, text, integer, text, integer, bigint) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.recover_wait_timeouts(integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.next_wait_delay_milliseconds() TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid, bigint) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.recover_result_wait_timeouts(integer) TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.spawn_task(uuid, integer, uuid, text, integer, text, jsonb, text, integer, timestamptz, smallint, integer, jsonb) TO %s',
        p_worker
    );
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.put_schedule(uuid, text, text, bigint, text, text, integer, text, text, integer, jsonb, jsonb, smallint, integer, timestamptz) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_idempotency_keys(text, integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.queue_demand(text, text[], integer[]) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.storage_protocol_version() TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.ready_channel(text) TO %s', p_worker);

    EXECUTE format('GRANT SELECT ON %s TO %s', readable, p_observer);

    EXECUTE format('GRANT SELECT ON %s TO %s', readable, p_administrator);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.put_queue(text, bigint, bigint, bigint, bigint) TO %s',
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.set_queue_paused(text, boolean) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.cancel_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s', p_administrator);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.delete_expired_idempotency_keys(text, integer) TO %s',
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.task_result(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.result_channel(uuid) TO %s', p_administrator);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.put_schedule(uuid, text, text, bigint, text, text, integer, text, text, integer, jsonb, jsonb, smallint, integer, timestamptz) TO %s',
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_schedule(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.set_schedule_paused(uuid, boolean) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_schedule(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.admin_cancel_task(uuid, text) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.admin_retry_task(uuid, text) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.admin_set_schedule_paused(uuid, boolean, text) TO %s', p_administrator);
END;
$$;

-- Privileges

REVOKE ALL ON SCHEMA pgtask FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA pgtask FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgtask FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgtask REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
