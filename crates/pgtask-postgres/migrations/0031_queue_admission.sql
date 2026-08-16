ALTER TABLE pgtask.queues
ADD COLUMN max_outstanding_tasks bigint CHECK (max_outstanding_tasks > 0),
ADD COLUMN starvation_timeout_seconds bigint NOT NULL DEFAULT 300
CHECK (starvation_timeout_seconds >= 0);

CREATE INDEX tasks_outstanding_idx
ON pgtask.tasks (queue_name, id)
WHERE state IN ('pending', 'running', 'waiting');

CREATE INDEX tasks_oldest_ready_idx
ON pgtask.tasks (queue_name, run_at, id)
WHERE state = 'pending';

CREATE FUNCTION pgtask.enforce_queue_capacity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    maximum bigint;
    outstanding bigint;
BEGIN
    IF NEW.state IN ('succeeded', 'failed', 'cancelled') THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.state IN ('pending', 'running', 'waiting') THEN
        RETURN NEW;
    END IF;

    SELECT queues.max_outstanding_tasks
    INTO maximum
    FROM pgtask.queues
    WHERE queues.name = NEW.queue_name;

    IF maximum IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT queues.max_outstanding_tasks
    INTO maximum
    FROM pgtask.queues
    WHERE queues.name = NEW.queue_name
    FOR UPDATE;

    IF maximum IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT count(*)
    INTO outstanding
    FROM pgtask.tasks
    WHERE tasks.queue_name = NEW.queue_name
        AND tasks.state IN ('pending', 'running', 'waiting');

    IF outstanding >= maximum THEN
        RAISE EXCEPTION 'queue % has reached its capacity of % outstanding tasks', NEW.queue_name, maximum
            USING ERRCODE = 'PT001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_queue_capacity
BEFORE INSERT OR UPDATE OF state ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.enforce_queue_capacity();

CREATE OR REPLACE FUNCTION pgtask.claim(
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

CREATE OR REPLACE FUNCTION pgtask.materialize_schedule(
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
    maximum bigint;
    materialized bigint;
    occurrence_count integer := COALESCE(cardinality(p_occurrences), 0);
    outstanding bigint;
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

    SELECT queues.max_outstanding_tasks
    INTO maximum
    FROM pgtask.queues
    WHERE queues.name = target.queue_name
    FOR UPDATE;

    IF maximum IS NULL THEN
        allowed_count = occurrence_count;
    ELSE
        SELECT count(*)
        INTO outstanding
        FROM pgtask.tasks
        WHERE tasks.queue_name = target.queue_name
            AND tasks.state IN ('pending', 'running', 'waiting');
        allowed_count = GREATEST(LEAST(occurrence_count::bigint, maximum - outstanding), 0)::integer;
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

CREATE FUNCTION pgtask.put_queue(
    p_name text,
    p_terminal_retention_seconds bigint,
    p_idempotency_retention_seconds bigint,
    p_max_outstanding_tasks bigint,
    p_starvation_timeout_seconds bigint
)
RETURNS SETOF pgtask.queues
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    INSERT INTO pgtask.queues (
        name,
        terminal_retention_seconds,
        idempotency_retention_seconds,
        max_outstanding_tasks,
        starvation_timeout_seconds
    )
    VALUES (
        p_name,
        p_terminal_retention_seconds,
        p_idempotency_retention_seconds,
        p_max_outstanding_tasks,
        p_starvation_timeout_seconds
    )
    ON CONFLICT (name) DO UPDATE
    SET terminal_retention_seconds = EXCLUDED.terminal_retention_seconds,
        idempotency_retention_seconds = EXCLUDED.idempotency_retention_seconds,
        max_outstanding_tasks = EXCLUDED.max_outstanding_tasks,
        starvation_timeout_seconds = EXCLUDED.starvation_timeout_seconds,
        updated_at = statement_timestamp()
    RETURNING queues.*;
$$;

CREATE OR REPLACE VIEW pgtask.queue_overview WITH (security_barrier = true) AS
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

REVOKE ALL ON FUNCTION pgtask.enforce_queue_capacity() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.put_queue(text, bigint, bigint, bigint, bigint) FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_queue_admission;

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
BEGIN
    PERFORM pgtask.configure_grants_before_queue_admission(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.put_queue(text, bigint, bigint, bigint, bigint) TO %s',
        p_administrator
    );
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_queue_admission(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
