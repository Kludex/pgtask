CREATE FUNCTION pgtask.register_worker(
    p_worker_id uuid,
    p_queue_name text,
    p_version text,
    p_task_names text[],
    p_handler_versions integer[],
    p_ttl_milliseconds bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF cardinality(p_task_names) = 0 OR cardinality(p_task_names) <> cardinality(p_handler_versions) THEN
        RAISE EXCEPTION 'worker capabilities must be nonempty and aligned' USING ERRCODE = '22023';
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

CREATE FUNCTION pgtask.get_task(p_task_id uuid)
RETURNS SETOF pgtask.tasks
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT * FROM pgtask.tasks WHERE id = p_task_id;
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

CREATE FUNCTION pgtask.put_queue(p_name text, p_terminal_retention_seconds bigint)
RETURNS SETOF pgtask.queues
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    INSERT INTO pgtask.queues (name, terminal_retention_seconds)
    VALUES (p_name, p_terminal_retention_seconds)
    ON CONFLICT (name) DO UPDATE
    SET terminal_retention_seconds = EXCLUDED.terminal_retention_seconds,
        updated_at = statement_timestamp()
    RETURNING queues.*;
$$;

CREATE FUNCTION pgtask.set_queue_paused(p_name text, p_paused boolean)
RETURNS SETOF pgtask.queues
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    UPDATE pgtask.queues
    SET paused_at = CASE WHEN p_paused THEN COALESCE(paused_at, statement_timestamp()) ELSE NULL END,
        updated_at = statement_timestamp()
    WHERE name = p_name
    RETURNING queues.*;
$$;

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

REVOKE ALL ON FUNCTION pgtask.register_worker(uuid, text, text, text[], integer[], bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.commit_checkpoint(uuid, integer, uuid, text, integer, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.get_task(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.put_queue(text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.set_queue_paused(text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.cancel_task(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.delete_expired_terminal(text, integer) FROM PUBLIC;
