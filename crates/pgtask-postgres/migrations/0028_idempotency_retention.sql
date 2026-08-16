ALTER TABLE pgtask.queues
ADD COLUMN idempotency_retention_seconds bigint NOT NULL DEFAULT 2592000
CHECK (idempotency_retention_seconds >= 0);

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

INSERT INTO pgtask.idempotency_keys (queue_name, idempotency_key, task_id, created_at, expires_at)
SELECT
    tasks.queue_name,
    tasks.idempotency_key,
    tasks.id,
    tasks.created_at,
    CASE
        WHEN tasks.state IN ('succeeded', 'failed', 'cancelled')
        THEN COALESCE(tasks.completed_at, tasks.updated_at)
            + (queues.idempotency_retention_seconds * interval '1 second')
        ELSE NULL
    END
FROM pgtask.tasks
JOIN pgtask.queues ON queues.name = tasks.queue_name
WHERE tasks.idempotency_key IS NOT NULL;

DROP INDEX pgtask.tasks_idempotency_key_idx;

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

CREATE TRIGGER tasks_manage_idempotency_key
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.manage_idempotency_key();

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

CREATE TRIGGER tasks_preserve_deleted_idempotency_key
AFTER DELETE ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.preserve_deleted_idempotency_key();

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

CREATE FUNCTION pgtask.put_queue(
    p_name text,
    p_terminal_retention_seconds bigint,
    p_idempotency_retention_seconds bigint
)
RETURNS SETOF pgtask.queues
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    INSERT INTO pgtask.queues (name, terminal_retention_seconds, idempotency_retention_seconds)
    VALUES (p_name, p_terminal_retention_seconds, p_idempotency_retention_seconds)
    ON CONFLICT (name) DO UPDATE
    SET terminal_retention_seconds = EXCLUDED.terminal_retention_seconds,
        idempotency_retention_seconds = EXCLUDED.idempotency_retention_seconds,
        updated_at = statement_timestamp()
    RETURNING queues.*;
$$;

CREATE OR REPLACE FUNCTION pgtask.put_queue(p_name text, p_terminal_retention_seconds bigint)
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
    queues.idempotency_retention_seconds
FROM pgtask.queues
LEFT JOIN pgtask.tasks ON tasks.queue_name = queues.name
GROUP BY queues.name;

REVOKE ALL ON TABLE pgtask.idempotency_keys FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.manage_idempotency_key() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.preserve_deleted_idempotency_key() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.delete_expired_idempotency_keys(text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.put_queue(text, bigint, bigint) FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_idempotency_retention;

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
    PERFORM pgtask.configure_grants_before_idempotency_retention(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_idempotency_keys(text, integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.put_queue(text, bigint, bigint) TO %s', p_administrator);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.delete_expired_idempotency_keys(text, integer) TO %s',
        p_administrator
    );
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_idempotency_retention(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
