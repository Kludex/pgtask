ALTER TABLE pgtask.tasks
ADD COLUMN parent_task_id uuid REFERENCES pgtask.tasks (id) ON DELETE SET NULL;

ALTER TABLE pgtask.tasks
ADD CONSTRAINT tasks_parent_check CHECK (parent_task_id IS NULL OR parent_task_id <> id);

CREATE INDEX tasks_parent_idx ON pgtask.tasks (parent_task_id, id) WHERE parent_task_id IS NOT NULL;

CREATE OR REPLACE VIEW pgtask.task_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.tasks;

ALTER TABLE pgtask.result_waits
ADD COLUMN timeout_at timestamptz;

ALTER TABLE pgtask.result_waits DROP CONSTRAINT result_waits_outcome_check;
ALTER TABLE pgtask.result_waits
ADD CONSTRAINT result_waits_outcome_check CHECK (outcome IN ('succeeded', 'failed', 'cancelled', 'timeout'));

CREATE INDEX result_waits_timeout_idx
ON pgtask.result_waits (timeout_at, task_id)
WHERE resolved_at IS NULL AND timeout_at IS NOT NULL;

CREATE OR REPLACE VIEW pgtask.result_wait_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.result_waits;

CREATE OR REPLACE FUNCTION pgtask.spawn_task(
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

CREATE OR REPLACE FUNCTION pgtask.wait_for_result(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_result_task_id uuid
)
RETURNS TABLE(status text, checkpoint jsonb)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT * FROM pgtask.wait_for_result(
        p_task_id,
        p_attempt,
        p_lease_token,
        p_step_name,
        p_occurrence,
        p_result_task_id,
        NULL
    );
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

CREATE OR REPLACE FUNCTION pgtask.next_wait_delay_milliseconds()
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

CREATE TRIGGER tasks_cancel_owned_children
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
WHEN (
    OLD.state NOT IN ('succeeded', 'failed', 'cancelled')
    AND NEW.state IN ('succeeded', 'failed', 'cancelled')
)
EXECUTE FUNCTION pgtask.cancel_owned_children();

CREATE OR REPLACE FUNCTION pgtask.delete_expired_terminal(p_queue_name text, p_limit integer)
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

REVOKE ALL ON FUNCTION pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.recover_result_wait_timeouts(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.cancel_owned_children() FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_workflow_ownership;

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
    PERFORM pgtask.configure_grants_before_workflow_ownership(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid, bigint) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.recover_result_wait_timeouts(integer) TO %s', p_worker);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_workflow_ownership(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
