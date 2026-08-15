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
    outcome text CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
    CHECK (task_id <> result_task_id),
    PRIMARY KEY (task_id, handler_version, step_name, occurrence)
);

CREATE UNIQUE INDEX result_waits_active_task_idx
    ON pgtask.result_waits (task_id)
    WHERE resolved_at IS NULL;

CREATE INDEX result_waits_target_idx
    ON pgtask.result_waits (result_task_id, task_id)
    WHERE resolved_at IS NULL;

CREATE FUNCTION pgtask.wait_for_result(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_result_task_id uuid
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
    WHERE id = p_result_task_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'result task does not exist' USING ERRCODE = '22023';
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

    INSERT INTO pgtask.result_waits (task_id, handler_version, step_name, occurrence, result_task_id)
    VALUES (p_task_id, target_handler_version, p_step_name, p_occurrence, p_result_task_id);

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

    RETURN QUERY SELECT 'waiting'::text, NULL::jsonb;
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

CREATE TRIGGER tasks_resolve_result
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
WHEN (
    OLD.state NOT IN ('succeeded', 'failed', 'cancelled')
    AND NEW.state IN ('succeeded', 'failed', 'cancelled')
)
EXECUTE FUNCTION pgtask.resolve_task_result();

CREATE VIEW pgtask.result_wait_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.result_waits;

REVOKE ALL ON TABLE pgtask.result_waits FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.result_wait_view FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.task_result(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.resolve_task_result() FROM PUBLIC;

CREATE OR REPLACE FUNCTION pgtask.configure_grants(
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
BEGIN
    FOREACH target IN ARRAY ARRAY[p_owner, p_producer, p_worker, p_observer, p_administrator]
    LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA pgtask TO %s', target);
    END LOOP;

    EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pgtask TO %s', p_owner);
    EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtask TO %s', p_owner);

    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.enqueue(text, jsonb, text, integer, timestamptz, smallint, integer, text, jsonb) TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.enqueue_many(jsonb) TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.emit_signal(uuid, text, integer, jsonb) TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.task_result(uuid) TO %s', p_producer);

    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.claim(text, uuid, text[], integer[], integer, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.renew_leases(uuid[], integer[], uuid[], bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.complete_task(uuid, integer, uuid, jsonb) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.fail_task(uuid, integer, uuid, jsonb, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.recover_expired(text, integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.register_worker(uuid, text, text, text[], integer[], bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.commit_checkpoint(uuid, integer, uuid, text, integer, jsonb) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_task(uuid) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.claim_due_schedules(integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.materialize_schedule(uuid, timestamptz, timestamptz[], timestamptz) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.next_schedule_delay_milliseconds() TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.next_task_delay_milliseconds(text, text[], integer[]) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.suspend_task(uuid, integer, uuid, text, integer, timestamptz, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.wait_for_signal(uuid, integer, uuid, text, integer, text, integer, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.recover_wait_timeouts(integer) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.next_wait_delay_milliseconds() TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.task_result(uuid) TO %s', p_worker);

    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.wait_view, pgtask.result_wait_view TO %s', p_observer);
    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.wait_view, pgtask.result_wait_view TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.put_queue(text, bigint) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.set_queue_paused(text, boolean) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.cancel_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.task_result(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.put_schedule(uuid, text, text, bigint, text, text, integer, text, text, integer, jsonb, jsonb, smallint, integer, timestamptz) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_schedule(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.set_schedule_paused(uuid, boolean) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_schedule(uuid) TO %s', p_administrator);
END;
$$;
