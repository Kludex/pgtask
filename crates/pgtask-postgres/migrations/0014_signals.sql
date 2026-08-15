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
        ceil(EXTRACT(epoch FROM (min(timeout_at) - statement_timestamp())) * 1000)::bigint
    )
    FROM pgtask.waits
    WHERE resolved_at IS NULL AND timeout_at IS NOT NULL;
$$;

CREATE VIEW pgtask.signal_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.signals;

CREATE VIEW pgtask.wait_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.waits;

REVOKE ALL ON TABLE pgtask.signals FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.waits FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.signal_view FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.wait_view FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.emit_signal(uuid, text, integer, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.wait_for_signal(uuid, integer, uuid, text, integer, text, integer, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.recover_wait_timeouts(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.next_wait_delay_milliseconds() FROM PUBLIC;

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

    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.wait_view TO %s', p_observer);
    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.wait_view TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.put_queue(text, bigint) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.set_queue_paused(text, boolean) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.cancel_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.put_schedule(uuid, text, text, bigint, text, text, integer, text, text, integer, jsonb, jsonb, smallint, integer, timestamptz) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_schedule(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.set_schedule_paused(uuid, boolean) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_schedule(uuid) TO %s', p_administrator);
END;
$$;
