ALTER TABLE pgtask.attempts
DROP CONSTRAINT attempts_state_check;

ALTER TABLE pgtask.attempts
ADD CONSTRAINT attempts_state_check CHECK (
    state IN ('running', 'succeeded', 'failed', 'lost', 'cancelled', 'suspended')
);

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
REVOKE ALL ON FUNCTION pgtask.suspend_task(uuid, integer, uuid, text, integer, timestamptz, bigint) FROM PUBLIC;

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

    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view TO %s', p_observer);
    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view TO %s', p_administrator);
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
