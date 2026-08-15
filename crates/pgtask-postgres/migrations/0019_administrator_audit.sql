CREATE TABLE pgtask.administrator_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor text NOT NULL CHECK (actor <> '' AND octet_length(actor) <= 255),
    action text NOT NULL CHECK (action IN ('task.cancel', 'task.retry', 'schedule.pause', 'schedule.resume')),
    task_id uuid REFERENCES pgtask.tasks (id) ON DELETE SET NULL,
    schedule_id uuid REFERENCES pgtask.schedules (id) ON DELETE SET NULL,
    occurred_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK ((task_id IS NOT NULL)::integer + (schedule_id IS NOT NULL)::integer = 1)
);

CREATE INDEX administrator_audit_task_idx
    ON pgtask.administrator_audit (task_id, occurred_at DESC)
    WHERE task_id IS NOT NULL;

CREATE INDEX administrator_audit_schedule_idx
    ON pgtask.administrator_audit (schedule_id, occurred_at DESC)
    WHERE schedule_id IS NOT NULL;

CREATE VIEW pgtask.administrator_audit_view AS
SELECT id, actor, action, task_id, schedule_id, occurred_at
FROM pgtask.administrator_audit;

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

REVOKE ALL ON TABLE pgtask.administrator_audit FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.administrator_audit_view FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.admin_cancel_task(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.admin_retry_task(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.admin_set_schedule_paused(uuid, boolean, text) FROM PUBLIC;

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
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.spawn_task(uuid, integer, uuid, text, integer, text, jsonb, text, integer, timestamptz, smallint, integer, jsonb) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid) TO %s', p_worker);

    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.wait_view, pgtask.result_wait_view, pgtask.administrator_audit_view TO %s', p_observer);
    EXECUTE format('GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.wait_view, pgtask.result_wait_view, pgtask.administrator_audit_view TO %s', p_administrator);
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
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.admin_cancel_task(uuid, text) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.admin_retry_task(uuid, text) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.admin_set_schedule_paused(uuid, boolean, text) TO %s', p_administrator);
END;
$$;
