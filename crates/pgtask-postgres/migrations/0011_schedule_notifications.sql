CREATE OR REPLACE FUNCTION pgtask.put_schedule(
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

CREATE OR REPLACE FUNCTION pgtask.set_schedule_paused(p_id uuid, p_paused boolean)
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

CREATE OR REPLACE FUNCTION pgtask.delete_schedule(p_id uuid)
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

REVOKE ALL ON FUNCTION pgtask.next_schedule_delay_milliseconds() FROM PUBLIC;

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
