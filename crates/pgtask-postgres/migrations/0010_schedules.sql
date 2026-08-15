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

ALTER TABLE pgtask.tasks
ADD COLUMN schedule_id uuid,
ADD COLUMN scheduled_for timestamptz;

ALTER TABLE pgtask.tasks
ADD CONSTRAINT tasks_schedule_occurrence_check CHECK (
    (schedule_id IS NULL AND scheduled_for IS NULL)
    OR (schedule_id IS NOT NULL AND scheduled_for IS NOT NULL)
);

CREATE UNIQUE INDEX tasks_schedule_occurrence_idx
    ON pgtask.tasks (schedule_id, scheduled_for)
    WHERE schedule_id IS NOT NULL;

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
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    UPDATE pgtask.schedules
    SET paused_at = CASE WHEN p_paused THEN COALESCE(paused_at, statement_timestamp()) ELSE NULL END,
        updated_at = statement_timestamp()
    WHERE id = p_id
    RETURNING schedules.*;
$$;

CREATE FUNCTION pgtask.delete_schedule(p_id uuid)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH deleted AS (
        DELETE FROM pgtask.schedules WHERE id = p_id RETURNING id
    )
    SELECT EXISTS(SELECT 1 FROM deleted);
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
    materialized bigint;
    target pgtask.schedules%ROWTYPE;
BEGIN
    UPDATE pgtask.schedules
    SET next_run_at = p_next_run_at, updated_at = statement_timestamp()
    WHERE id = p_id AND paused_at IS NULL AND next_run_at = p_expected_next_run_at
    RETURNING * INTO target;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    WITH inserted AS (
        INSERT INTO pgtask.tasks (
            id, queue_name, task_name, handler_version, payload, headers, priority, run_at,
            max_attempts, schedule_id, scheduled_for
        )
        SELECT
            gen_random_uuid(), target.queue_name, target.task_name, target.handler_version,
            target.payload, target.headers, target.priority, occurrence, target.max_attempts,
            target.id, occurrence
        FROM unnest(p_occurrences) AS occurrence
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

CREATE VIEW pgtask.schedule_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.schedules;

CREATE VIEW pgtask.schedule_occurrence_view WITH (security_barrier = true) AS
SELECT schedule_id, scheduled_for, id AS task_id, state, created_at, completed_at
FROM pgtask.tasks
WHERE schedule_id IS NOT NULL;

REVOKE ALL ON TABLE pgtask.schedules FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.schedule_view FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.schedule_occurrence_view FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.put_schedule(uuid, text, text, bigint, text, text, integer, text, text, integer, jsonb, jsonb, smallint, integer, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.get_schedule(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.set_schedule_paused(uuid, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.delete_schedule(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.claim_due_schedules(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.materialize_schedule(uuid, timestamptz, timestamptz[], timestamptz) FROM PUBLIC;

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

    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.enqueue(text, jsonb, text, integer, timestamptz, smallint, integer, text, jsonb) TO %s',
        p_producer
    );
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

    EXECUTE format(
        'GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, '
        'pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, '
        'pgtask.schedule_occurrence_view TO %s',
        p_observer
    );
    EXECUTE format(
        'GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, '
        'pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, '
        'pgtask.schedule_occurrence_view TO %s',
        p_administrator
    );
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
