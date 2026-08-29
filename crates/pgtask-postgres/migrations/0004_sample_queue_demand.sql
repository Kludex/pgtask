ALTER TABLE pgtask.queues
    ADD COLUMN demand_sampled_at timestamptz,
    ADD COLUMN demand_live_workers bigint NOT NULL DEFAULT 0,
    ADD COLUMN demand_routable_tasks bigint NOT NULL DEFAULT 0,
    ADD COLUMN demand_unroutable_tasks bigint NOT NULL DEFAULT 0;

CREATE INDEX workers_queue_expiry_idx ON pgtask.workers(queue_name, expires_at);

CREATE OR REPLACE FUNCTION pgtask.storage_protocol_version()
RETURNS integer
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 2;
$$;

CREATE OR REPLACE FUNCTION pgtask.storage_protocol_range()
RETURNS TABLE(minimum integer, maximum integer)
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 1, 2;
$$;

CREATE FUNCTION pgtask.sample_queue_demand(
    p_queue_name text,
    p_sample_interval_milliseconds bigint
)
RETURNS TABLE(
    sampled boolean,
    live_workers bigint,
    routable_tasks bigint,
    unroutable_tasks bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
SET lock_timeout = '100ms'
AS $$
DECLARE
    sampled_at timestamptz;
BEGIN
    IF p_sample_interval_milliseconds <= 0 THEN
        RAISE EXCEPTION 'sample interval must be positive' USING ERRCODE = '22023';
    END IF;

    sampled := false;
    SELECT
        queues.demand_sampled_at,
        queues.demand_live_workers,
        queues.demand_routable_tasks,
        queues.demand_unroutable_tasks
    INTO sampled_at, live_workers, routable_tasks, unroutable_tasks
    FROM pgtask.queues
    WHERE queues.name = p_queue_name;
    IF NOT FOUND THEN
        live_workers := 0;
        routable_tasks := 0;
        unroutable_tasks := 0;
        RETURN NEXT;
        RETURN;
    END IF;

    IF NOT pg_try_advisory_xact_lock(hashtextextended('pgtask.demand.' || p_queue_name, 0)) THEN
        RETURN NEXT;
        RETURN;
    END IF;

    SELECT
        queues.demand_sampled_at,
        queues.demand_live_workers,
        queues.demand_routable_tasks,
        queues.demand_unroutable_tasks
    INTO sampled_at, live_workers, routable_tasks, unroutable_tasks
    FROM pgtask.queues
    WHERE queues.name = p_queue_name;
    IF sampled_at IS NOT NULL
        AND sampled_at > statement_timestamp()
            - (
                (p_sample_interval_milliseconds - p_sample_interval_milliseconds / 10)
                * interval '1 millisecond'
            )
    THEN
        RETURN NEXT;
        RETURN;
    END IF;

    SELECT count(*)
    INTO live_workers
    FROM pgtask.workers
    WHERE workers.queue_name = p_queue_name
        AND workers.expires_at > statement_timestamp();

    WITH demand AS MATERIALIZED (
        SELECT EXISTS (
                SELECT 1
                FROM pgtask.workers
                JOIN pgtask.worker_capabilities ON worker_capabilities.worker_id = workers.id
                WHERE workers.queue_name = tasks.queue_name
                    AND workers.draining = false
                    AND workers.expires_at > statement_timestamp()
                    AND worker_capabilities.task_name = tasks.task_name
                    AND worker_capabilities.handler_version = tasks.handler_version
            ) AS routable
        FROM pgtask.tasks
        JOIN pgtask.queues ON queues.name = tasks.queue_name
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
            AND queues.paused_at IS NULL
    )
    SELECT
        count(*) FILTER (WHERE demand.routable),
        count(*) - count(*) FILTER (WHERE demand.routable)
    INTO routable_tasks, unroutable_tasks
    FROM demand;

    BEGIN
        UPDATE pgtask.queues
        SET demand_sampled_at = statement_timestamp(),
            demand_live_workers = live_workers,
            demand_routable_tasks = routable_tasks,
            demand_unroutable_tasks = unroutable_tasks
        WHERE name = p_queue_name;
    EXCEPTION
        WHEN lock_not_available THEN
            SELECT
                queues.demand_live_workers,
                queues.demand_routable_tasks,
                queues.demand_unroutable_tasks
            INTO live_workers, routable_tasks, unroutable_tasks
            FROM pgtask.queues
            WHERE queues.name = p_queue_name;
            RETURN NEXT;
            RETURN;
    END;
    sampled := true;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION pgtask.sample_queue_demand(text, bigint) FROM PUBLIC;

DO $$
DECLARE
    definition text;
    rewritten text;
    target regrole;
BEGIN
    FOR target IN
        SELECT privileges.grantee::regrole
        FROM pg_proc
        CROSS JOIN LATERAL aclexplode(pg_proc.proacl) AS privileges
        WHERE pg_proc.oid = 'pgtask.heartbeat_worker(uuid, bigint, boolean)'::regprocedure
            AND privileges.privilege_type = 'EXECUTE'
            AND privileges.grantee <> 0
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.sample_queue_demand(text, bigint) TO %s', target);
    END LOOP;

    definition := pg_get_functiondef('pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)'::regprocedure);
    rewritten := replace(
        definition,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s'', p_worker);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s'', p_worker);\n    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.sample_queue_demand(text, bigint) TO %s'', p_worker);\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not extend pgtask.configure_grants';
    END IF;
    EXECUTE rewritten;
END;
$$;
