ALTER TABLE pgtask.queues
    ADD COLUMN demand_sampled_at timestamptz,
    ADD COLUMN demand_ready_tasks bigint NOT NULL DEFAULT 0,
    ADD COLUMN demand_unroutable_tasks bigint NOT NULL DEFAULT 0;

CREATE FUNCTION pgtask.heartbeat_worker_with_sampling(
    p_worker_id uuid,
    p_ttl_milliseconds bigint,
    p_draining boolean,
    p_sample_interval_milliseconds bigint
)
RETURNS TABLE(
    updated boolean,
    sampled boolean,
    live_workers bigint,
    ready_tasks bigint,
    unroutable_tasks bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target_queue text;
BEGIN
    IF p_sample_interval_milliseconds <= 0 THEN
        RAISE EXCEPTION 'sample interval must be positive' USING ERRCODE = '22023';
    END IF;

    SELECT pgtask.heartbeat_worker(p_worker_id, p_ttl_milliseconds, p_draining) INTO updated;
    sampled := false;
    live_workers := 0;
    ready_tasks := 0;
    unroutable_tasks := 0;
    IF NOT updated THEN
        RETURN NEXT;
        RETURN;
    END IF;

    SELECT workers.queue_name INTO target_queue
    FROM pgtask.workers
    WHERE workers.id = p_worker_id;
    SELECT pgtask.live_worker_count(target_queue) INTO live_workers;

    IF NOT p_draining
        AND EXISTS (
            SELECT 1
            FROM pgtask.queues
            WHERE name = target_queue
                AND (
                    demand_sampled_at IS NULL
                    OR demand_sampled_at <= statement_timestamp()
                        - (
                            (p_sample_interval_milliseconds - p_sample_interval_milliseconds / 10)
                            * interval '1 millisecond'
                        )
                )
        )
        AND pg_try_advisory_xact_lock(hashtextextended('pgtask.demand.' || target_queue, 0))
    THEN
        SELECT
            count(tasks.id) FILTER (
                WHERE EXISTS (
                    SELECT 1
                    FROM pgtask.workers
                    JOIN pgtask.worker_capabilities ON worker_capabilities.worker_id = workers.id
                    WHERE workers.queue_name = tasks.queue_name
                        AND workers.draining = false
                        AND workers.expires_at > statement_timestamp()
                        AND worker_capabilities.task_name = tasks.task_name
                        AND worker_capabilities.handler_version = tasks.handler_version
                )
            ),
            count(tasks.id) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pgtask.workers
                    JOIN pgtask.worker_capabilities ON worker_capabilities.worker_id = workers.id
                    WHERE workers.queue_name = tasks.queue_name
                        AND workers.draining = false
                        AND workers.expires_at > statement_timestamp()
                        AND worker_capabilities.task_name = tasks.task_name
                        AND worker_capabilities.handler_version = tasks.handler_version
                )
            )
        INTO ready_tasks, unroutable_tasks
        FROM pgtask.tasks
        JOIN pgtask.queues ON queues.name = tasks.queue_name
        WHERE tasks.queue_name = target_queue
            AND tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
            AND queues.paused_at IS NULL;

        UPDATE pgtask.queues
        SET demand_sampled_at = statement_timestamp(),
            demand_ready_tasks = ready_tasks,
            demand_unroutable_tasks = unroutable_tasks
        WHERE name = target_queue;
        sampled := true;
    ELSE
        SELECT demand_ready_tasks, demand_unroutable_tasks
        INTO ready_tasks, unroutable_tasks
        FROM pgtask.queues
        WHERE name = target_queue;
    END IF;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint) FROM PUBLIC;

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
    LOOP
        EXECUTE format(
            'GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint) TO %s',
            target
        );
    END LOOP;

    definition := pg_get_functiondef('pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)'::regprocedure);
    rewritten := replace(
        definition,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s'', p_worker);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s'', p_worker);\n    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint) TO %s'', p_worker);\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not extend pgtask.configure_grants';
    END IF;
    EXECUTE rewritten;
END;
$$;
