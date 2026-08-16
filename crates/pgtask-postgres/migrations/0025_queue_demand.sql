CREATE INDEX tasks_pending_capability_idx
ON pgtask.tasks (queue_name, task_name, handler_version, priority DESC, run_at, id)
WHERE state = 'pending';

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
    ) AS unroutable_count
FROM pgtask.queues
LEFT JOIN pgtask.tasks ON tasks.queue_name = queues.name
GROUP BY queues.name;

CREATE FUNCTION pgtask.queue_demand(
    p_queue_name text,
    p_task_names text[],
    p_handler_versions integer[]
)
RETURNS TABLE(ready_tasks bigint, capable_tasks bigint, unroutable_tasks bigint)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF cardinality(p_task_names) = 0 OR cardinality(p_task_names) <> cardinality(p_handler_versions) THEN
        RAISE EXCEPTION 'worker capabilities must be nonempty and aligned' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT
        count(tasks.id),
        count(tasks.id) FILTER (
            WHERE EXISTS (
                SELECT 1
                FROM unnest(p_task_names, p_handler_versions) AS capability(task_name, handler_version)
                WHERE capability.task_name = tasks.task_name
                    AND capability.handler_version = tasks.handler_version
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
    FROM pgtask.tasks
    JOIN pgtask.queues ON queues.name = tasks.queue_name
    WHERE tasks.queue_name = p_queue_name
        AND tasks.state = 'pending'
        AND tasks.run_at <= statement_timestamp()
        AND queues.paused_at IS NULL;
END;
$$;

REVOKE ALL ON FUNCTION pgtask.queue_demand(text, text[], integer[]) FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_queue_demand;

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
    PERFORM pgtask.configure_grants_before_queue_demand(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.queue_demand(text, text[], integer[]) TO %s', p_worker);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_queue_demand(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
