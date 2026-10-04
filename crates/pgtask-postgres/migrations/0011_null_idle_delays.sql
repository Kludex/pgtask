-- The next_*_delay_milliseconds functions report how long a worker may sleep before the next
-- deadline, and NULL when there is no deadline at all. Wrapping min() in GREATEST(0, ...) broke the
-- NULL case: GREATEST ignores NULL arguments, so an empty set returned 0 and idle workers polled in
-- a tight loop. Filtering the aggregate with HAVING keeps the 0 floor for overdue deadlines and
-- returns no row, which a scalar SQL function reports as NULL, when nothing is due.
--
-- CREATE OR REPLACE keeps each function's owner and grants.

CREATE OR REPLACE FUNCTION pgtask.next_schedule_delay_milliseconds()
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
    WHERE paused_at IS NULL
    HAVING min(next_run_at) IS NOT NULL;
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
    ) AS deadlines
    HAVING min(deadlines.timeout_at) IS NOT NULL;
$$;

CREATE OR REPLACE FUNCTION pgtask.next_task_delay_milliseconds(
    p_queue_name text,
    p_task_names text[],
    p_handler_versions integer[]
)
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT GREATEST(
        0,
        ceil(EXTRACT(epoch FROM (min(tasks.run_at) - statement_timestamp())) * 1000)::bigint
    )
    FROM pgtask.tasks
    WHERE tasks.queue_name = p_queue_name
        AND tasks.state = 'pending'
        AND COALESCE(tasks.failed_attempts, (SELECT count(*)::integer FROM pgtask.attempts AS history WHERE history.task_id = tasks.id AND history.state IN ('failed', 'lost'))) < tasks.max_attempts
        AND EXISTS (
            SELECT 1
            FROM pgtask.queues
            WHERE queues.name = tasks.queue_name AND queues.paused_at IS NULL
        )
        AND EXISTS (
            SELECT 1
            FROM unnest(p_task_names, p_handler_versions) AS handlers(task_name, handler_version)
            WHERE handlers.task_name = tasks.task_name
                AND handlers.handler_version = tasks.handler_version
        )
    HAVING min(tasks.run_at) IS NOT NULL;
$$;
