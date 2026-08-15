CREATE OR REPLACE FUNCTION pgtask.enqueue_many(p_tasks jsonb)
RETURNS TABLE(request_index bigint, task_id uuid, created boolean)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_tasks IS NULL OR jsonb_typeof(p_tasks) <> 'array' THEN
        RAISE EXCEPTION 'tasks must be a JSON array' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT items.ordinality - 1, enqueued.task_id, enqueued.created
    FROM jsonb_array_elements(p_tasks) WITH ORDINALITY AS items(task, ordinality)
    CROSS JOIN LATERAL pgtask.enqueue(
        p_task_name => items.task->>'task_name',
        p_payload => items.task->'payload',
        p_queue_name => COALESCE(items.task->>'queue_name', 'default'),
        p_handler_version => COALESCE((items.task->>'handler_version')::integer, 1),
        p_run_at => (items.task->>'run_at')::timestamptz,
        p_priority => COALESCE((items.task->>'priority')::smallint, 0::smallint),
        p_max_attempts => COALESCE((items.task->>'max_attempts')::integer, 5),
        p_idempotency_key => items.task->>'idempotency_key',
        p_headers => COALESCE(items.task->'headers', '{}'::jsonb)
    ) AS enqueued
    ORDER BY items.ordinality;
END;
$$;
