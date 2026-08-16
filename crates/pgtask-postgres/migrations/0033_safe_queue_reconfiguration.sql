CREATE OR REPLACE FUNCTION pgtask.put_queue(
    p_name text,
    p_terminal_retention_seconds bigint,
    p_idempotency_retention_seconds bigint,
    p_max_outstanding_tasks bigint,
    p_starvation_timeout_seconds bigint
)
RETURNS SETOF pgtask.queues
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    current_capacity bigint;
    current_maximum bigint;
    initial_capacity bigint := 0;
    queue_exists boolean;
BEGIN
    SELECT queues.max_outstanding_tasks, queues.capacity_outstanding_tasks
    INTO current_maximum, current_capacity
    FROM pgtask.queues
    WHERE queues.name = p_name
    FOR UPDATE;
    queue_exists := FOUND;

    IF p_max_outstanding_tasks IS NULL THEN
        initial_capacity := 0;
    ELSIF queue_exists AND current_maximum IS NOT NULL THEN
        initial_capacity := current_capacity;
    ELSE
        SELECT count(*)
        INTO initial_capacity
        FROM pgtask.tasks
        WHERE tasks.queue_name = p_name
            AND tasks.state IN ('pending', 'running', 'waiting');
    END IF;

    RETURN QUERY
    INSERT INTO pgtask.queues (
        name,
        terminal_retention_seconds,
        idempotency_retention_seconds,
        max_outstanding_tasks,
        starvation_timeout_seconds,
        capacity_outstanding_tasks
    )
    VALUES (
        p_name,
        p_terminal_retention_seconds,
        p_idempotency_retention_seconds,
        p_max_outstanding_tasks,
        p_starvation_timeout_seconds,
        initial_capacity
    )
    ON CONFLICT (name) DO UPDATE
    SET terminal_retention_seconds = EXCLUDED.terminal_retention_seconds,
        idempotency_retention_seconds = EXCLUDED.idempotency_retention_seconds,
        max_outstanding_tasks = EXCLUDED.max_outstanding_tasks,
        starvation_timeout_seconds = EXCLUDED.starvation_timeout_seconds,
        capacity_outstanding_tasks = EXCLUDED.capacity_outstanding_tasks,
        updated_at = statement_timestamp()
    RETURNING queues.*;
END;
$$;
