-- Serialize result registration with child completion and ancestor-first lease renewal.

CREATE OR REPLACE FUNCTION pgtask.wait_for_result(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_result_task_id uuid,
    p_timeout_milliseconds bigint
)
RETURNS TABLE(status text, checkpoint jsonb)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target_handler_version integer;
    result_state text;
    result_value jsonb;
    result_error jsonb;
    checkpoint_value jsonb;
BEGIN
    IF p_task_id = p_result_task_id THEN
        RAISE EXCEPTION 'a task cannot wait for its own result' USING ERRCODE = '22023';
    END IF;
    IF p_timeout_milliseconds IS NOT NULL AND p_timeout_milliseconds <= 0 THEN
        RAISE EXCEPTION 'result wait timeout must be positive' USING ERRCODE = '22023';
    END IF;

    SELECT handler_version
    INTO target_handler_version
    FROM pgtask.tasks
    WHERE id = p_task_id
        AND state = 'running'
        AND attempt = p_attempt
        AND lease_token = p_lease_token
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT value
    INTO checkpoint_value
    FROM pgtask.checkpoints
    WHERE task_id = p_task_id
        AND handler_version = target_handler_version
        AND step_name = p_step_name
        AND occurrence = p_occurrence;

    IF FOUND THEN
        RETURN QUERY SELECT 'ready'::text, checkpoint_value;
        RETURN;
    END IF;

    SELECT state, result, error
    INTO result_state, result_value, result_error
    FROM pgtask.tasks
    WHERE id = p_result_task_id AND parent_task_id = p_task_id
    FOR NO KEY UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'result task is not a direct child of this task' USING ERRCODE = '22023';
    END IF;

    IF result_state IN ('succeeded', 'failed', 'cancelled') THEN
        checkpoint_value = jsonb_build_object(
            'state', result_state,
            'result', result_value,
            'error', result_error
        );
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        VALUES (p_task_id, target_handler_version, p_step_name, p_occurrence, checkpoint_value)
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value
        RETURNING value INTO checkpoint_value;
        RETURN QUERY SELECT 'ready'::text, checkpoint_value;
        RETURN;
    END IF;

    INSERT INTO pgtask.result_waits (
        task_id, handler_version, step_name, occurrence, result_task_id, timeout_at
    )
    VALUES (
        p_task_id,
        target_handler_version,
        p_step_name,
        p_occurrence,
        p_result_task_id,
        CASE
            WHEN p_timeout_milliseconds IS NULL THEN NULL
            ELSE statement_timestamp() + (p_timeout_milliseconds * interval '1 millisecond')
        END
    );

    UPDATE pgtask.tasks
    SET state = 'waiting',
        lease_token = NULL,
        lease_owner = NULL,
        lease_expires_at = NULL,
        updated_at = statement_timestamp()
    WHERE id = p_task_id;

    UPDATE pgtask.attempts
    SET state = 'suspended', finished_at = statement_timestamp()
    WHERE task_id = p_task_id AND attempt = p_attempt;

    IF p_timeout_milliseconds IS NOT NULL THEN
        PERFORM pg_notify('pgtask_wait', 'changed');
    END IF;
    RETURN QUERY SELECT 'waiting'::text, NULL::jsonb;
END;
$$;


CREATE OR REPLACE FUNCTION pgtask.renew_leases(
    p_task_ids uuid[],
    p_attempts integer[],
    p_lease_tokens uuid[],
    p_lease_milliseconds bigint
)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH RECURSIVE requested AS MATERIALIZED (
        SELECT *
        FROM unnest(p_task_ids, p_attempts, p_lease_tokens) AS leases(task_id, attempt, lease_token)
    ),
    ancestry AS (
        SELECT tasks.id, tasks.parent_task_id, 0 AS depth
        FROM pgtask.tasks
        WHERE tasks.id = ANY(p_task_ids)
        UNION ALL
        SELECT ancestry.id, parents.parent_task_id, ancestry.depth + 1
        FROM ancestry
        JOIN pgtask.tasks AS parents ON parents.id = ancestry.parent_task_id
    ),
    depths AS (
        SELECT id, max(depth) AS depth
        FROM ancestry
        GROUP BY id
    ),
    locked AS MATERIALIZED (
        SELECT tasks.id
        FROM pgtask.tasks
        JOIN depths ON depths.id = tasks.id
        ORDER BY depths.depth, tasks.id
        FOR NO KEY UPDATE OF tasks
    )
    UPDATE pgtask.tasks
    SET lease_expires_at = statement_timestamp() + (p_lease_milliseconds * interval '1 millisecond'),
        updated_at = statement_timestamp()
    FROM requested
    JOIN locked ON locked.id = requested.task_id
    WHERE tasks.id = requested.task_id
        AND tasks.state = 'running'
        AND tasks.attempt = requested.attempt
        AND tasks.lease_token = requested.lease_token
        AND tasks.cancel_requested_at IS NULL
    RETURNING tasks.id;
$$;
