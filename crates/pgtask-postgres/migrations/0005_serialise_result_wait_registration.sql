-- Close the lost wake-up window in wait_for_result.
--
-- The waiter read the child's state without a lock, then registered the wait and
-- parked the parent. Under READ COMMITTED that wait row stays invisible until it
-- commits, and the waker -- complete_task on the CHILD -- takes no lock on the
-- parent row the waiter holds. So both could miss each other, leaving the parent
-- in `waiting` with its wake-up already spent and no sweep able to recover it.
--
-- Taking FOR NO KEY UPDATE on the child makes complete_task's UPDATE of that
-- child wait for the wait row to commit. An UPDATE that leaves key columns alone
-- acquires FOR NO KEY UPDATE, and that mode self-conflicts, so this is enough.
-- Lock order stays parent-then-child, matching cancel_owned_children, so it adds
-- no deadlock cycle.
--
-- This is what already protects wait_for_signal, though there only by accident:
-- emit_signal's insert into pgtask.signals needs a key-share lock on the task row
-- for its foreign key, and that conflicts with the waiter's FOR UPDATE.

CREATE OR REPLACE FUNCTION pgtask.wait_for_result(p_task_id uuid, p_attempt integer, p_lease_token uuid, p_step_name text, p_occurrence integer, p_result_task_id uuid, p_timeout_milliseconds bigint)
 RETURNS TABLE(status text, checkpoint jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'pgtask'
AS $function$
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
$function$
