CREATE OR REPLACE FUNCTION pgtask.wait_for_signal(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_signal_name text,
    p_signal_occurrence integer,
    p_timeout_milliseconds bigint
)
RETURNS TABLE(status text, checkpoint jsonb)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target_handler_version integer;
    signal_value jsonb;
    checkpoint_value jsonb;
    timeout_at timestamptz;
BEGIN
    IF p_timeout_milliseconds IS NOT NULL AND p_timeout_milliseconds < 0 THEN
        RAISE EXCEPTION 'signal timeout must not be negative' USING ERRCODE = '22023';
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

    SELECT value
    INTO signal_value
    FROM pgtask.signals
    WHERE task_id = p_task_id
        AND signal_name = p_signal_name
        AND occurrence = p_signal_occurrence;

    IF FOUND THEN
        INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
        VALUES (
            p_task_id,
            target_handler_version,
            p_step_name,
            p_occurrence,
            jsonb_build_object('outcome', 'signal', 'value', signal_value)
        )
        ON CONFLICT (task_id, handler_version, step_name, occurrence)
        DO UPDATE SET value = checkpoints.value
        RETURNING value INTO checkpoint_value;
        RETURN QUERY SELECT 'ready'::text, checkpoint_value;
        RETURN;
    END IF;

    timeout_at = CASE
        WHEN p_timeout_milliseconds IS NULL THEN NULL
        ELSE statement_timestamp() + (p_timeout_milliseconds * interval '1 millisecond')
    END;

    INSERT INTO pgtask.waits (
        task_id, handler_version, step_name, occurrence, signal_name, signal_occurrence, timeout_at
    )
    VALUES (
        p_task_id, target_handler_version, p_step_name, p_occurrence, p_signal_name, p_signal_occurrence, timeout_at
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

    PERFORM pg_notify('pgtask_wait', 'changed');
    RETURN QUERY SELECT 'waiting'::text, NULL::jsonb;
END;
$$;
