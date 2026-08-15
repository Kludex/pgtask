CREATE FUNCTION pgtask.spawn_task(
    p_parent_task_id uuid,
    p_parent_attempt integer,
    p_parent_lease_token uuid,
    p_step_name text,
    p_occurrence integer,
    p_task_name text,
    p_payload jsonb,
    p_queue_name text,
    p_handler_version integer,
    p_run_at timestamptz,
    p_priority smallint,
    p_max_attempts integer,
    p_headers jsonb
)
RETURNS TABLE(task_id uuid, created boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    parent_handler_version integer;
    checkpoint_value jsonb;
    child_task_id uuid;
    child_created boolean;
BEGIN
    SELECT handler_version
    INTO parent_handler_version
    FROM pgtask.tasks
    WHERE id = p_parent_task_id
        AND state = 'running'
        AND attempt = p_parent_attempt
        AND lease_token = p_parent_lease_token
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT value
    INTO checkpoint_value
    FROM pgtask.checkpoints
    WHERE task_id = p_parent_task_id
        AND handler_version = parent_handler_version
        AND step_name = p_step_name
        AND occurrence = p_occurrence;

    IF FOUND THEN
        RETURN QUERY SELECT (checkpoint_value->>'task_id')::uuid, false;
        RETURN;
    END IF;

    SELECT enqueued.task_id, enqueued.created
    INTO child_task_id, child_created
    FROM pgtask.enqueue(
        p_task_name => p_task_name,
        p_payload => p_payload,
        p_queue_name => p_queue_name,
        p_handler_version => p_handler_version,
        p_run_at => p_run_at,
        p_priority => p_priority,
        p_max_attempts => p_max_attempts,
        p_idempotency_key => format(
            'pgtask:spawn:%s:%s:%s:%s',
            p_parent_task_id,
            parent_handler_version,
            p_step_name,
            p_occurrence
        ),
        p_headers => p_headers
    ) AS enqueued;

    INSERT INTO pgtask.checkpoints (task_id, handler_version, step_name, occurrence, value)
    VALUES (
        p_parent_task_id,
        parent_handler_version,
        p_step_name,
        p_occurrence,
        jsonb_build_object('task_id', child_task_id)
    );
    RETURN QUERY SELECT child_task_id, child_created;
END;
$$;

REVOKE ALL ON FUNCTION pgtask.spawn_task(
    uuid, integer, uuid, text, integer, text, jsonb, text, integer, timestamptz, smallint, integer, jsonb
) FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_child_tasks;

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
    PERFORM pgtask.configure_grants_before_child_tasks(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.spawn_task(uuid, integer, uuid, text, integer, text, jsonb, text, integer, timestamptz, smallint, integer, jsonb) TO %s',
        p_worker
    );
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_child_tasks(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
