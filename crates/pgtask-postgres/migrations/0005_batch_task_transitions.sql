CREATE FUNCTION pgtask.complete_tasks(p_completions jsonb)
RETURNS TABLE(request_index bigint, completed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF p_completions IS NULL OR jsonb_typeof(p_completions) <> 'array' THEN
        RAISE EXCEPTION 'completions must be a JSON array' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_completions) AS items(completion)
        GROUP BY completion->>'task_id'
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'completions contain duplicate task IDs' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH requested AS (
        SELECT
            items.ordinality - 1 AS request_index,
            (items.completion->>'task_id')::uuid AS task_id,
            (items.completion->>'attempt')::integer AS attempt,
            (items.completion->>'lease_token')::uuid AS lease_token,
            CASE
                WHEN (items.completion->>'has_result')::boolean THEN items.completion->'result'
                ELSE NULL
            END AS result
        FROM jsonb_array_elements(p_completions) WITH ORDINALITY AS items(completion, ordinality)
    ),
    completed_tasks AS (
        UPDATE pgtask.tasks
        SET state = 'succeeded',
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = statement_timestamp(),
            updated_at = statement_timestamp(),
            result = requested.result,
            error = NULL
        FROM requested
        WHERE tasks.id = requested.task_id
            AND tasks.state = 'running'
            AND tasks.attempt = requested.attempt
            AND tasks.lease_token = requested.lease_token
        RETURNING requested.request_index, tasks.id, tasks.attempt
    ),
    completed_attempts AS (
        UPDATE pgtask.attempts
        SET state = 'succeeded', finished_at = statement_timestamp(), error = NULL
        FROM completed_tasks
        WHERE attempts.task_id = completed_tasks.id AND attempts.attempt = completed_tasks.attempt
    )
    SELECT requested.request_index, completed_tasks.id IS NOT NULL
    FROM requested
    LEFT JOIN completed_tasks USING (request_index)
    ORDER BY requested.request_index;
END;
$$;

CREATE FUNCTION pgtask.fail_tasks(p_failures jsonb)
RETURNS TABLE(request_index bigint, state text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF p_failures IS NULL OR jsonb_typeof(p_failures) <> 'array' THEN
        RAISE EXCEPTION 'failures must be a JSON array' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_failures) AS items(failure)
        GROUP BY failure->>'task_id'
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'failures contain duplicate task IDs' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH requested AS (
        SELECT
            items.ordinality - 1 AS request_index,
            (items.failure->>'task_id')::uuid AS task_id,
            (items.failure->>'attempt')::integer AS attempt,
            (items.failure->>'lease_token')::uuid AS lease_token,
            items.failure->'error' AS error,
            (items.failure->>'retry_milliseconds')::bigint AS retry_milliseconds
        FROM jsonb_array_elements(p_failures) WITH ORDINALITY AS items(failure, ordinality)
    ),
    failed_tasks AS (
        UPDATE pgtask.tasks
        SET state = CASE
                WHEN requested.retry_milliseconds IS NOT NULL AND tasks.attempt < tasks.max_attempts THEN 'pending'
                ELSE 'failed'
            END,
            run_at = CASE
                WHEN requested.retry_milliseconds IS NOT NULL
                    THEN statement_timestamp() + (requested.retry_milliseconds * interval '1 millisecond')
                ELSE tasks.run_at
            END,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = CASE
                WHEN requested.retry_milliseconds IS NOT NULL AND tasks.attempt < tasks.max_attempts THEN NULL
                ELSE statement_timestamp()
            END,
            updated_at = statement_timestamp(),
            error = requested.error
        FROM requested
        WHERE tasks.id = requested.task_id
            AND tasks.state = 'running'
            AND tasks.attempt = requested.attempt
            AND tasks.lease_token = requested.lease_token
        RETURNING requested.request_index, tasks.id, tasks.attempt, tasks.state, requested.error
    ),
    failed_attempts AS (
        UPDATE pgtask.attempts
        SET state = 'failed', finished_at = statement_timestamp(), error = failed_tasks.error
        FROM failed_tasks
        WHERE attempts.task_id = failed_tasks.id AND attempts.attempt = failed_tasks.attempt
    )
    SELECT requested.request_index, failed_tasks.state
    FROM requested
    LEFT JOIN failed_tasks USING (request_index)
    ORDER BY requested.request_index;
END;
$$;

REVOKE ALL ON FUNCTION pgtask.complete_tasks(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.fail_tasks(jsonb) FROM PUBLIC;

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
        WHERE pg_proc.oid = 'pgtask.complete_task(uuid, integer, uuid, jsonb)'::regprocedure
            AND privileges.privilege_type = 'EXECUTE'
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.complete_tasks(jsonb) TO %s', target);
    END LOOP;
    FOR target IN
        SELECT privileges.grantee::regrole
        FROM pg_proc
        CROSS JOIN LATERAL aclexplode(pg_proc.proacl) AS privileges
        WHERE pg_proc.oid = 'pgtask.fail_task(uuid, integer, uuid, jsonb, bigint)'::regprocedure
            AND privileges.privilege_type = 'EXECUTE'
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.fail_tasks(jsonb) TO %s', target);
    END LOOP;

    definition := pg_get_functiondef('pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)'::regprocedure);
    rewritten := replace(
        definition,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.complete_task(uuid, integer, uuid, jsonb) TO %s'', p_worker);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.complete_task(uuid, integer, uuid, jsonb) TO %s'', p_worker);\n    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.complete_tasks(jsonb) TO %s'', p_worker);\n'
    );
    rewritten := replace(
        rewritten,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.fail_task(uuid, integer, uuid, jsonb, bigint) TO %s'', p_worker);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.fail_task(uuid, integer, uuid, jsonb, bigint) TO %s'', p_worker);\n    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.fail_tasks(jsonb) TO %s'', p_worker);\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not extend pgtask.configure_grants';
    END IF;
    EXECUTE rewritten;
END;
$$;
