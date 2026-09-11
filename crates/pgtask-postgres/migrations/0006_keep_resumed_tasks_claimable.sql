ALTER TABLE pgtask.tasks
ADD COLUMN failed_attempts integer
CHECK (failed_attempts IS NULL OR failed_attempts >= 0 AND failed_attempts <= attempt);

ALTER TABLE pgtask.tasks ALTER COLUMN failed_attempts SET DEFAULT 0;

DO $$
DECLARE
    function_oid oid;
    definition text;
    rewritten text;
    historical_failures constant text :=
        'COALESCE(tasks.failed_attempts, (SELECT count(*)::integer FROM pgtask.attempts AS history WHERE history.task_id = tasks.id AND history.state IN (''failed'', ''lost'')))';
BEGIN
    FOREACH function_oid IN ARRAY ARRAY[
        'pgtask.claim(text, uuid, text[], integer[], integer, bigint)'::regprocedure::oid,
        'pgtask.next_task_delay_milliseconds(text, text[], integer[])'::regprocedure::oid
    ]
    LOOP
        definition := pg_get_functiondef(function_oid);
        rewritten := replace(
            definition,
            'tasks.attempt < tasks.max_attempts',
            historical_failures || ' < tasks.max_attempts'
        );
        IF rewritten = definition OR rewritten LIKE '%tasks.attempt < tasks.max_attempts%' THEN
            RAISE EXCEPTION 'could not replace the claim budget in function %', function_oid::regprocedure;
        END IF;
        IF function_oid = 'pgtask.claim(text, uuid, text[], integer[], integer, bigint)'::regprocedure::oid THEN
            rewritten := replace(
                rewritten,
                'SET state = ''running'',',
                'SET state = ''running'', failed_attempts = ' || historical_failures || ','
            );
        END IF;
        EXECUTE rewritten;
    END LOOP;

    function_oid := 'pgtask.fail_task(uuid, integer, uuid, jsonb, bigint)'::regprocedure::oid;
    definition := pg_get_functiondef(function_oid);
    rewritten := replace(
        definition,
        'attempt < max_attempts',
        'COALESCE(failed_attempts, (SELECT count(*)::integer FROM pgtask.attempts AS history WHERE history.task_id = tasks.id AND history.state IN (''failed'', ''lost''))) + 1 < max_attempts'
    );
    rewritten := replace(
        rewritten,
        '            error = p_error',
        E'            error = p_error,\n            failed_attempts = COALESCE(failed_attempts, (SELECT count(*)::integer FROM pgtask.attempts AS history WHERE history.task_id = tasks.id AND history.state IN (''failed'', ''lost''))) + 1'
    );
    IF rewritten = definition
        OR rewritten LIKE '%attempt < max_attempts%'
        OR rewritten NOT LIKE '%failed_attempts = COALESCE(failed_attempts,%'
    THEN
        RAISE EXCEPTION 'could not replace the failure budget in function %', function_oid::regprocedure;
    END IF;
    EXECUTE rewritten;

    function_oid := 'pgtask.recover_expired(text, integer)'::regprocedure::oid;
    definition := pg_get_functiondef(function_oid);
    rewritten := replace(
        definition,
        'tasks.attempt < tasks.max_attempts',
        historical_failures || ' + 1 < tasks.max_attempts'
    );
    rewritten := replace(
        rewritten,
        '            error = jsonb_build_object(''type'', ''lease_expired'')',
        E'            error = jsonb_build_object(''type'', ''lease_expired''),\n            failed_attempts = ' || historical_failures || ' + 1'
    );
    IF rewritten = definition
        OR rewritten LIKE '%tasks.attempt < tasks.max_attempts%'
        OR rewritten NOT LIKE '%failed_attempts = COALESCE(tasks.failed_attempts,%'
    THEN
        RAISE EXCEPTION 'could not replace the recovery budget in function %', function_oid::regprocedure;
    END IF;
    EXECUTE rewritten;

    function_oid := 'pgtask.admin_retry_task(uuid, text)'::regprocedure::oid;
    definition := pg_get_functiondef(function_oid);
    rewritten := replace(
        definition,
        'max_attempts = GREATEST(max_attempts, attempt + 1)',
        'max_attempts = GREATEST(max_attempts, COALESCE(failed_attempts, (SELECT count(*)::integer FROM pgtask.attempts AS history WHERE history.task_id = tasks.id AND history.state IN (''failed'', ''lost''))) + 1)'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not replace the administrator retry budget in function %', function_oid::regprocedure;
    END IF;
    EXECUTE rewritten;
END;
$$;
