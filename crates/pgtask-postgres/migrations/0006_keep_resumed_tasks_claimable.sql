ALTER TABLE pgtask.tasks
ADD COLUMN failed_attempts integer NOT NULL DEFAULT 0
CHECK (failed_attempts >= 0 AND failed_attempts <= attempt);

UPDATE pgtask.tasks AS tasks
SET failed_attempts = history.failed_attempts
FROM (
    SELECT task_id, count(*)::integer AS failed_attempts
    FROM pgtask.attempts
    WHERE state IN ('failed', 'lost')
    GROUP BY task_id
) AS history
WHERE tasks.id = history.task_id;

DO $$
DECLARE
    function_oid oid;
    definition text;
    rewritten text;
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
            'tasks.failed_attempts < tasks.max_attempts'
        );
        IF rewritten = definition OR rewritten LIKE '%tasks.attempt < tasks.max_attempts%' THEN
            RAISE EXCEPTION 'could not replace the claim budget in function %', function_oid::regprocedure;
        END IF;
        EXECUTE rewritten;
    END LOOP;

    function_oid := 'pgtask.fail_task(uuid, integer, uuid, jsonb, bigint)'::regprocedure::oid;
    definition := pg_get_functiondef(function_oid);
    rewritten := replace(
        definition,
        'attempt < max_attempts',
        'failed_attempts + 1 < max_attempts'
    );
    rewritten := replace(
        rewritten,
        '            error = p_error',
        E'            error = p_error,\n            failed_attempts = failed_attempts + 1'
    );
    IF rewritten = definition
        OR rewritten LIKE '%attempt < max_attempts%'
        OR rewritten NOT LIKE '%failed_attempts = failed_attempts + 1%'
    THEN
        RAISE EXCEPTION 'could not replace the failure budget in function %', function_oid::regprocedure;
    END IF;
    EXECUTE rewritten;

    function_oid := 'pgtask.recover_expired(text, integer)'::regprocedure::oid;
    definition := pg_get_functiondef(function_oid);
    rewritten := replace(
        definition,
        'tasks.attempt < tasks.max_attempts',
        'tasks.failed_attempts + 1 < tasks.max_attempts'
    );
    rewritten := replace(
        rewritten,
        '            error = jsonb_build_object(''type'', ''lease_expired'')',
        E'            error = jsonb_build_object(''type'', ''lease_expired''),\n            failed_attempts = tasks.failed_attempts + 1'
    );
    IF rewritten = definition
        OR rewritten LIKE '%tasks.attempt < tasks.max_attempts%'
        OR rewritten NOT LIKE '%failed_attempts = tasks.failed_attempts + 1%'
    THEN
        RAISE EXCEPTION 'could not replace the recovery budget in function %', function_oid::regprocedure;
    END IF;
    EXECUTE rewritten;

    function_oid := 'pgtask.admin_retry_task(uuid, text)'::regprocedure::oid;
    definition := pg_get_functiondef(function_oid);
    rewritten := replace(
        definition,
        'max_attempts = GREATEST(max_attempts, attempt + 1)',
        'max_attempts = GREATEST(max_attempts, failed_attempts + 1)'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not replace the administrator retry budget in function %', function_oid::regprocedure;
    END IF;
    EXECUTE rewritten;
END;
$$;
