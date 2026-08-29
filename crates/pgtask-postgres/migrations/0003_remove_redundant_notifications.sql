DROP TRIGGER IF EXISTS ensure_queue_for_task ON pgtask.tasks;

DO $$
DECLARE
    function_oid oid;
    definition text;
    rewritten text;
BEGIN
    FOR function_oid IN
        SELECT pg_proc.oid
        FROM pg_proc
        JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
        WHERE pg_namespace.nspname = 'pgtask'
            AND (
                pg_proc.prosrc LIKE '%pg_notify(''pgtask_ready''%'
                OR pg_proc.prosrc LIKE '%pg_notify(''pgtask_result''%'
            )
    LOOP
        definition := pg_get_functiondef(function_oid);
        rewritten := replace(
            definition,
            E'    IF FOUND AND NOT p_paused THEN\n        PERFORM pg_notify(''pgtask_ready'', p_name);\n    END IF;\n',
            ''
        );
        rewritten := replace(
            rewritten,
            E'    IF materialized > 0 THEN\n        PERFORM pg_notify(''pgtask_ready'', target.queue_name);\n    END IF;\n',
            ''
        );
        rewritten := replace(
            rewritten,
            E'    FOREACH queue_name IN ARRAY COALESCE(queues, ARRAY[]::text[])\n    LOOP\n        PERFORM pg_notify(''pgtask_ready'', queue_name);\n    END LOOP;\n',
            ''
        );
        rewritten := regexp_replace(
            rewritten,
            E'    PERFORM pg_notify\\(''pgtask_(ready|result)'', [^;]+\\);\n',
            '',
            'g'
        );
        IF rewritten = definition THEN
            RAISE EXCEPTION 'could not remove legacy notifications from function %', function_oid::regprocedure;
        END IF;
        EXECUTE rewritten;
    END LOOP;

    SELECT pg_proc.oid
    INTO function_oid
    FROM pg_proc
    JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
    WHERE pg_namespace.nspname = 'pgtask'
        AND pg_proc.proname = 'recover_result_wait_timeouts';
    definition := pg_get_functiondef(function_oid);
    rewritten := replace(
        definition,
        E'        IF parent_queue IS NOT NULL THEN\n            PERFORM pg_notify(pgtask.ready_channel(parent_queue), parent_queue);\n        END IF;\n',
        ''
    );
    IF rewritten <> definition THEN
        EXECUTE rewritten;
    END IF;
END;
$$;
