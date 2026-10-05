-- A task that finishes while it waits for a signal (cancellation is the only way) used to keep its
-- pgtask.waits row unresolved. When an administrator retried it, the handler replayed to the same
-- wait_for_signal step and inserted the same key again, which failed on waits_pkey every attempt.
--
-- 1. Finishing a task resolves its open signal waits with outcome 'cancelled', in the trigger that
--    already does this for its result waits, and existing leftovers are resolved the same way.
-- 2. wait_for_signal and wait_for_result re-arm a wait row left over from an earlier attempt. They
--    hold the task row lock and have just found no checkpoint for the step, so any row under that
--    key is from an attempt that ended without the wait completing; a completed wait always writes
--    its checkpoint in the same transaction that resolves the row.
--
-- The functions are rewritten from their installed definitions, so earlier rewrites are kept.

ALTER TABLE pgtask.waits DROP CONSTRAINT waits_outcome_check;
ALTER TABLE pgtask.waits
    ADD CONSTRAINT waits_outcome_check CHECK (outcome IN ('signal', 'timeout', 'cancelled'));

UPDATE pgtask.waits
SET resolved_at = statement_timestamp(), outcome = 'cancelled'
FROM pgtask.tasks
WHERE waits.task_id = tasks.id
    AND waits.resolved_at IS NULL
    AND tasks.state IN ('succeeded', 'failed', 'cancelled');

DO $$
DECLARE
    definition text;
    rewritten text;
BEGIN
    definition := pg_get_functiondef('pgtask.resolve_task_result()'::regprocedure);
    rewritten := replace(
        definition,
        E'    UPDATE pgtask.result_waits\n    SET resolved_at = statement_timestamp(), outcome = ''cancelled''\n    WHERE task_id = NEW.id AND resolved_at IS NULL;\n',
        E'    UPDATE pgtask.result_waits\n    SET resolved_at = statement_timestamp(), outcome = ''cancelled''\n    WHERE task_id = NEW.id AND resolved_at IS NULL;\n\n'
        || E'    UPDATE pgtask.waits\n    SET resolved_at = statement_timestamp(), outcome = ''cancelled''\n    WHERE task_id = NEW.id AND resolved_at IS NULL;\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not extend pgtask.resolve_task_result';
    END IF;
    EXECUTE rewritten;

    definition := pg_get_functiondef(
        'pgtask.wait_for_signal(uuid, integer, uuid, text, integer, text, integer, bigint)'::regprocedure
    );
    rewritten := replace(
        definition,
        E'        p_task_id, target_handler_version, p_step_name, p_occurrence, p_signal_name, p_signal_occurrence, timeout_at\n    );\n',
        E'        p_task_id, target_handler_version, p_step_name, p_occurrence, p_signal_name, p_signal_occurrence, timeout_at\n    )\n'
        || E'    ON CONFLICT (task_id, handler_version, step_name, occurrence) DO UPDATE\n'
        || E'    SET signal_name = EXCLUDED.signal_name,\n'
        || E'        signal_occurrence = EXCLUDED.signal_occurrence,\n'
        || E'        timeout_at = EXCLUDED.timeout_at,\n'
        || E'        created_at = EXCLUDED.created_at,\n'
        || E'        resolved_at = NULL,\n'
        || E'        outcome = NULL;\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not re-arm stale waits in pgtask.wait_for_signal';
    END IF;
    EXECUTE rewritten;

    definition := pg_get_functiondef(
        'pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid, bigint)'::regprocedure
    );
    rewritten := replace(
        definition,
        E'            ELSE statement_timestamp() + (p_timeout_milliseconds * interval ''1 millisecond'')\n        END\n    );\n',
        E'            ELSE statement_timestamp() + (p_timeout_milliseconds * interval ''1 millisecond'')\n        END\n    )\n'
        || E'    ON CONFLICT (task_id, handler_version, step_name, occurrence) DO UPDATE\n'
        || E'    SET result_task_id = EXCLUDED.result_task_id,\n'
        || E'        timeout_at = EXCLUDED.timeout_at,\n'
        || E'        created_at = EXCLUDED.created_at,\n'
        || E'        resolved_at = NULL,\n'
        || E'        outcome = NULL;\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not re-arm stale waits in pgtask.wait_for_result';
    END IF;
    EXECUTE rewritten;
END;
$$;
