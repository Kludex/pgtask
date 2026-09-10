-- A task that sleeps or waits on its final attempt was stranded in `pending`.
--
-- `claim` only considers tasks with `attempt < max_attempts`, and `attempt` is
-- incremented at claim time, so it counts runs started rather than failures.
-- Every durable primitive returns a task to `pending` without touching
-- `attempt`, so a task claimed on its last attempt and then suspended came back
-- as `pending` at `attempt = max_attempts` and was never picked up again. It was
-- not running, not waiting, and not terminal, and no sweep reached it:
-- `recover_expired` only visits `running` tasks and the two wait sweeps only
-- visit unresolved waits.
--
-- Resuming from a durable wait is not a retry, so the resume must not have to
-- pay for one. Each of the five resume paths now grants the same headroom
-- `admin_retry_task` already grants when it requeues a task:
--
--     max_attempts = GREATEST(max_attempts, attempt + 1)
--
-- `GREATEST` makes this a no-op whenever budget remains, so it only fires on the
-- final attempt. It grants exactly one further claim: after the resumed run
-- takes `attempt` to `max_attempts`, a failure is terminal as before, so no
-- extra retry is created.
--
-- The alternative was to decrement `attempt` on suspend so the resumed run
-- reuses it. That keeps `max_attempts` meaning what the caller set, but it makes
-- `attempt` stop counting runs, it hides sleeps from the attempts history, and
-- it makes attempt numbers repeat, which the running-task fence is keyed on.
-- Granting headroom leaves both `attempt` and the fence monotone.
--
-- Rewriting the live definitions rather than restating them keeps whatever
-- 0003 removed from these bodies removed.

DO $$
DECLARE
    target_name text;
    function_oid oid;
    definition text;
    rewritten text;
BEGIN
    FOREACH target_name IN ARRAY ARRAY[
        'suspend_task',
        'emit_signal',
        'recover_wait_timeouts',
        'resolve_task_result',
        'recover_result_wait_timeouts'
    ]
    LOOP
        FOR function_oid IN
            SELECT pg_proc.oid
            FROM pg_proc
            JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
            WHERE pg_namespace.nspname = 'pgtask'
                AND pg_proc.proname = target_name
        LOOP
            definition := pg_get_functiondef(function_oid);
            rewritten := replace(
                definition,
                'SET state = ''pending'',',
                'SET max_attempts = GREATEST(tasks.max_attempts, tasks.attempt + 1), state = ''pending'','
            );
            IF rewritten = definition THEN
                RAISE EXCEPTION
                    'could not grant resume headroom in function %', function_oid::regprocedure;
            END IF;
            EXECUTE rewritten;
        END LOOP;
    END LOOP;
END;
$$;

-- The rewrite is a blind string replacement, so prove it landed where it had to
-- and nowhere else. The patterns below are whole literals rather than wildcards
-- either side of a column name: `prosrc LIKE '%GREATEST(%max_attempts%'` also
-- matches `claim`, whose unrelated `GREATEST(p_limit - ..., 0)` sits earlier in
-- a body that mentions `max_attempts` further down.
DO $$
DECLARE
    resumed constant text[] := ARRAY[
        'suspend_task',
        'emit_signal',
        'recover_wait_timeouts',
        'resolve_task_result',
        'recover_result_wait_timeouts'
    ];
    granted constant text :=
        '%SET max_attempts = GREATEST(tasks.max_attempts, tasks.attempt + 1), state = ''pending'',%';
    target_name text;
    missing text[] := ARRAY[]::text[];
BEGIN
    FOREACH target_name IN ARRAY resumed
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_proc
            JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
            WHERE pg_namespace.nspname = 'pgtask'
                AND pg_proc.proname = target_name
                AND pg_proc.prosrc LIKE granted
        ) THEN
            missing := missing || target_name;
        END IF;
    END LOOP;

    IF cardinality(missing) > 0 THEN
        RAISE EXCEPTION 'resume headroom missing from: %', array_to_string(missing, ', ');
    END IF;

    -- `admin_retry_task` had this clause before this migration and must still
    -- have exactly the one it had: it also matches `SET state = 'pending',`, so
    -- a rewrite that reached it would leave two assignments to `max_attempts`.
    IF EXISTS (
        SELECT 1
        FROM pg_proc
        JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
        WHERE pg_namespace.nspname = 'pgtask'
            AND pg_proc.proname = 'admin_retry_task'
            AND pg_proc.prosrc LIKE granted
    ) THEN
        RAISE EXCEPTION 'the rewrite reached admin_retry_task, which already had headroom';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
        WHERE pg_namespace.nspname = 'pgtask'
            AND pg_proc.proname = 'admin_retry_task'
            AND pg_proc.prosrc LIKE '%max_attempts = GREATEST(max_attempts, attempt + 1),%'
    ) THEN
        RAISE EXCEPTION 'admin_retry_task lost the headroom it already had';
    END IF;

    -- The budget filter is the thing this migration works around, not the thing
    -- it edits. Both read paths must still refuse a task that is out of budget.
    FOREACH target_name IN ARRAY ARRAY['claim', 'next_task_delay_milliseconds']
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_proc
            JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
            WHERE pg_namespace.nspname = 'pgtask'
                AND pg_proc.proname = target_name
                AND pg_proc.prosrc LIKE '%tasks.attempt < tasks.max_attempts%'
                AND pg_proc.prosrc NOT LIKE granted
        ) THEN
            RAISE EXCEPTION 'the rewrite disturbed the budget filter in %', target_name;
        END IF;
    END LOOP;
END;
$$;
