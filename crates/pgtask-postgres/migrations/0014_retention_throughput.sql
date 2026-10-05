-- Retention read every terminal task in a queue and sorted the expired ones to delete a batch of
-- 100, and every deleted task cascaded into result_waits with a sequential scan, because the only
-- index on result_waits.result_task_id is partial. Worker rows were never deleted at all.
--
-- On a large tasks table, building tasks_terminal_retention_idx blocks writes to tasks while it
-- runs. To avoid that, create the same index with CREATE INDEX CONCURRENTLY before upgrading;
-- IF NOT EXISTS then skips it here.

CREATE INDEX IF NOT EXISTS tasks_terminal_retention_idx
    ON pgtask.tasks (queue_name, completed_at, id)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

-- The ON DELETE CASCADE from tasks looks rows up by result_task_id alone, resolved or not.
CREATE INDEX IF NOT EXISTS result_waits_result_task_idx
    ON pgtask.result_waits (result_task_id);

-- Delete the locked candidates by tuple id instead of joining back on the key columns. CREATE OR
-- REPLACE keeps each function's owner and grants; signatures and results are unchanged.

CREATE OR REPLACE FUNCTION pgtask.delete_expired_terminal(p_queue_name text, p_limit integer)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH candidates AS (
        SELECT tasks.ctid AS tuple_id
        FROM pgtask.tasks
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state IN ('succeeded', 'failed', 'cancelled')
            -- A scalar cutoff, not a join filter, so it bounds the index scan: a short final batch
            -- stops at the first unexpired row instead of reading every terminal task in the queue.
            AND tasks.completed_at <= (
                SELECT statement_timestamp() - (queues.terminal_retention_seconds * interval '1 second')
                FROM pgtask.queues
                WHERE queues.name = p_queue_name
            )
            AND NOT EXISTS (
                SELECT 1 FROM pgtask.tasks AS children WHERE children.parent_task_id = tasks.id
            )
        ORDER BY tasks.completed_at, tasks.id
        FOR UPDATE OF tasks SKIP LOCKED
        LIMIT p_limit
    ),
    deleted AS (
        DELETE FROM pgtask.tasks
        WHERE tasks.ctid = ANY (ARRAY(SELECT candidates.tuple_id FROM candidates))
        RETURNING tasks.id
    )
    SELECT count(*) FROM deleted;
$$;

CREATE OR REPLACE FUNCTION pgtask.delete_expired_idempotency_keys(p_queue_name text, p_limit integer)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH candidates AS (
        SELECT idempotency_keys.ctid AS tuple_id
        FROM pgtask.idempotency_keys
        WHERE idempotency_keys.queue_name = p_queue_name
            AND idempotency_keys.expires_at <= statement_timestamp()
        ORDER BY idempotency_keys.expires_at, idempotency_keys.idempotency_key
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    ),
    deleted AS (
        DELETE FROM pgtask.idempotency_keys
        WHERE idempotency_keys.ctid = ANY (ARRAY(SELECT candidates.tuple_id FROM candidates))
        RETURNING idempotency_keys.idempotency_key
    )
    SELECT count(*) FROM deleted;
$$;

-- A worker row is inserted per process start and only ever expired. Delete rows that expired more
-- than p_grace_milliseconds ago; worker_capabilities follows by cascade.
CREATE FUNCTION pgtask.delete_expired_workers(p_grace_milliseconds bigint, p_limit integer)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH candidates AS (
        SELECT workers.ctid AS tuple_id
        FROM pgtask.workers
        WHERE workers.expires_at <= statement_timestamp() - (p_grace_milliseconds * interval '1 millisecond')
        ORDER BY workers.expires_at, workers.id
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    ),
    deleted AS (
        DELETE FROM pgtask.workers
        WHERE workers.ctid = ANY (ARRAY(SELECT candidates.tuple_id FROM candidates))
        RETURNING workers.id
    )
    SELECT count(*) FROM deleted;
$$;

REVOKE ALL ON FUNCTION pgtask.delete_expired_workers(bigint, integer) FROM PUBLIC;

-- Grant it to every role that may already run retention, and teach configure_grants to do the same.
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
        WHERE pg_proc.oid = 'pgtask.delete_expired_terminal(text, integer)'::regprocedure
            AND privileges.privilege_type = 'EXECUTE'
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_workers(bigint, integer) TO %s', target);
    END LOOP;

    definition := pg_get_functiondef('pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)'::regprocedure);
    rewritten := replace(
        definition,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s'', p_worker);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s'', p_worker);\n'
        || E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.delete_expired_workers(bigint, integer) TO %s'', p_worker);\n'
    );
    rewritten := replace(
        rewritten,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s'', p_administrator);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s'', p_administrator);\n'
        || E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.delete_expired_workers(bigint, integer) TO %s'', p_administrator);\n'
    );
    IF rewritten NOT LIKE '%delete_expired_workers(bigint, integer) TO %%s'', p_worker)%'
        OR rewritten NOT LIKE '%delete_expired_workers(bigint, integer) TO %%s'', p_administrator)%'
    THEN
        RAISE EXCEPTION 'could not extend pgtask.configure_grants';
    END IF;
    EXECUTE rewritten;
END;
$$;
