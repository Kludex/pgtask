-- A worker that shuts down aborts handlers still running after its grace period. Their leases used
-- to stay in place until they expired, and lease recovery then charged each task a failed attempt,
-- so a routine restart could fail a task on its last attempt for good.
--
-- release_tasks hands such tasks back. It is fenced like every other lease-owned transition, makes
-- the task pending and claimable at once, and leaves failed_attempts alone: the handler did not
-- fail, the worker stopped. The attempt is recorded with the new state 'released'.

ALTER TABLE pgtask.attempts DROP CONSTRAINT attempts_state_check;
ALTER TABLE pgtask.attempts
    ADD CONSTRAINT attempts_state_check CHECK (
        state IN ('running', 'succeeded', 'failed', 'lost', 'cancelled', 'suspended', 'released')
    );

-- Rows are locked ancestors first, in the same order renew_leases uses, so a release cannot
-- deadlock with a renewal or a result wait touching the same workflow.
CREATE FUNCTION pgtask.release_tasks(p_task_ids uuid[], p_attempts integer[], p_lease_tokens uuid[])
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH RECURSIVE requested AS MATERIALIZED (
        SELECT *
        FROM unnest(p_task_ids, p_attempts, p_lease_tokens) AS leases(task_id, attempt, lease_token)
    ),
    ancestry AS (
        SELECT tasks.id, tasks.parent_task_id, 0 AS depth
        FROM pgtask.tasks
        WHERE tasks.id = ANY(p_task_ids)
        UNION ALL
        SELECT ancestry.id, parents.parent_task_id, ancestry.depth + 1
        FROM ancestry
        JOIN pgtask.tasks AS parents ON parents.id = ancestry.parent_task_id
    ),
    depths AS (
        SELECT id, max(depth) AS depth
        FROM ancestry
        GROUP BY id
    ),
    locked AS MATERIALIZED (
        SELECT tasks.id
        FROM pgtask.tasks
        JOIN depths ON depths.id = tasks.id
        ORDER BY depths.depth, tasks.id
        FOR NO KEY UPDATE OF tasks
    ),
    released AS (
        UPDATE pgtask.tasks
        SET state = 'pending',
            run_at = statement_timestamp(),
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            updated_at = statement_timestamp()
        FROM requested
        JOIN locked ON locked.id = requested.task_id
        WHERE tasks.id = requested.task_id
            AND tasks.state = 'running'
            AND tasks.attempt = requested.attempt
            AND tasks.lease_token = requested.lease_token
        RETURNING tasks.id, tasks.attempt
    ),
    released_attempts AS (
        UPDATE pgtask.attempts
        SET state = 'released', finished_at = statement_timestamp()
        FROM released
        WHERE attempts.task_id = released.id AND attempts.attempt = released.attempt
    )
    SELECT released.id FROM released;
$$;

REVOKE ALL ON FUNCTION pgtask.release_tasks(uuid[], integer[], uuid[]) FROM PUBLIC;

-- Grant it wherever renew_leases is granted, and teach configure_grants to do the same.
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
        WHERE pg_proc.oid = 'pgtask.renew_leases(uuid[], integer[], uuid[], bigint)'::regprocedure
            AND privileges.privilege_type = 'EXECUTE'
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.release_tasks(uuid[], integer[], uuid[]) TO %s', target);
    END LOOP;

    definition := pg_get_functiondef('pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)'::regprocedure);
    rewritten := replace(
        definition,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.renew_leases(uuid[], integer[], uuid[], bigint) TO %s'', p_worker);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.renew_leases(uuid[], integer[], uuid[], bigint) TO %s'', p_worker);\n'
        || E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.release_tasks(uuid[], integer[], uuid[]) TO %s'', p_worker);\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not extend pgtask.configure_grants';
    END IF;
    EXECUTE rewritten;
END;
$$;
