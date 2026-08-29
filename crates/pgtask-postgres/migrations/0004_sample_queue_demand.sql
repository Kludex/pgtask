ALTER TABLE pgtask.queues
ADD COLUMN demand_sampled_at timestamptz;

CREATE FUNCTION pgtask.heartbeat_worker_with_sampling(
    p_worker_id uuid,
    p_ttl_milliseconds bigint,
    p_draining boolean,
    p_sample_interval_milliseconds bigint
)
RETURNS TABLE(updated boolean, should_sample boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    target_queue text;
BEGIN
    IF p_sample_interval_milliseconds <= 0 THEN
        RAISE EXCEPTION 'sample interval must be positive' USING ERRCODE = '22023';
    END IF;
    SELECT pgtask.heartbeat_worker(p_worker_id, p_ttl_milliseconds, p_draining) INTO updated;
    should_sample := false;
    IF updated AND NOT p_draining THEN
        SELECT workers.queue_name INTO target_queue
        FROM pgtask.workers
        WHERE workers.id = p_worker_id;

        UPDATE pgtask.queues
        SET demand_sampled_at = statement_timestamp()
        WHERE name = target_queue
            AND (
                demand_sampled_at IS NULL
                OR demand_sampled_at <= statement_timestamp()
                    - (p_sample_interval_milliseconds * interval '1 millisecond')
            );
        should_sample := FOUND;
    END IF;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint) FROM PUBLIC;

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
        WHERE pg_proc.oid = 'pgtask.heartbeat_worker(uuid, bigint, boolean)'::regprocedure
            AND privileges.privilege_type = 'EXECUTE'
    LOOP
        EXECUTE format(
            'GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint) TO %s',
            target
        );
    END LOOP;

    definition := pg_get_functiondef('pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)'::regprocedure);
    rewritten := replace(
        definition,
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s'', p_worker);\n',
        E'    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s'', p_worker);\n    EXECUTE format(''GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint) TO %s'', p_worker);\n'
    );
    IF rewritten = definition THEN
        RAISE EXCEPTION 'could not extend pgtask.configure_grants';
    END IF;
    EXECUTE rewritten;
END;
$$;
