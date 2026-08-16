ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_worker_maintenance;

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
    PERFORM pgtask.configure_grants_before_worker_maintenance(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.put_schedule(uuid, text, text, bigint, text, text, integer, text, text, integer, jsonb, jsonb, smallint, integer, timestamptz) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s', p_worker);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_worker_maintenance(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
