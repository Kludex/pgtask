CREATE FUNCTION pgtask.storage_protocol_version()
RETURNS integer
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 1;
$$;

REVOKE ALL ON FUNCTION pgtask.storage_protocol_version() FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_storage_protocol;

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
    PERFORM pgtask.configure_grants_before_storage_protocol(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.storage_protocol_version() TO %s', p_worker);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_storage_protocol(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
