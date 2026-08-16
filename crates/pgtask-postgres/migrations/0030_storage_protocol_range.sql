CREATE FUNCTION pgtask.storage_protocol_range()
RETURNS TABLE(minimum integer, maximum integer)
LANGUAGE sql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT 1, 1;
$$;

REVOKE ALL ON FUNCTION pgtask.storage_protocol_range() FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_storage_protocol_range;

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
    PERFORM pgtask.configure_grants_before_storage_protocol_range(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.storage_protocol_range() TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.storage_protocol_range() TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.storage_protocol_range() TO %s', p_observer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.storage_protocol_range() TO %s', p_administrator);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_storage_protocol_range(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
