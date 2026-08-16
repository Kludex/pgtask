CREATE FUNCTION pgtask.ready_channel(p_queue_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT format(
        'pgtask_ready_%s',
        lpad(((hashtextextended(p_queue_name, 0) & 63)::integer)::text, 2, '0')
    );
$$;

CREATE FUNCTION pgtask.result_channel(p_task_id uuid)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    SELECT format(
        'pgtask_result_%s',
        lpad(((hashtextextended(p_task_id::text, 0) & 63)::integer)::text, 2, '0')
    );
$$;

CREATE FUNCTION pgtask.notify_ready_shard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF NEW.state = 'pending' AND (
        TG_OP = 'INSERT'
        OR OLD.state IS DISTINCT FROM NEW.state
        OR OLD.run_at IS DISTINCT FROM NEW.run_at
        OR OLD.queue_name IS DISTINCT FROM NEW.queue_name
    ) THEN
        PERFORM pg_notify(pgtask.ready_channel(NEW.queue_name), NEW.queue_name);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_notify_ready_shard
AFTER INSERT OR UPDATE OF state, run_at, queue_name ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.notify_ready_shard();

CREATE FUNCTION pgtask.notify_result_shard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    PERFORM pg_notify(pgtask.result_channel(NEW.id), NEW.id::text);
    RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_notify_result_shard
AFTER UPDATE OF state ON pgtask.tasks
FOR EACH ROW
WHEN (
    OLD.state NOT IN ('succeeded', 'failed', 'cancelled')
    AND NEW.state IN ('succeeded', 'failed', 'cancelled')
)
EXECUTE FUNCTION pgtask.notify_result_shard();

CREATE FUNCTION pgtask.notify_resumed_queue_shard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF OLD.paused_at IS NOT NULL AND NEW.paused_at IS NULL THEN
        PERFORM pg_notify(pgtask.ready_channel(NEW.name), NEW.name);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER queues_notify_resumed_shard
AFTER UPDATE OF paused_at ON pgtask.queues
FOR EACH ROW
EXECUTE FUNCTION pgtask.notify_resumed_queue_shard();

REVOKE ALL ON FUNCTION pgtask.ready_channel(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.result_channel(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.notify_ready_shard() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.notify_result_shard() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.notify_resumed_queue_shard() FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_notification_shards;

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
    PERFORM pgtask.configure_grants_before_notification_shards(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.ready_channel(text) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.result_channel(uuid) TO %s', p_producer);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.result_channel(uuid) TO %s', p_administrator);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_notification_shards(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
