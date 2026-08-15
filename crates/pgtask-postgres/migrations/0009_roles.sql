CREATE VIEW pgtask.queue_overview WITH (security_barrier = true) AS
SELECT
    queues.name,
    queues.terminal_retention_seconds,
    queues.paused_at,
    queues.created_at,
    queues.updated_at,
    count(tasks.id) FILTER (WHERE tasks.state = 'pending') AS pending_count,
    count(tasks.id) FILTER (WHERE tasks.state = 'running') AS running_count,
    count(tasks.id) FILTER (WHERE tasks.state = 'waiting') AS waiting_count,
    count(tasks.id) FILTER (WHERE tasks.state IN ('succeeded', 'failed', 'cancelled')) AS terminal_count
FROM pgtask.queues
LEFT JOIN pgtask.tasks ON tasks.queue_name = queues.name
GROUP BY queues.name;

CREATE VIEW pgtask.task_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.tasks;

CREATE VIEW pgtask.attempt_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.attempts;

CREATE VIEW pgtask.worker_view WITH (security_barrier = true) AS
SELECT workers.*, workers.expires_at > statement_timestamp() AS live
FROM pgtask.workers;

CREATE VIEW pgtask.worker_capability_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.worker_capabilities;

CREATE VIEW pgtask.checkpoint_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.checkpoints;

REVOKE ALL ON SCHEMA pgtask FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA pgtask FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgtask FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgtask REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

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
DECLARE
    target regrole;
BEGIN
    FOREACH target IN ARRAY ARRAY[p_owner, p_producer, p_worker, p_observer, p_administrator]
    LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA pgtask TO %s', target);
    END LOOP;

    EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pgtask TO %s', p_owner);
    EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtask TO %s', p_owner);

    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.enqueue(text, jsonb, text, integer, timestamptz, smallint, integer, text, jsonb) TO %s',
        p_producer
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.enqueue_many(jsonb) TO %s', p_producer);

    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.claim(text, uuid, text[], integer[], integer, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.renew_leases(uuid[], integer[], uuid[], bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.complete_task(uuid, integer, uuid, jsonb) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.fail_task(uuid, integer, uuid, jsonb, bigint) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.recover_expired(text, integer) TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.register_worker(uuid, text, text, text[], integer[], bigint) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) TO %s', p_worker);
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.commit_checkpoint(uuid, integer, uuid, text, integer, jsonb) TO %s',
        p_worker
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_task(uuid) TO %s', p_worker);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) TO %s', p_worker);

    EXECUTE format(
        'GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, '
        'pgtask.worker_capability_view, pgtask.checkpoint_view TO %s',
        p_observer
    );

    EXECUTE format(
        'GRANT SELECT ON pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, '
        'pgtask.worker_capability_view, pgtask.checkpoint_view TO %s',
        p_administrator
    );
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.put_queue(text, bigint) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.set_queue_paused(text, boolean) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.cancel_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.delete_expired_terminal(text, integer) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_task(uuid) TO %s', p_administrator);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgtask.get_checkpoint(uuid, integer, text, integer) TO %s', p_administrator);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
