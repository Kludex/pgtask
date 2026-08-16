ALTER TABLE pgtask.tasks
ADD COLUMN retry_kind text,
ADD COLUMN retry_base_delay_milliseconds bigint,
ADD COLUMN retry_factor integer,
ADD COLUMN retry_max_delay_milliseconds bigint,
ADD CONSTRAINT tasks_retry_policy_check CHECK (
    (
        retry_kind IS NULL
        AND retry_base_delay_milliseconds IS NULL
        AND retry_factor IS NULL
        AND retry_max_delay_milliseconds IS NULL
    )
    OR (
        retry_kind = 'never'
        AND retry_base_delay_milliseconds IS NULL
        AND retry_factor IS NULL
        AND retry_max_delay_milliseconds IS NULL
    )
    OR (
        retry_kind = 'fixed'
        AND retry_base_delay_milliseconds >= 0
        AND retry_factor IS NULL
        AND retry_max_delay_milliseconds IS NULL
    )
    OR (
        retry_kind = 'exponential'
        AND retry_base_delay_milliseconds >= 0
        AND retry_factor > 0
        AND retry_max_delay_milliseconds >= retry_base_delay_milliseconds
    )
);

CREATE TABLE pgtask.handler_policies (
    queue_name text NOT NULL REFERENCES pgtask.queues (name) ON DELETE CASCADE,
    task_name text NOT NULL,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    retry_kind text NOT NULL,
    retry_base_delay_milliseconds bigint,
    retry_factor integer,
    retry_max_delay_milliseconds bigint,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK (
        (
            retry_kind = 'never'
            AND retry_base_delay_milliseconds IS NULL
            AND retry_factor IS NULL
            AND retry_max_delay_milliseconds IS NULL
        )
        OR (
            retry_kind = 'fixed'
            AND retry_base_delay_milliseconds >= 0
            AND retry_factor IS NULL
            AND retry_max_delay_milliseconds IS NULL
        )
        OR (
            retry_kind = 'exponential'
            AND retry_base_delay_milliseconds >= 0
            AND retry_factor > 0
            AND retry_max_delay_milliseconds >= retry_base_delay_milliseconds
        )
    ),
    PRIMARY KEY (queue_name, task_name, handler_version)
);

CREATE VIEW pgtask.handler_policy_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.handler_policies;

CREATE OR REPLACE VIEW pgtask.task_view WITH (security_barrier = true) AS
SELECT * FROM pgtask.tasks;

CREATE FUNCTION pgtask.snapshot_task_retry_policy()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF NEW.retry_kind IS NULL THEN
        SELECT
            handler_policies.retry_kind,
            handler_policies.retry_base_delay_milliseconds,
            handler_policies.retry_factor,
            handler_policies.retry_max_delay_milliseconds
        INTO
            NEW.retry_kind,
            NEW.retry_base_delay_milliseconds,
            NEW.retry_factor,
            NEW.retry_max_delay_milliseconds
        FROM pgtask.handler_policies
        WHERE handler_policies.queue_name = NEW.queue_name
            AND handler_policies.task_name = NEW.task_name
            AND handler_policies.handler_version = NEW.handler_version;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_snapshot_retry_policy
BEFORE INSERT ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.snapshot_task_retry_policy();

CREATE FUNCTION pgtask.register_worker(
    p_worker_id uuid,
    p_queue_name text,
    p_version text,
    p_task_names text[],
    p_handler_versions integer[],
    p_retry_kinds text[],
    p_retry_base_delay_milliseconds bigint[],
    p_retry_factors integer[],
    p_retry_max_delay_milliseconds bigint[],
    p_ttl_milliseconds bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
BEGIN
    IF cardinality(p_task_names) = 0
        OR cardinality(p_task_names) <> cardinality(p_handler_versions)
        OR cardinality(p_task_names) <> cardinality(p_retry_kinds)
        OR cardinality(p_task_names) <> cardinality(p_retry_base_delay_milliseconds)
        OR cardinality(p_task_names) <> cardinality(p_retry_factors)
        OR cardinality(p_task_names) <> cardinality(p_retry_max_delay_milliseconds)
    THEN
        RAISE EXCEPTION 'worker capabilities and retry policies must be nonempty and aligned'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM unnest(p_task_names, p_handler_versions) AS capabilities(task_name, handler_version)
        GROUP BY capabilities.task_name, capabilities.handler_version
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'worker capabilities must be unique' USING ERRCODE = '22023';
    END IF;

    PERFORM pgtask.register_worker(
        p_worker_id,
        p_queue_name,
        p_version,
        p_task_names,
        p_handler_versions,
        p_ttl_milliseconds
    );

    IF EXISTS (
        SELECT 1
        FROM unnest(
            p_task_names,
            p_handler_versions,
            p_retry_kinds,
            p_retry_base_delay_milliseconds,
            p_retry_factors,
            p_retry_max_delay_milliseconds
        ) AS requested(
            task_name,
            handler_version,
            retry_kind,
            retry_base_delay_milliseconds,
            retry_factor,
            retry_max_delay_milliseconds
        )
        JOIN pgtask.handler_policies
            ON handler_policies.queue_name = p_queue_name
            AND handler_policies.task_name = requested.task_name
            AND handler_policies.handler_version = requested.handler_version
        WHERE ROW(
            handler_policies.retry_kind,
            handler_policies.retry_base_delay_milliseconds,
            handler_policies.retry_factor,
            handler_policies.retry_max_delay_milliseconds
        ) IS DISTINCT FROM ROW(
            requested.retry_kind,
            requested.retry_base_delay_milliseconds,
            requested.retry_factor,
            requested.retry_max_delay_milliseconds
        )
    ) THEN
        RAISE EXCEPTION 'retry policy is immutable for a queue, task name, and handler version'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO pgtask.handler_policies (
        queue_name,
        task_name,
        handler_version,
        retry_kind,
        retry_base_delay_milliseconds,
        retry_factor,
        retry_max_delay_milliseconds
    )
    SELECT
        p_queue_name,
        requested.task_name,
        requested.handler_version,
        requested.retry_kind,
        requested.retry_base_delay_milliseconds,
        requested.retry_factor,
        requested.retry_max_delay_milliseconds
    FROM unnest(
        p_task_names,
        p_handler_versions,
        p_retry_kinds,
        p_retry_base_delay_milliseconds,
        p_retry_factors,
        p_retry_max_delay_milliseconds
    ) AS requested(
        task_name,
        handler_version,
        retry_kind,
        retry_base_delay_milliseconds,
        retry_factor,
        retry_max_delay_milliseconds
    )
    ON CONFLICT DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION pgtask.claim(
    p_queue_name text,
    p_worker_id uuid,
    p_task_names text[],
    p_handler_versions integer[],
    p_limit integer,
    p_lease_milliseconds bigint
)
RETURNS SETOF pgtask.tasks
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH candidates AS (
        SELECT
            tasks.id,
            handler_policies.retry_kind,
            handler_policies.retry_base_delay_milliseconds,
            handler_policies.retry_factor,
            handler_policies.retry_max_delay_milliseconds
        FROM pgtask.tasks
        LEFT JOIN pgtask.handler_policies
            ON handler_policies.queue_name = tasks.queue_name
            AND handler_policies.task_name = tasks.task_name
            AND handler_policies.handler_version = tasks.handler_version
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state = 'pending'
            AND tasks.run_at <= statement_timestamp()
            AND tasks.attempt < tasks.max_attempts
            AND EXISTS (
                SELECT 1
                FROM pgtask.queues
                WHERE queues.name = tasks.queue_name AND queues.paused_at IS NULL
            )
            AND EXISTS (
                SELECT 1
                FROM unnest(p_task_names, p_handler_versions) AS handlers(task_name, handler_version)
                WHERE handlers.task_name = tasks.task_name
                    AND handlers.handler_version = tasks.handler_version
            )
        ORDER BY tasks.priority DESC, tasks.run_at, tasks.id
        FOR NO KEY UPDATE OF tasks SKIP LOCKED
        LIMIT p_limit
    ),
    claimed AS (
        UPDATE pgtask.tasks AS tasks
        SET state = 'running',
            attempt = tasks.attempt + 1,
            lease_token = gen_random_uuid(),
            lease_owner = p_worker_id,
            lease_expires_at = statement_timestamp() + (p_lease_milliseconds * interval '1 millisecond'),
            updated_at = statement_timestamp(),
            retry_kind = COALESCE(tasks.retry_kind, candidates.retry_kind),
            retry_base_delay_milliseconds = COALESCE(
                tasks.retry_base_delay_milliseconds,
                candidates.retry_base_delay_milliseconds
            ),
            retry_factor = COALESCE(tasks.retry_factor, candidates.retry_factor),
            retry_max_delay_milliseconds = COALESCE(
                tasks.retry_max_delay_milliseconds,
                candidates.retry_max_delay_milliseconds
            )
        FROM candidates
        WHERE tasks.id = candidates.id
        RETURNING tasks.*
    ),
    inserted_attempts AS (
        INSERT INTO pgtask.attempts (task_id, attempt, lease_token, worker_id)
        SELECT id, attempt, lease_token, lease_owner
        FROM claimed
    )
    SELECT * FROM claimed
    ORDER BY priority DESC, run_at, id;
$$;

REVOKE ALL ON TABLE pgtask.handler_policies FROM PUBLIC;
REVOKE ALL ON TABLE pgtask.handler_policy_view FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.snapshot_task_retry_policy() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.register_worker(
    uuid, text, text, text[], integer[], text[], bigint[], integer[], bigint[], bigint
) FROM PUBLIC;

ALTER FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole)
RENAME TO configure_grants_before_retry_policy;

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
    PERFORM pgtask.configure_grants_before_retry_policy(
        p_owner,
        p_producer,
        p_worker,
        p_observer,
        p_administrator
    );
    EXECUTE format(
        'GRANT EXECUTE ON FUNCTION pgtask.register_worker(uuid, text, text, text[], integer[], text[], bigint[], integer[], bigint[], bigint) TO %s',
        p_worker
    );
    EXECUTE format('GRANT SELECT ON pgtask.handler_policy_view TO %s', p_observer);
    EXECUTE format('GRANT SELECT ON pgtask.handler_policy_view TO %s', p_administrator);
END;
$$;

REVOKE ALL ON FUNCTION pgtask.configure_grants_before_retry_policy(
    regrole, regrole, regrole, regrole, regrole
) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.configure_grants(regrole, regrole, regrole, regrole, regrole) FROM PUBLIC;
