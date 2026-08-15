ALTER FUNCTION pgtask.enqueue(text, jsonb, text, integer, timestamptz, smallint, integer, text, jsonb)
SECURITY DEFINER
SET search_path = pg_catalog, pgtask;

ALTER FUNCTION pgtask.enqueue_many(jsonb)
SECURITY DEFINER
SET search_path = pg_catalog, pgtask;

CREATE FUNCTION pgtask.claim(
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
        SELECT tasks.id
        FROM pgtask.tasks
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
            updated_at = statement_timestamp()
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

CREATE FUNCTION pgtask.renew_leases(
    p_task_ids uuid[],
    p_attempts integer[],
    p_lease_tokens uuid[],
    p_lease_milliseconds bigint
)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH requested AS (
        SELECT *
        FROM unnest(p_task_ids, p_attempts, p_lease_tokens) AS leases(task_id, attempt, lease_token)
    )
    UPDATE pgtask.tasks
    SET lease_expires_at = statement_timestamp() + (p_lease_milliseconds * interval '1 millisecond'),
        updated_at = statement_timestamp()
    FROM requested
    WHERE tasks.id = requested.task_id
        AND tasks.state = 'running'
        AND tasks.attempt = requested.attempt
        AND tasks.lease_token = requested.lease_token
        AND tasks.cancel_requested_at IS NULL
    RETURNING tasks.id;
$$;

CREATE FUNCTION pgtask.complete_task(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_result jsonb
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH completed AS (
        UPDATE pgtask.tasks
        SET state = 'succeeded',
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = statement_timestamp(),
            updated_at = statement_timestamp(),
            result = p_result,
            error = NULL
        WHERE id = p_task_id AND state = 'running' AND attempt = p_attempt AND lease_token = p_lease_token
        RETURNING id, attempt
    ),
    completed_attempt AS (
        UPDATE pgtask.attempts AS attempts
        SET state = 'succeeded', finished_at = statement_timestamp(), error = NULL
        FROM completed
        WHERE attempts.task_id = completed.id AND attempts.attempt = completed.attempt
    )
    SELECT EXISTS(SELECT 1 FROM completed);
$$;

CREATE FUNCTION pgtask.fail_task(
    p_task_id uuid,
    p_attempt integer,
    p_lease_token uuid,
    p_error jsonb,
    p_retry_milliseconds bigint
)
RETURNS text
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH failed AS (
        UPDATE pgtask.tasks
        SET state = CASE
                WHEN p_retry_milliseconds IS NOT NULL AND attempt < max_attempts THEN 'pending'
                ELSE 'failed'
            END,
            run_at = CASE
                WHEN p_retry_milliseconds IS NOT NULL
                    THEN statement_timestamp() + (p_retry_milliseconds * interval '1 millisecond')
                ELSE run_at
            END,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = CASE
                WHEN p_retry_milliseconds IS NOT NULL AND attempt < max_attempts THEN NULL
                ELSE statement_timestamp()
            END,
            updated_at = statement_timestamp(),
            error = p_error
        WHERE id = p_task_id AND state = 'running' AND attempt = p_attempt AND lease_token = p_lease_token
        RETURNING id, attempt, state
    ),
    failed_attempt AS (
        UPDATE pgtask.attempts AS attempts
        SET state = 'failed', finished_at = statement_timestamp(), error = p_error
        FROM failed
        WHERE attempts.task_id = failed.id AND attempts.attempt = failed.attempt
    )
    SELECT state FROM failed;
$$;

CREATE FUNCTION pgtask.recover_expired(p_queue_name text, p_limit integer)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
    WITH expired AS (
        SELECT tasks.id
        FROM pgtask.tasks
        WHERE tasks.queue_name = p_queue_name
            AND tasks.state = 'running'
            AND tasks.lease_expires_at <= statement_timestamp()
        ORDER BY tasks.lease_expires_at, tasks.id
        FOR NO KEY UPDATE OF tasks SKIP LOCKED
        LIMIT p_limit
    ),
    recovered AS (
        UPDATE pgtask.tasks AS tasks
        SET state = CASE WHEN tasks.attempt < tasks.max_attempts THEN 'pending' ELSE 'failed' END,
            run_at = CASE WHEN tasks.attempt < tasks.max_attempts THEN statement_timestamp() ELSE tasks.run_at END,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = CASE WHEN tasks.attempt < tasks.max_attempts THEN NULL ELSE statement_timestamp() END,
            updated_at = statement_timestamp(),
            error = jsonb_build_object('type', 'lease_expired')
        FROM expired
        WHERE tasks.id = expired.id
        RETURNING tasks.id, tasks.attempt
    ),
    lost_attempts AS (
        UPDATE pgtask.attempts AS attempts
        SET state = 'lost', finished_at = statement_timestamp(), error = jsonb_build_object('type', 'lease_expired')
        FROM recovered
        WHERE attempts.task_id = recovered.id AND attempts.attempt = recovered.attempt
    )
    SELECT count(*) FROM recovered;
$$;

REVOKE ALL ON FUNCTION pgtask.claim(text, uuid, text[], integer[], integer, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.renew_leases(uuid[], integer[], uuid[], bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.complete_task(uuid, integer, uuid, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.fail_task(uuid, integer, uuid, jsonb, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtask.recover_expired(text, integer) FROM PUBLIC;
