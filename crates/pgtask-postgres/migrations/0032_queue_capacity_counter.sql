ALTER TABLE pgtask.queues
ADD COLUMN capacity_outstanding_tasks bigint NOT NULL DEFAULT 0
CHECK (capacity_outstanding_tasks >= 0);

UPDATE pgtask.queues
SET capacity_outstanding_tasks = counts.outstanding
FROM (
    SELECT tasks.queue_name, count(*) AS outstanding
    FROM pgtask.tasks
    WHERE tasks.state IN ('pending', 'running', 'waiting')
    GROUP BY tasks.queue_name
) AS counts
WHERE queues.name = counts.queue_name
    AND queues.max_outstanding_tasks IS NOT NULL;

CREATE OR REPLACE FUNCTION pgtask.enforce_queue_capacity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    maximum bigint;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.state IN ('pending', 'running', 'waiting')
            AND NEW.state IN ('succeeded', 'failed', 'cancelled')
        THEN
            UPDATE pgtask.queues
            SET capacity_outstanding_tasks = GREATEST(capacity_outstanding_tasks - 1, 0)
            WHERE name = NEW.queue_name AND max_outstanding_tasks IS NOT NULL;
            RETURN NEW;
        END IF;
        IF OLD.state IN ('pending', 'running', 'waiting')
            OR NEW.state IN ('succeeded', 'failed', 'cancelled')
        THEN
            RETURN NEW;
        END IF;
    ELSIF NEW.state IN ('succeeded', 'failed', 'cancelled') THEN
        RETURN NEW;
    END IF;

    UPDATE pgtask.queues
    SET capacity_outstanding_tasks = capacity_outstanding_tasks + 1
    WHERE name = NEW.queue_name
        AND max_outstanding_tasks IS NOT NULL
        AND capacity_outstanding_tasks < max_outstanding_tasks
    RETURNING max_outstanding_tasks INTO maximum;

    IF FOUND THEN
        RETURN NEW;
    END IF;

    SELECT queues.max_outstanding_tasks
    INTO maximum
    FROM pgtask.queues
    WHERE queues.name = NEW.queue_name;

    IF maximum IS NOT NULL THEN
        RAISE EXCEPTION 'queue % has reached its capacity of % outstanding tasks', NEW.queue_name, maximum
            USING ERRCODE = 'PT001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION pgtask.materialize_schedule(
    p_id uuid,
    p_expected_next_run_at timestamptz,
    p_occurrences timestamptz[],
    p_next_run_at timestamptz
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    allowed_count integer;
    capacity_outstanding bigint;
    maximum bigint;
    materialized bigint;
    occurrence_count integer := COALESCE(cardinality(p_occurrences), 0);
    target pgtask.schedules%ROWTYPE;
BEGIN
    SELECT schedules.*
    INTO target
    FROM pgtask.schedules
    WHERE schedules.id = p_id
        AND schedules.paused_at IS NULL
        AND schedules.next_run_at = p_expected_next_run_at
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    SELECT queues.max_outstanding_tasks, queues.capacity_outstanding_tasks
    INTO maximum, capacity_outstanding
    FROM pgtask.queues
    WHERE queues.name = target.queue_name
    FOR UPDATE;

    IF maximum IS NULL THEN
        allowed_count = occurrence_count;
    ELSE
        allowed_count = GREATEST(
            LEAST(occurrence_count::bigint, maximum - capacity_outstanding),
            0
        )::integer;
    END IF;

    UPDATE pgtask.schedules
    SET next_run_at = CASE
            WHEN allowed_count < occurrence_count THEN p_occurrences[allowed_count + 1]
            ELSE p_next_run_at
        END,
        updated_at = statement_timestamp()
    WHERE id = p_id;

    WITH inserted AS (
        INSERT INTO pgtask.tasks (
            id, queue_name, task_name, handler_version, payload, headers, priority, run_at,
            max_attempts, schedule_id, scheduled_for
        )
        SELECT
            gen_random_uuid(), target.queue_name, target.task_name, target.handler_version,
            target.payload, target.headers, target.priority, occurrence, target.max_attempts,
            target.id, occurrence
        FROM unnest(p_occurrences[1:allowed_count]) AS occurrence
        ON CONFLICT (schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL DO NOTHING
        RETURNING id
    )
    SELECT count(*) INTO materialized FROM inserted;

    IF materialized > 0 THEN
        PERFORM pg_notify('pgtask_ready', target.queue_name);
    END IF;
    RETURN materialized;
END;
$$;

CREATE OR REPLACE FUNCTION pgtask.put_queue(
    p_name text,
    p_terminal_retention_seconds bigint,
    p_idempotency_retention_seconds bigint,
    p_max_outstanding_tasks bigint,
    p_starvation_timeout_seconds bigint
)
RETURNS SETOF pgtask.queues
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgtask
AS $$
DECLARE
    outstanding bigint := 0;
BEGIN
    IF p_max_outstanding_tasks IS NOT NULL THEN
        SELECT count(*)
        INTO outstanding
        FROM pgtask.tasks
        WHERE tasks.queue_name = p_name
            AND tasks.state IN ('pending', 'running', 'waiting');
    END IF;

    RETURN QUERY
    INSERT INTO pgtask.queues (
        name,
        terminal_retention_seconds,
        idempotency_retention_seconds,
        max_outstanding_tasks,
        starvation_timeout_seconds,
        capacity_outstanding_tasks
    )
    VALUES (
        p_name,
        p_terminal_retention_seconds,
        p_idempotency_retention_seconds,
        p_max_outstanding_tasks,
        p_starvation_timeout_seconds,
        outstanding
    )
    ON CONFLICT (name) DO UPDATE
    SET terminal_retention_seconds = EXCLUDED.terminal_retention_seconds,
        idempotency_retention_seconds = EXCLUDED.idempotency_retention_seconds,
        max_outstanding_tasks = EXCLUDED.max_outstanding_tasks,
        starvation_timeout_seconds = EXCLUDED.starvation_timeout_seconds,
        capacity_outstanding_tasks = EXCLUDED.capacity_outstanding_tasks,
        updated_at = statement_timestamp()
    RETURNING queues.*;
END;
$$;
