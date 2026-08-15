CREATE TABLE pgtask.queues (
    name text PRIMARY KEY,
    terminal_retention_seconds bigint NOT NULL DEFAULT 604800 CHECK (terminal_retention_seconds >= 0),
    paused_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK (name <> '' AND octet_length(name) <= 128 AND name ~ '^[A-Za-z0-9._:-]+$')
);

INSERT INTO pgtask.queues (name)
SELECT DISTINCT queue_name
FROM pgtask.tasks
ON CONFLICT DO NOTHING;

CREATE FUNCTION pgtask.ensure_queue_for_task()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO pgtask.queues (name)
    VALUES (NEW.queue_name)
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ensure_queue_for_task
BEFORE INSERT ON pgtask.tasks
FOR EACH ROW
EXECUTE FUNCTION pgtask.ensure_queue_for_task();

ALTER TABLE pgtask.tasks
ADD CONSTRAINT tasks_queue_name_fkey
FOREIGN KEY (queue_name) REFERENCES pgtask.queues (name);
