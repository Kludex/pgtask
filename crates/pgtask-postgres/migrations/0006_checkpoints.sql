CREATE TABLE pgtask.checkpoints (
    task_id uuid NOT NULL REFERENCES pgtask.tasks (id) ON DELETE CASCADE,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    step_name text NOT NULL CHECK (
        step_name <> ''
        AND octet_length(step_name) <= 255
        AND step_name ~ '^[A-Za-z0-9._:-]+$'
    ),
    occurrence integer NOT NULL CHECK (occurrence >= 0),
    value jsonb NOT NULL CHECK (octet_length(value::text) <= 1048576),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (task_id, handler_version, step_name, occurrence)
);
