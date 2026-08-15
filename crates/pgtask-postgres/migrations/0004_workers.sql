CREATE TABLE pgtask.workers (
    id uuid PRIMARY KEY,
    queue_name text NOT NULL REFERENCES pgtask.queues (name),
    version text NOT NULL,
    draining boolean NOT NULL DEFAULT false,
    started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX workers_expiry_idx ON pgtask.workers (expires_at, id);

CREATE TABLE pgtask.worker_capabilities (
    worker_id uuid NOT NULL REFERENCES pgtask.workers (id) ON DELETE CASCADE,
    task_name text NOT NULL,
    handler_version integer NOT NULL CHECK (handler_version > 0),
    PRIMARY KEY (worker_id, task_name, handler_version)
);
