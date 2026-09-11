CREATE INDEX workers_heartbeat_idx ON pgtask.workers (heartbeat_at DESC, id DESC);
