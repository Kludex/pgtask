DROP INDEX pgtask.tasks_expired_lease_idx;

CREATE INDEX tasks_expired_lease_idx
    ON pgtask.tasks (queue_name, lease_expires_at, id)
    WHERE state = 'running';
