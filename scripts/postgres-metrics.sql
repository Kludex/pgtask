SELECT json_build_object(
    'phase', :'phase',
    'captured_at', statement_timestamp(),
    'wal_bytes', (SELECT wal_bytes::text FROM pg_stat_wal),
    'locks', (
        SELECT count(*)
        FROM pg_locks
        WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
    ),
    'waiting_locks', (
        SELECT count(*)
        FROM pg_locks
        WHERE NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
    ),
    'connections', numbackends,
    'cache_hit_ratio', CASE
        WHEN blks_hit + blks_read = 0 THEN 1
        ELSE blks_hit::double precision / (blks_hit + blks_read)::double precision
    END,
    'transactions_committed', xact_commit,
    'transactions_rolled_back', xact_rollback,
    'temporary_bytes', temp_bytes,
    'deadlocks', deadlocks,
    'database_bytes', pg_database_size(current_database()),
    'task_table_bytes', pg_total_relation_size('pgtask.tasks'),
    'task_index_bytes', pg_indexes_size('pgtask.tasks')
)
FROM pg_stat_database
WHERE datname = current_database();
