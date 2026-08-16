# 0005: Require PostgreSQL notifications

Status: accepted

## Decision

Every worker opens a session connection and commits `LISTEN pgtask_ready` before it begins claiming. Transactions that make work ready call `pg_notify`. A low-frequency reconciliation poll remains mandatory because notifications sent while a listener is disconnected are not replayed.

## Consequences

Notifications are the normal dispatch path and polling is only a correctness backstop. Workers require one additional session-capable PostgreSQL connection. Transaction-pooling proxies must provide a direct or session-pooled endpoint for listener connections.
