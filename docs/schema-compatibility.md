# Schema compatibility

## Apply forward migrations

```console
PGTASK_DATABASE_URL=postgresql://pgtask_owner:secret@postgres/pgtask pgtask migrate
```

Migrations are ordered, immutable, and forward-only. The migrator serializes concurrent runs with a PostgreSQL advisory lock. Never edit a migration that has reached another environment.

## Storage protocol

The database exposes `pgtask.storage_protocol_version()`. Every worker compares it with its compiled `STORAGE_PROTOCOL_VERSION` before opening listeners, registering capabilities, or claiming work. A mismatch fails startup.

The storage protocol changes only when a worker cannot safely share the schema with the previous protocol. Additive tables, columns, indexes, views, and functions do not require a protocol change when old workers can ignore them.

## Rolling releases

Use expand-and-contract changes:

1. Add the new shape while keeping the old shape valid.
2. Deploy workers that can use both shapes.
3. Move producers to the new shape.
4. Wait until the oldest supported worker is gone.
5. Remove the old shape in a later release.

One released worker version before and after a migration must be able to run concurrently. If a change cannot preserve that window, stop all workers and producers before migration and treat it as a declared maintenance release.

## Rollback

Roll application binaries and Helm configuration back only while the installed schema supports the older worker protocol. Do not reverse migrations. If a new protocol has already committed incompatible tasks, restore the old binary only after a forward compatibility migration makes those tasks safe for it.
