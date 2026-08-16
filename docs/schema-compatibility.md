# Schema compatibility

## Apply forward migrations

```console
PGTASK_DATABASE_URL=postgresql://pgtask_owner:secret@postgres/pgtask pgtask migrate
```

Migrations are ordered, immutable, and forward-only. The migrator serializes concurrent runs with a PostgreSQL advisory lock. Never edit a migration that has reached another environment.

## Storage protocol

The database exposes `pgtask.storage_protocol_range()`. Workers and normal SDK clients declare their own inclusive
range. They continue only when the ranges overlap. A worker performs this check before it opens listeners, registers
capabilities, or claims work.

`pgtask.storage_protocol_version()` remains the current protocol identifier for integrations that only need to report
it. Do not use exact equality as a compatibility check.

The storage protocol changes only when a worker cannot safely share the schema with the previous protocol. Additive tables, columns, indexes, views, and functions do not require a protocol change when old workers can ignore them.

## Rolling releases

Use expand-and-contract changes:

1. Add the new shape while keeping the old shape valid.
2. Deploy workers that can use both shapes.
3. Move producers to the new shape.
4. Wait until the oldest supported worker is gone.
5. Remove the old shape in a later release.

One released worker version before and after a migration must be able to run concurrently. If a change cannot preserve that window, stop all workers and producers before migration and treat it as a declared maintenance release.

For an incompatible change, expand the database range before deploying the new clients. Contract the range only after
the old clients are gone. A range such as `1..=2` allows both releases to operate during that interval.

Retry policies are part of a handler version's durable identity. Changing a policy requires a new handler version. The database rejects policy drift under an existing queue, task name, and handler version.

## Rollback

Roll application binaries and Helm configuration back only while the installed schema supports the older worker protocol. Do not reverse migrations. If a new protocol has already committed incompatible tasks, restore the old binary only after a forward compatibility migration makes those tasks safe for it.
