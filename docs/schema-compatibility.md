# Schema compatibility

Migrations are ordered, immutable, and forward-only. The migrator serializes concurrent runs with an advisory lock, so
every process can call it at startup.

```console
PGTASK_DATABASE_URL=postgresql://pgtask_owner:secret@postgres/pgtask pgtask migrate
```

!!! warning "Never edit a migration that has reached another environment"

    The migrator records a checksum. Editing an applied migration makes every database that already ran it refuse to start.

## When the protocol changes

The storage protocol changes **only** when a worker cannot safely share the schema with the previous protocol.

Additive tables, columns, indexes, views, and functions do not require a protocol change, because an old worker can
ignore them. Reserve the protocol bump for changes that would make an old worker behave incorrectly rather than merely
miss a feature.

Check compatibility with the range. Do not compare `pgtask.storage_protocol_version()` for equality - that is the
current identifier, useful for reporting, not a compatibility test.

## Rolling releases

Use expand and contract:

1. Add the new shape while the old shape stays valid.
2. Deploy workers that understand both.
3. Move producers to the new shape.
4. Wait until the oldest supported worker is gone.
5. Remove the old shape in a later release.

The invariant to hold: one released worker version before and after a migration must run concurrently. If a change
cannot preserve that window, stop workers and producers and declare a maintenance release. Discovering that mid-rollout
is considerably worse than planning it.

For an incompatible change, expand the database range **before** deploying new clients, and contract it only after the
old ones are gone. A range like `1..=2` is what keeps both releases working during the overlap.

## Retry policies are part of the identity

A retry policy belongs to a `(queue, task_name, handler_version)`. The database rejects policy drift under an existing
identity, so changing retries requires a new handler version. See [Retries](concepts/retries.md).
