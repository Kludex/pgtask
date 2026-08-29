# Schema compatibility

Migrations are ordered, immutable, and forward-only. The migrator serializes concurrent runs with an advisory lock, so
every process can call it at startup.

```console
PGTASK_DATABASE_URL=postgresql://pgtask_owner:secret@postgres/pgtask pgtask migrate
```

!!! warning "Never edit a migration that has reached another environment"

    The migrator records a checksum. Editing an applied migration makes every database that already ran it refuse to start.

## When the protocol changes

The storage protocol changes when an old client cannot safely use the new schema or a new client requires a shape the old
schema does not provide.

Additive tables, columns, indexes, views, and functions do not require a protocol change when new clients can run without
them. Raise the new client's minimum when it requires an additive shape, while the database keeps advertising the old
protocol during the rollout.

Check compatibility with the range. Do not compare `pgtask.storage_protocol_version()` for equality - that is the
current identifier, useful for reporting, not a compatibility test.

## What enforces this

The rule above is checked rather than trusted.

`tests/sql_surface_baseline.txt` records the schema as of the oldest storage protocol the release
still supports. A test asserts that every function signature, view, and grant in that baseline still
exists unchanged. Additions pass, because a worker built for the older protocol never calls them.
Removals and changes fail, because that same worker still calls what it always called:

```
the schema is no longer backward compatible with storage protocol 1
  changed function queue_demand(p_queue_name text, ...) -> TABLE(...)
    baseline: grants: pgtask_surface_worker=X/{owner}, {owner}=X/{owner}
    now:      grants: {owner}=X/{owner}
```

Dropping support is deliberate rather than accidental. Raise `STORAGE_PROTOCOL_MIN_VERSION`, then
rerun with `PGTASK_UPDATE_SQL_BASELINE=1` to rebase the baseline on the new minimum.

!!! warning "Structure is checked, meaning is not"

    A function that keeps its signature and changes what it does passes this test. That is exactly
    the change a protocol bump exists for, and it remains a judgement you make rather than one the
    suite makes for you.

Client access goes through functions and views only, so those signatures and their grants are the
whole contract. Table columns are internal and can change freely.

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

For the operational procedure, including what happens to work in flight and how to roll back, see
[Upgrade pgtask](upgrading.md).
