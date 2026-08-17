# Upgrade pgtask

Upgrading `pgtask` means applying a schema migration to a database that already has work in it, then
replacing workers that are in the middle of running tasks.

Nothing here asks you to drain the queue first. The point of the design is that you do not have to.

## What protects you

Three mechanisms do the work, and knowing which one covers which failure makes the procedure below
obvious rather than memorised.

**Migrations are forward-only and additive within a release window.** A new column, index, or function
does not disturb a worker that has never heard of it. This is what lets two worker versions run at
once, and it is also why rollback works.

**Workers verify the storage protocol before they start.** A worker asks the database for its
inclusive protocol range and refuses to run when it does not overlap its own:

```
database storage protocols 1..=1 are incompatible with client protocols 2..=2
```

That is a worker that will not start, printing both ranges. It is not a worker that starts and
corrupts something.

**Workers never migrate.** Only the migration Job or `pgtask migrate` changes the schema. A worker
that finds an old schema stops; it does not try to fix it.

!!! note "Schema version is not handler version"

    Upgrading `pgtask` does not change your handler versions, so durable workflows keep their
    checkpoints and resume normally on the new binary. The two version numbers are independent, and
    only you change the second one.

## The ordinary upgrade

For an additive migration, which is every upgrade that does not raise the storage protocol:

```console
helm upgrade pgtask ./charts/pgtask --values production.yaml
```

<figure class="deployment" markdown="0">
<svg viewBox="0 0 640 300" role="img" aria-label="A rolling upgrade: the migration Job runs first, then each replica is replaced by a new pod from the v2 ReplicaSet, so v1 and v2 pods claim side by side until the roll finishes.">
  <text class="caption" x="0" y="14">helm upgrade</text>
  <g class="from-12">
    <line class="marker" x1="181" y1="6" x2="181" y2="22" />
    <text class="caption" x="188" y="18">applied</text>
  </g>

  <text class="pod-label" x="0" y="56">Job</text>
  <g class="job-running">
    <rect class="job" x="196" y="40" width="76" height="24" rx="6" />
    <text class="caption" x="204" y="56">migrate</text>
  </g>
  <g class="from-30"><text class="caption" x="280" y="56">schema now serves 1..=2</text></g>

  <text class="pod-label" x="0" y="106">replica 1</text>
  <g class="until-32"><rect class="pod-old" x="120" y="90" width="162" height="24" rx="6" /><text class="caption" x="130" y="106">worker-7d4f9 · v1</text></g>
  <g class="drain-a"><rect class="pod-drain" x="282" y="90" width="40" height="24" rx="6" /></g>
  <g class="from-40"><rect class="pod-new" x="322" y="90" width="304" height="24" rx="6" /><text class="caption" x="332" y="106">worker-b82e1 · v2</text></g>

  <text class="pod-label" x="0" y="146">replica 2</text>
  <g class="until-46"><rect class="pod-old" x="120" y="130" width="233" height="24" rx="6" /><text class="caption" x="130" y="146">worker-7d4f9 · v1</text></g>
  <g class="drain-b"><rect class="pod-drain" x="353" y="130" width="40" height="24" rx="6" /></g>
  <g class="from-54"><rect class="pod-new" x="393" y="130" width="233" height="24" rx="6" /><text class="caption" x="403" y="146">worker-b82e1 · v2</text></g>

  <text class="pod-label" x="0" y="186">replica 3</text>
  <g class="until-60"><rect class="pod-old" x="120" y="170" width="304" height="24" rx="6" /><text class="caption" x="130" y="186">worker-7d4f9 · v1</text></g>
  <g class="drain-c"><rect class="pod-drain" x="424" y="170" width="40" height="24" rx="6" /></g>
  <g class="from-68"><rect class="pod-new" x="464" y="170" width="162" height="24" rx="6" /><text class="caption" x="474" y="186">worker-b82e1 · v2</text></g>

  <g class="overlap">
    <line class="marker" x1="282" y1="200" x2="464" y2="200" />
    <line class="marker" x1="282" y1="196" x2="282" y2="204" />
    <line class="marker" x1="464" y1="196" x2="464" y2="204" />
    <text class="caption" x="288" y="216">v1 and v2 pods both claiming</text>
  </g>

  <text class="pod-label" x="0" y="248">PostgreSQL</text>
  <rect class="database" x="120" y="232" width="506" height="24" rx="6" />
  <text class="caption" x="130" y="248">claims, leases, and completions never stop</text>

  <g class="from-74"><text class="caption" x="494" y="278">upgrade complete</text></g>

  <line class="playhead" x1="120" y1="34" x2="120" y2="264" />
</svg>
<figcaption>Each replica is replaced by a new pod from the v2 ReplicaSet, so both versions claim at once until the roll finishes. A dashed pod is draining: it finishes its handlers and claims nothing new, and whatever it cannot finish returns through lease expiry.</figcaption>
</figure>

The chart runs the migration as a `pre-upgrade` hook with weight `-10`, so the schema is migrated
before any worker Deployment rolls. Then Kubernetes replaces workers one at a time.

A replica is not upgraded in place. Kubernetes starts a pod from the new ReplicaSet and terminates
the old one, so for most of the roll **both versions are live and both are claiming from the same
queue**. That is the state the migration has to be safe for, and it is why the schema is migrated
first and why the change has to be additive: the v1 pod still running has to keep working against a
schema that v2 also understands.

Without Helm, the same order applies. Run `pgtask migrate`, wait for it to finish, then deploy
workers.

`migrate` takes an advisory lock, so running it from ten places at once applies the schema once and
the rest wait.

## What happens to work in flight

Each worker that gets replaced goes through the same path, and none of it is special to upgrades.

`SIGTERM` stops claim admission first, so the worker takes no new work. Handlers already running get
until the grace period to finish, and anything still running is aborted. Those tasks keep their rows,
their leases expire, and another worker claims them as a new attempt.

That is the ordinary at-least-once path, which is why an upgrade needs no special handling: a
replaced worker looks exactly like a crashed one, and the engine already handles a crashed one.

Suspended workflows are unaffected. A task sleeping for six hours is a row with a deadline, so it
does not care that every worker was replaced while it waited.

!!! warning "Handlers must still be safe to run again"

    A rolling upgrade increases how often a handler is interrupted mid-flight. If your handler is not
    idempotent, an upgrade is when you find out.

## When the storage protocol changes

A protocol bump means an old worker cannot safely share the schema with a new one. Expand first, and
contract only after the old workers are gone:

1. Release a schema whose range covers both, such as `1..=2`.
2. Migrate. Both worker versions now pass the handshake.
3. Roll workers to the new version.
4. Confirm no old workers remain, using `pgtask.worker_view`.
5. Contract the range to `2..=2` in a later release.

Skipping step 1 makes the upgrade a stop-the-world change. Old workers fail the handshake the moment
the migration lands, and they stay down until you deploy the new version.

Check what the database currently offers at any time:

```sql
SELECT minimum, maximum FROM pgtask.storage_protocol_range();
```

## Rolling back

You roll back workers. You do not roll back the schema.

Migrations are forward-only, and the reason rollback still works is that an additive schema is
compatible with the previous worker. The extra column is simply unused.

Route producers back to the old version first, keep the new workers running until their queues are
empty, then remove them. Reversing that order strands committed tasks in a queue nobody is serving.

!!! warning "A contracted protocol cannot be rolled back into"

    Once you remove the old shape in a contract step, the previous worker version can no longer run.
    Keep the expanded range in place for at least one release longer than you expect to need it.

## Verifying an upgrade

Four checks, in the order they fail:

```sql
-- 1. The schema is at the range you expect.
SELECT minimum, maximum FROM pgtask.storage_protocol_range();

-- 2. Every live worker registered, so none are stuck on the handshake.
SELECT version, count(*) FROM pgtask.worker_view WHERE live GROUP BY version;

-- 3. Nothing is unroutable, so no queue lost its handler.
SELECT name, ready_count, unroutable_count FROM pgtask.queue_overview
 WHERE unroutable_count > 0;

-- 4. Nothing is stuck running with an expired lease.
SELECT count(*) FROM pgtask.task_view
 WHERE state = 'running' AND lease_expires_at < now();
```

A worker that fails the handshake never appears in `worker_view`, so an upgrade that halved your
worker count shows up in check 2 rather than as silence.

See [Schema compatibility](schema-compatibility.md) for the rules a migration must follow, and
[Operations](operations.md) for draining and incident response.
