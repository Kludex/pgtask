# pgtask Helm chart

## Install with an existing PostgreSQL secret

```console
kubectl create secret generic pgtask-database \
  --from-literal=url="$PGTASK_DATABASE_URL"

helm upgrade --install pgtask ./charts/pgtask \
  --set image.repository=ghcr.io/kludex/pgtask \
  --set image.tag=0.1.0
```

The chart never stores production PostgreSQL credentials in values. `database.existingSecret.name` and `database.existingSecret.key` select a Secret managed by your deployment system.

Workers also receive `PGTASK_LISTENER_DATABASE_URL`. It uses the main database Secret by default. Set `database.listenerExistingSecret.name` and `database.listenerExistingSecret.key` when session listeners must use a separate PgBouncer session pool or direct PostgreSQL endpoint. Do not route `LISTEN` through transaction pooling.

Every worker replica can run maintenance. PostgreSQL `SKIP LOCKED` claims divide schedule materialization, wait timeout recovery, expired lease recovery, terminal history retention, and idempotency reservation retention without a leader. `workers.<name>.maintenance.retention` controls bounded retention batches for that worker's queue.

Queue admission capacity and starvation timeouts are database configuration, not pod settings. Apply them with the CLI
before scaling producers. The UI shows current outstanding tasks against the configured capacity.

The default external autoscaling metric, `pgtask_queue_ready_tasks`, is the Prometheus-normalized form of the queue-wide
`pgtask.queue.ready.tasks` OpenTelemetry gauge. Configure your metrics adapter to take the maximum across worker
instances. Alert on `pgtask_queue_unroutable_tasks` instead of scaling from it: those tasks require a deployment with the
missing task name and handler version.

The migration Job runs as a `pre-install` and `pre-upgrade` hook. The engine also takes a PostgreSQL advisory lock, so overlapping Helm operations cannot apply migrations concurrently.

The chart does not start a worker by default. A worker image contains your registered handlers. The project image only
provides migrations, administration, the observer UI, and smoke-test binaries.

## Configure queue workers

```yaml
workers:
  email:
    enabled: true
    replicas: 3
    queue: email
    concurrency: 20
    scheduler:
      enabled: true
    drainOnly: false
    health:
      enabled: true
      port: 8081
    image:
      repository: ghcr.io/your-org/email-worker
      tag: latest
    command: ["/app/email-worker"]
    args: []
    terminationGracePeriodSeconds: 60
    env: []
    envFrom: []
    resources:
      requests:
        cpu: 250m
        memory: 256Mi
    startupProbe:
      httpGet:
        path: /livez
        port: health
      failureThreshold: 30
      periodSeconds: 2
    readinessProbe:
      httpGet:
        path: /readyz
        port: health
      periodSeconds: 10
    livenessProbe:
      httpGet:
        path: /livez
        port: health
    podAnnotations: {}
    podLabels: {}
    nodeSelector: {}
    tolerations: []
    affinity: {}
    topologySpreadConstraints: []
    volumes: []
    volumeMounts: []
    extraContainers: []
    podDisruptionBudget:
      enabled: true
      minAvailable: 1
    autoscaling:
      enabled: false
      minReplicas: 1
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
      queueDemand:
        enabled: false
        metricName: pgtask_queue_ready_tasks
        targetAverageValue: "100"
        selectorLabels: {}
```

Each worker requires a session-capable PostgreSQL connection for `LISTEN` and `NOTIFY`. Do not point listener connections at a transaction-pooling endpoint.

When health is enabled, the chart sets `PGTASK_HEALTH_ADDRESS` and uses pod-local HTTP probes. Pass that value to Rust `WorkerConfig.health_address` or Python `Worker(..., health_address=...)`. The chart does not create a Service for worker health ports.

Queue-demand autoscaling is optional. Set `autoscaling.queueDemand.metricName` to an external metric for ready-task count or oldest-ready age. The chart renders a standard HPA metric and does not install KEDA or a metrics adapter. Autoscaling is not part of task correctness. Scheduler-enabled workers cannot scale to zero.

## Enable administrator actions

```yaml
ui:
  enabled: true
  administrator:
    enabled: true
    actorHeader: x-authenticated-user
```

Administrator mode adds audited POST actions for task cancellation, task retry, and schedule pause or resume. Keep it disabled for an observer-only UI. When enabled, place the UI behind an authenticating proxy that removes any client-provided identity header and injects the trusted actor header. Use a database role configured as the `pgtask` administrator.

## Export OpenTelemetry

```console
helm upgrade --install pgtask ./charts/pgtask \
  --set otel.enabled=true \
  --set otel.endpoint=http://otel-collector.observability:4317
```

## Run the development chart

```console
tilt up
```

Tilt builds the project image, installs this chart with `values-development.yaml`, runs migrations, and forwards
PostgreSQL to `localhost:54329`. The development values create an `emptyDir` PostgreSQL deployment. They are only for
local clusters, demos, and chart lifecycle tests. Run `tilt down` to remove the release.

Run the complete local lifecycle test with `./scripts/test-kind-lifecycle.sh`. It builds and loads the local image, installs the chart, drains tasks, performs a rolling restart, force-deletes an active worker, verifies lease recovery, upgrades the release, and rolls it back. The script uses a uniquely named disposable `kind` cluster and removes it on exit.
