# TypeScript SDK

## Enqueue a task

Define the task with its payload and result types, then enqueue it:

```typescript
import { Client, defineTask } from "@pgtask/client";

type RenderRequest = {
  reportId: string;
};

type RenderResult = {
  rendered: string;
};

const render = defineTask<RenderRequest, RenderResult>("reports.render", {
  queueName: "reports",
});

const client = await Client.connect(process.env.PGTASK_DATABASE_URL!, {
  listenerConnectionString: process.env.PGTASK_LISTENER_DATABASE_URL,
});
try {
  const task = await client.enqueue(
    render.request(
      { reportId: "report-123" },
      { idempotencyKey: "report-123:v1" },
    ),
  );
  const result = await task.result({ timeoutMs: 30_000 });
  if (result === null) {
    throw new Error("task was not found or did not finish before the timeout");
  }
  if (result.state !== "succeeded") {
    throw new Error(`task failed: ${JSON.stringify(result.error)}`);
  }
  console.log(result.result?.rendered);
} finally {
  await client.close();
}
```

Install the producer client with `npm install @pgtask/client`. Run migrations with the `pgtask` CLI before enqueueing.
The Rust and Python runtimes execute handlers. The TypeScript package is a typed producer and result client.

`task.result()` subscribes to the task's deterministic result channel before reading task state. One dedicated
PostgreSQL session multiplexes every result wait owned by the client. This ordering prevents a completion from being
lost between subscription and inspection. A transaction-pooling proxy cannot provide this session. Pass
`listenerConnectionString` when the query URL uses transaction pooling.

## Enqueue in a transaction

Pass a `pg` client that is already inside a transaction:

```typescript
import { Client, defineTask } from "@pgtask/client";
import { Pool } from "pg";

type RenderRequest = { reportId: string };
type RenderResult = { rendered: string };

const render = defineTask<RenderRequest, RenderResult>("reports.render", {
  queueName: "reports",
});
const pool = new Pool({ connectionString: process.env.PGTASK_DATABASE_URL });
const connection = await pool.connect();

try {
  await connection.query("BEGIN");
  await connection.query("INSERT INTO reports (id) VALUES ($1)", ["report-123"]);
  await Client.enqueueOn(
    connection,
    render.request(
      { reportId: "report-123" },
      { idempotencyKey: "report-123:v1" },
    ),
  );
  await connection.query("COMMIT");
} catch (error) {
  await connection.query("ROLLBACK");
  throw error;
} finally {
  connection.release();
  await pool.end();
}
```

`Client.enqueueOn()` uses the connection you pass. It does not open another connection or commit for you.
It checks the storage protocol once for each executor. `Client.connect()` also checks compatibility before returning.

## OpenTelemetry

The client injects the active OpenTelemetry context into task headers with the global text-map propagator. Configure an
OpenTelemetry SDK in your application before enqueueing. The package does not install an exporter or choose a telemetry
backend.

## Signals and cancellation

A producer can resolve a waiting task or cancel it:

```typescript
const task = client.task<{ rendered: string }>(taskId);
await task.signal("approval", { approved: true });
await task.cancel();
```

Signals require a producer database role. Cancellation is an administrative operation and requires the pgtask
administrator role.
