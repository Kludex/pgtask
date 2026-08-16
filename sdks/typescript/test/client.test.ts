import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import test from "node:test";

import { propagation, type TextMapPropagator } from "@opentelemetry/api";
import { Pool, type QueryResult } from "pg";

import {
  Client,
  EnqueueRequest,
  TaskDefinition,
  defineTask,
  type JSONValue,
  type QueryExecutor,
} from "../src/index.js";

type RenderRequest = { reportId: string };
type RenderResult = { rendered: string };

const TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

const propagator: TextMapPropagator = {
  inject(_activeContext, carrier, setter) {
    setter.set(carrier, "traceparent", TRACEPARENT);
  },
  extract(activeContext) {
    return activeContext;
  },
  fields() {
    return ["traceparent"];
  },
};

test("task definitions validate and preserve their typed request contract", () => {
  const definition = defineTask<RenderRequest, RenderResult>("reports.render", {
    queueName: "reports",
    handlerVersion: 2,
  });
  assert.ok(definition instanceof TaskDefinition);
  const runAt = new Date("2026-08-15T12:00:00Z");
  const request = definition.request(
    { reportId: "report-123" },
    {
      runAt,
      priority: 4,
      maxAttempts: 3,
      idempotencyKey: "report-123:v2",
      headers: { source: "test" },
    },
  );
  assert.deepEqual(
    { ...request },
    {
      taskName: "reports.render",
      payload: { reportId: "report-123" },
      queueName: "reports",
      handlerVersion: 2,
      runAt,
      priority: 4,
      maxAttempts: 3,
      idempotencyKey: "report-123:v2",
      headers: { source: "test" },
    },
  );

  const invalidRequests: (() => unknown)[] = [
    () => new EnqueueRequest("", null),
    () => new EnqueueRequest("bad name", null),
    () => new EnqueueRequest("x".repeat(256), null),
    () => new EnqueueRequest("valid", null, { queueName: "" }),
    () => new EnqueueRequest("valid", null, { queueName: "ü" }),
    () => new EnqueueRequest("valid", null, { queueName: "x".repeat(129) }),
    () => new EnqueueRequest("valid", null, { handlerVersion: 0 }),
    () => new EnqueueRequest("valid", null, { handlerVersion: 1.5 }),
    () => new EnqueueRequest("valid", null, { priority: 32_768 }),
    () => new EnqueueRequest("valid", null, { priority: Number.NaN }),
    () => new EnqueueRequest("valid", null, { maxAttempts: 0 }),
    () => new EnqueueRequest("valid", null, { runAt: new Date(Number.NaN) }),
  ];
  for (const invalid of invalidRequests) {
    assert.throws(invalid, TypeError);
  }
});

test("client operations use PostgreSQL transactions, notifications, and trace propagation", async (t) => {
  const databaseUrl = process.env.PGTASK_DATABASE_URL;
  assert.ok(databaseUrl);
  propagation.disable();
  assert.equal(propagation.setGlobalPropagator(propagator), true);
  const pool = new Pool({ connectionString: databaseUrl });
  const listenerPool = new Pool({ connectionString: databaseUrl, max: 1 });
  const client = Client.fromPool(pool, listenerPool);
  t.after(async () => {
    propagation.disable();
    await client.close();
    await listenerPool.end();
    await pool.end();
  });
  await client.close();
  await pool.query("SELECT 1");

  const queueName = `typescript-${randomUUID()}`;
  const definition = defineTask<RenderRequest, RenderResult>("typescript.render", { queueName });
  const request = definition.request(
    { reportId: "report-123" },
    { idempotencyKey: randomUUID(), headers: { source: "typescript", structured: { value: 1 } } },
  );
  const handle = await client.enqueue(request);
  assert.equal(client.task<RenderResult>(handle.id).id, handle.id);
  const pending = await handle.inspect();
  assert.equal(pending?.state, "pending");
  assert.equal(pending?.completedAt, null);
  assert.equal(await handle.result({ timeoutMs: 1 }), null);
  const channel = await pool.query<{ channel: string }>(
    "SELECT pgtask.result_channel($1) AS channel",
    [handle.id],
  );
  await pool.query("SELECT pg_notify($1, $2)", [channel.rows[0]!.channel, randomUUID()]);
  await new Promise((resolve) => setImmediate(resolve));

  const stored = await pool.query<{ headers: Record<string, JSONValue> }>(
    "SELECT headers FROM pgtask.task_view WHERE id = $1",
    [handle.id],
  );
  assert.deepEqual(stored.rows[0]?.headers, {
    source: "typescript",
    structured: { value: 1 },
    traceparent: TRACEPARENT,
  });

  assert.deepEqual(await handle.signal("approval", { approved: true }), { approved: true });
  assert.deepEqual(await handle.signal("approval", { approved: false }), { approved: true });

  const waiting = handle.result({ timeoutMs: 2_000 });
  assert.equal(await handle.cancel(), true);
  const cancelled = await waiting;
  assert.equal(cancelled?.state, "cancelled");
  assert.ok(cancelled?.completedAt instanceof Date);
  assert.equal(await handle.cancel(), false);
  assert.equal(await client.cancel("00000000-0000-0000-0000-000000000000"), false);
  assert.equal(await client.taskResult("00000000-0000-0000-0000-000000000000"), null);
  assert.equal(await client.waitResult("00000000-0000-0000-0000-000000000000"), null);

  const transaction = await pool.connect();
  let rolledBackId: string;
  try {
    await transaction.query("BEGIN");
    const first = await Client.enqueueOn(
      transaction,
      definition.request({ reportId: "rolled-back" }),
    );
    rolledBackId = first.taskId;
    assert.equal(first.created, true);
    await transaction.query("ROLLBACK");
  } finally {
    transaction.release();
  }
  assert.equal(await client.taskResult(rolledBackId), null);

  const idempotencyKey = randomUUID();
  const first = await Client.enqueueOn(
    pool,
    definition.request({ reportId: "one" }, { idempotencyKey }),
  );
  const second = await Client.enqueueOn(
    pool,
    definition.request({ reportId: "two" }, { idempotencyKey }),
  );
  assert.equal(first.created, true);
  assert.deepEqual(second, { taskId: first.taskId, created: false });

  const controller = new AbortController();
  const aborted = client.task(first.taskId).result({ signal: controller.signal });
  setTimeout(() => controller.abort(new Error("stop waiting")), 50);
  await assert.rejects(aborted, /stop waiting/);
  const alreadyAborted = new AbortController();
  alreadyAborted.abort(new Error("already stopped"));
  await assert.rejects(
    client.task(first.taskId).result({ signal: alreadyAborted.signal }),
    /already stopped/,
  );
  await assert.rejects(client.task(first.taskId).result({ timeoutMs: -1 }), /timeoutMs/);
  await assert.rejects(client.task(first.taskId).result({ timeoutMs: Number.NaN }), /timeoutMs/);

  const multiplexed = await Promise.all([
    client.enqueue(definition.request({ reportId: "multiplexed-one" })),
    client.enqueue(definition.request({ reportId: "multiplexed-two" })),
  ]);
  const multiplexedResults = multiplexed.map((task) => task.result({ timeoutMs: 2_000 }));
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(await Promise.all(multiplexed.map((task) => task.cancel())), [true, true]);
  assert.deepEqual(
    (await Promise.all(multiplexedResults)).map((result) => result?.state),
    ["cancelled", "cancelled"],
  );
});

test("owned clients close their pools and failed connections reject", async () => {
  const databaseUrl = process.env.PGTASK_DATABASE_URL;
  assert.ok(databaseUrl);
  const client = await Client.connect(databaseUrl, { max: 2 });
  await client.close();
  await assert.rejects(
    Client.connect("postgresql://127.0.0.1:1/pgtask", { connectionTimeoutMillis: 10 }),
  );
});

test("database boundary failures remain explicit", async () => {
  const emptyExecutor = {
    async query(): Promise<QueryResult<never>> {
      return { rows: [], rowCount: 0, command: "SELECT", oid: 0, fields: [] };
    },
  } as QueryExecutor;
  await assert.rejects(
    Client.enqueueOn(emptyExecutor, new EnqueueRequest("typescript.empty", null)),
    /returned no result/,
  );

  const fakePool = {
    ...emptyExecutor,
    async connect() {
      throw new Error("unused");
    },
    async end() {},
  } as unknown as Pool;
  const client = Client.fromPool(fakePool);
  await assert.rejects(client.emitSignal(randomUUID(), "empty", null), /returned no result/);
  assert.equal(await client.cancel(randomUUID()), false);
  await client.close();

  for (const rows of [[], [{ channel: "invalid" }]]) {
    const invalidConnection = {
      async query(sql: string): Promise<QueryResult> {
        return {
          rows: sql.startsWith("SELECT pgtask.result_channel") ? rows : [],
          rowCount: 0,
          command: "SELECT",
          oid: 0,
          fields: [],
        };
      },
      on() {},
      off() {},
      release() {},
    };
    const invalidChannelPool = {
      query: invalidConnection.query,
      async connect() {
        return invalidConnection;
      },
    } as unknown as Pool;
    await assert.rejects(
      Client.fromPool(invalidChannelPool).waitResult(randomUUID()),
      /invalid channel/,
    );
  }

  const connection = new EventEmitter() as EventEmitter & {
    query(sql: string): Promise<QueryResult>;
    release(error?: Error): void;
  };
  connection.query = async () => ({
    rows: [],
    rowCount: 0,
    command: "LISTEN",
    oid: 0,
    fields: [],
  });
  connection.release = () => {};
  const pendingPool = {
    async query(sql: string): Promise<QueryResult> {
      return {
        rows: sql.startsWith("SELECT pgtask.result_channel")
          ? [{ channel: "pgtask_result_00" }]
          : [{ state: "pending", result: null, error: null, completed_at: null }],
        rowCount: 1,
        command: "SELECT",
        oid: 0,
        fields: [],
      };
    },
  } as unknown as Pool;
  const listenerPool = {
    async connect() {
      return connection;
    },
  } as unknown as Pool;
  const disconnected = Client.fromPool(pendingPool, listenerPool);
  const waiting = disconnected.waitResult(randomUUID());
  await new Promise((resolve) => setImmediate(resolve));
  connection.emit("error", new Error("listener disconnected"));
  await assert.rejects(waiting, /listener disconnected/);
  await disconnected.close();
});
