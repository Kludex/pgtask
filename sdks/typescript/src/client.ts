import { context, propagation } from "@opentelemetry/api";
import { Pool, type Notification, type PoolClient, type PoolConfig, type QueryResultRow } from "pg";

import { type EnqueueRequest, type JSONValue, type TaskState } from "./types.js";

const TERMINAL_STATES = new Set<TaskState>(["succeeded", "failed", "cancelled"]);

type EnqueueRow = QueryResultRow & { task_id: string; created: boolean };
type SignalRow = QueryResultRow & { value: JSONValue };
type CancelRow = QueryResultRow & { cancelled: boolean };
type ResultRow = QueryResultRow & {
  state: TaskState;
  result: unknown;
  error: JSONValue;
  completed_at: Date | string | null;
};

export type QueryExecutor = Pick<PoolClient, "query">;

export type EnqueueResult = {
  taskId: string;
  created: boolean;
};

export type ResultOptions = {
  timeoutMs?: number;
  signal?: AbortSignal;
};

export type TaskResult<Result> = {
  state: TaskState;
  result: Result | null;
  error: JSONValue;
  completedAt: Date | null;
};

export class TaskHandle<Result> {
  readonly id: string;
  readonly #client: Client;

  constructor(id: string, client: Client) {
    this.id = id;
    this.#client = client;
  }

  inspect(): Promise<TaskResult<Result> | null> {
    return this.#client.taskResult<Result>(this.id);
  }

  result(options: ResultOptions = {}): Promise<TaskResult<Result> | null> {
    return this.#client.waitResult<Result>(this.id, options);
  }

  signal(name: string, value: JSONValue, occurrence = 0): Promise<JSONValue> {
    return this.#client.emitSignal(this.id, name, value, occurrence);
  }

  cancel(): Promise<boolean> {
    return this.#client.cancel(this.id);
  }
}

export class Client {
  readonly #pool: Pool;
  readonly #ownsPool: boolean;

  private constructor(pool: Pool, ownsPool: boolean) {
    this.#pool = pool;
    this.#ownsPool = ownsPool;
  }

  static async connect(
    connectionString: string,
    options: Omit<PoolConfig, "connectionString"> = {},
  ) {
    const pool = new Pool({ ...options, connectionString });
    try {
      await pool.query("SELECT 1");
    } catch (error) {
      await pool.end();
      throw error;
    }
    return new Client(pool, true);
  }

  static fromPool(pool: Pool): Client {
    return new Client(pool, false);
  }

  async close(): Promise<void> {
    if (this.#ownsPool) {
      await this.#pool.end();
    }
  }

  async enqueue<Payload, Result>(request: EnqueueRequest<Payload, Result>) {
    const value = await Client.enqueueOn(this.#pool, request);
    return new TaskHandle<Result>(value.taskId, this);
  }

  task<Result = JSONValue>(taskId: string): TaskHandle<Result> {
    return new TaskHandle<Result>(taskId, this);
  }

  static async enqueueOn<Payload, Result>(
    executor: QueryExecutor,
    request: EnqueueRequest<Payload, Result>,
  ) {
    const headers = injectHeaders(request.headers);
    const result = await executor.query<EnqueueRow>(
      `SELECT task_id::text, created
       FROM pgtask.enqueue($1, $2::jsonb, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
      [
        request.taskName,
        JSON.stringify(request.payload),
        request.queueName,
        request.handlerVersion,
        request.runAt,
        request.priority,
        request.maxAttempts,
        request.idempotencyKey,
        JSON.stringify(headers),
      ],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error("pgtask.enqueue returned no result");
    }
    return { taskId: row.task_id, created: row.created } satisfies EnqueueResult;
  }

  async taskResult<Result>(taskId: string): Promise<TaskResult<Result> | null> {
    return readTaskResult<Result>(this.#pool, taskId);
  }

  async waitResult<Result>(
    taskId: string,
    options: ResultOptions = {},
  ): Promise<TaskResult<Result> | null> {
    validateResultOptions(options);
    const connection = await this.#pool.connect();
    let notification: ((value: void) => void) | undefined;
    const notified = new Promise<void>((resolve) => {
      notification = resolve;
    });
    const onNotification = (message: Notification): void => {
      if (message.channel === "pgtask_result" && message.payload === taskId) {
        notification?.();
      }
    };
    connection.on("notification", onNotification);
    try {
      await connection.query("LISTEN pgtask_result");
      const current = await readTaskResult<Result>(connection, taskId);
      if (current === null || TERMINAL_STATES.has(current.state)) {
        return current;
      }
      const outcome = await waitForNotification(notified, options);
      return outcome === "timeout" ? null : readTaskResult<Result>(connection, taskId);
    } finally {
      connection.off("notification", onNotification);
      await connection.query("UNLISTEN pgtask_result");
      connection.release();
    }
  }

  async emitSignal(
    taskId: string,
    name: string,
    value: JSONValue,
    occurrence = 0,
  ): Promise<JSONValue> {
    const result = await this.#pool.query<SignalRow>(
      "SELECT value FROM pgtask.emit_signal($1, $2, $3, $4::jsonb)",
      [taskId, name, occurrence, JSON.stringify(value)],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error("pgtask.emit_signal returned no result");
    }
    return row.value;
  }

  async cancel(taskId: string): Promise<boolean> {
    const result = await this.#pool.query<CancelRow>(
      "SELECT EXISTS(SELECT 1 FROM pgtask.cancel_task($1)) AS cancelled",
      [taskId],
    );
    return result.rows[0]?.cancelled ?? false;
  }
}

function injectHeaders(headers: Record<string, JSONValue>): Record<string, JSONValue> {
  const carrier: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (typeof value === "string") {
      carrier[key] = value;
    }
  }
  propagation.inject(context.active(), carrier);
  return { ...headers, ...carrier };
}

async function readTaskResult<Result>(
  executor: QueryExecutor,
  taskId: string,
): Promise<TaskResult<Result> | null> {
  const response = await executor.query<ResultRow>(
    "SELECT state, result, error, completed_at FROM pgtask.task_result($1)",
    [taskId],
  );
  const row = response.rows[0];
  if (row === undefined) {
    return null;
  }
  return {
    state: row.state,
    result: (row.result ?? null) as Result | null,
    error: row.error,
    completedAt: row.completed_at === null ? null : new Date(row.completed_at),
  };
}

function validateResultOptions(options: ResultOptions): void {
  if (
    options.timeoutMs !== undefined &&
    (!Number.isFinite(options.timeoutMs) || options.timeoutMs < 0)
  ) {
    throw new TypeError("timeoutMs must be a finite non-negative number");
  }
  if (options.signal?.aborted) {
    throw options.signal.reason;
  }
}

async function waitForNotification(
  notified: Promise<void>,
  options: ResultOptions,
): Promise<"notified" | "timeout"> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let abort: (() => void) | undefined;
  const choices: Promise<"notified" | "timeout">[] = [notified.then(() => "notified")];
  if (options.timeoutMs !== undefined) {
    choices.push(
      new Promise((resolve) => {
        timer = setTimeout(() => resolve("timeout"), options.timeoutMs);
      }),
    );
  }
  const signal = options.signal;
  if (signal !== undefined) {
    choices.push(
      new Promise((_, reject) => {
        abort = () => reject(signal.reason);
        signal.addEventListener("abort", abort, { once: true });
        signal.throwIfAborted();
      }),
    );
  }
  try {
    return await Promise.race(choices);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
    if (abort !== undefined) {
      signal!.removeEventListener("abort", abort);
    }
  }
}
