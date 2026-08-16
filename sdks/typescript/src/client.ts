import { context, propagation } from "@opentelemetry/api";
import { Pool, type Notification, type PoolClient, type PoolConfig, type QueryResultRow } from "pg";

import { type EnqueueRequest, type JSONValue, type TaskState } from "./types.js";

const TERMINAL_STATES = new Set<TaskState>(["succeeded", "failed", "cancelled"]);

type EnqueueRow = QueryResultRow & { task_id: string; created: boolean };
type SignalRow = QueryResultRow & { value: JSONValue };
type CancelRow = QueryResultRow & { cancelled: boolean };
type ChannelRow = QueryResultRow & { channel: string };
type ResultRow = QueryResultRow & {
  state: TaskState;
  result: unknown;
  error: JSONValue;
  completed_at: Date | string | null;
};

export type QueryExecutor = Pick<PoolClient, "query">;

export type ClientOptions = Omit<PoolConfig, "connectionString"> & {
  listenerConnectionString?: string;
  listener?: Omit<PoolConfig, "connectionString">;
};

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
  readonly #listenerPool: Pool;
  readonly #resultListener: ResultListener;
  readonly #ownsPool: boolean;

  private constructor(pool: Pool, listenerPool: Pool, ownsPool: boolean) {
    this.#pool = pool;
    this.#listenerPool = listenerPool;
    this.#resultListener = new ResultListener(listenerPool);
    this.#ownsPool = ownsPool;
  }

  static async connect(connectionString: string, options: ClientOptions = {}) {
    const { listenerConnectionString = connectionString, listener = {}, ...queryOptions } = options;
    const pool = new Pool({ ...queryOptions, connectionString });
    const listenerPool = new Pool({
      max: 1,
      ...listener,
      connectionString: listenerConnectionString,
    });
    try {
      await Promise.all([pool.query("SELECT 1"), listenerPool.query("SELECT 1")]);
    } catch (error) {
      await Promise.all([pool.end(), listenerPool.end()]);
      throw error;
    }
    return new Client(pool, listenerPool, true);
  }

  static fromPool(pool: Pool, listenerPool = pool): Client {
    return new Client(pool, listenerPool, false);
  }

  async close(): Promise<void> {
    await this.#resultListener.close();
    if (this.#ownsPool) {
      await Promise.all([this.#pool.end(), this.#listenerPool.end()]);
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
    const channelResult = await this.#pool.query<ChannelRow>(
      "SELECT pgtask.result_channel($1) AS channel",
      [taskId],
    );
    const channel = channelResult.rows[0]?.channel;
    if (channel === undefined || !/^pgtask_result_[0-9]{2}$/.test(channel)) {
      throw new Error("pgtask.result_channel returned an invalid channel");
    }
    const subscription = await this.#resultListener.subscribe(channel, taskId);
    try {
      const current = await readTaskResult<Result>(this.#pool, taskId);
      if (current === null || TERMINAL_STATES.has(current.state)) {
        return current;
      }
      const outcome = await waitForNotification(subscription.notified, options);
      return outcome === "timeout" ? null : readTaskResult<Result>(this.#pool, taskId);
    } finally {
      subscription.unsubscribe();
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

type ResultSubscription = {
  notified: Promise<void>;
  unsubscribe(): void;
};

type ResultWaiter = {
  notify(): void;
  reject(error: Error): void;
};

class ResultListener {
  readonly #pool: Pool;
  readonly #channels = new Set<string>();
  readonly #waiters = new Map<string, Set<ResultWaiter>>();
  #client: PoolClient | undefined;
  #connecting: Promise<PoolClient> | undefined;

  constructor(pool: Pool) {
    this.#pool = pool;
  }

  async subscribe(channel: string, taskId: string): Promise<ResultSubscription> {
    const client = await this.#connect();
    if (!this.#channels.has(channel)) {
      await client.query(`LISTEN ${channel}`);
      this.#channels.add(channel);
    }
    let waiter!: ResultWaiter;
    const notified = new Promise<void>((resolve, reject) => {
      waiter = { notify: resolve, reject };
    });
    const waiters = this.#waiters.get(taskId) ?? new Set();
    waiters.add(waiter);
    this.#waiters.set(taskId, waiters);
    return {
      notified,
      unsubscribe: () => {
        waiters.delete(waiter);
        if (waiters.size === 0) {
          this.#waiters.delete(taskId);
        }
      },
    };
  }

  async close(): Promise<void> {
    const client = this.#client;
    this.#client = undefined;
    this.#channels.clear();
    this.#waiters.clear();
    if (client !== undefined) {
      client.off("notification", this.#onNotification);
      client.off("error", this.#onError);
      await client.query("UNLISTEN *");
      client.release();
    }
  }

  readonly #onNotification = (message: Notification): void => {
    if (message.channel.startsWith("pgtask_result_") && message.payload !== undefined) {
      for (const waiter of this.#waiters.get(message.payload) ?? []) {
        waiter.notify();
      }
    }
  };

  readonly #onError = (error: Error): void => {
    for (const waiters of this.#waiters.values()) {
      for (const waiter of waiters) {
        waiter.reject(error);
      }
    }
    this.#waiters.clear();
    this.#channels.clear();
    const client = this.#client;
    this.#client = undefined;
    if (client !== undefined) {
      client.off("notification", this.#onNotification);
      client.off("error", this.#onError);
      client.release(error);
    }
  };

  async #connect(): Promise<PoolClient> {
    if (this.#client !== undefined) {
      return this.#client;
    }
    this.#connecting ??= this.#pool.connect().then((client) => {
      client.on("notification", this.#onNotification);
      client.on("error", this.#onError);
      this.#client = client;
      return client;
    });
    try {
      return await this.#connecting;
    } finally {
      this.#connecting = undefined;
    }
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
