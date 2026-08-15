export type JSONValue =
  | null
  | boolean
  | number
  | string
  | JSONValue[]
  | { [key: string]: JSONValue };

export type TaskState = "pending" | "running" | "waiting" | "succeeded" | "failed" | "cancelled";

export type EnqueueOptions = {
  queueName?: string;
  handlerVersion?: number;
  runAt?: Date;
  priority?: number;
  maxAttempts?: number;
  idempotencyKey?: string;
  headers?: Record<string, JSONValue>;
};

export class EnqueueRequest<Payload, Result> {
  readonly taskName: string;
  readonly payload: Payload;
  readonly queueName: string;
  readonly handlerVersion: number;
  readonly runAt: Date | null;
  readonly priority: number;
  readonly maxAttempts: number;
  readonly idempotencyKey: string | null;
  readonly headers: Record<string, JSONValue>;
  declare readonly __resultType?: Result;

  constructor(taskName: string, payload: Payload, options: EnqueueOptions = {}) {
    validateName("task", taskName, 255);
    const queueName = options.queueName ?? "default";
    validateName("queue", queueName, 128);
    const handlerVersion = options.handlerVersion ?? 1;
    const priority = options.priority ?? 0;
    const maxAttempts = options.maxAttempts ?? 5;
    validatePositiveInteger("handlerVersion", handlerVersion);
    validateInteger("priority", priority, -32_768, 32_767);
    validatePositiveInteger("maxAttempts", maxAttempts);
    if (options.runAt !== undefined && Number.isNaN(options.runAt.getTime())) {
      throw new TypeError("runAt must be a valid Date");
    }
    this.taskName = taskName;
    this.payload = payload;
    this.queueName = queueName;
    this.handlerVersion = handlerVersion;
    this.runAt = options.runAt ?? null;
    this.priority = priority;
    this.maxAttempts = maxAttempts;
    this.idempotencyKey = options.idempotencyKey ?? null;
    this.headers = { ...options.headers };
  }
}

export type TaskOptions = Pick<EnqueueOptions, "queueName" | "handlerVersion">;

export class TaskDefinition<Payload, Result> {
  readonly name: string;
  readonly queueName: string;
  readonly handlerVersion: number;

  constructor(name: string, options: TaskOptions = {}) {
    const request = new EnqueueRequest<null, Result>(name, null, options);
    this.name = request.taskName;
    this.queueName = request.queueName;
    this.handlerVersion = request.handlerVersion;
  }

  request(payload: Payload, options: Omit<EnqueueOptions, "queueName" | "handlerVersion"> = {}) {
    return new EnqueueRequest<Payload, Result>(this.name, payload, {
      ...options,
      queueName: this.queueName,
      handlerVersion: this.handlerVersion,
    });
  }
}

export function defineTask<Payload, Result>(name: string, options: TaskOptions = {}) {
  return new TaskDefinition<Payload, Result>(name, options);
}

function validateName(kind: string, value: string, maximumBytes: number): void {
  if (!value || Buffer.byteLength(value) > maximumBytes || !/^[A-Za-z0-9._:-]+$/.test(value)) {
    throw new TypeError(`${kind} name is invalid`);
  }
}

function validatePositiveInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive integer`);
  }
}

function validateInteger(name: string, value: number, minimum: number, maximum: number): void {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer from ${minimum} to ${maximum}`);
  }
}
