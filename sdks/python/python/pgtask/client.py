from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generic, TypeVar, cast

from opentelemetry.context import attach, detach
from opentelemetry.propagate import extract, inject
from typing_extensions import Literal, Protocol, TypeAlias  # noqa: UP035

from pgtask import _native

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
TaskState: TypeAlias = Literal["pending", "running", "waiting", "succeeded", "failed", "cancelled"]
PayloadT = TypeVar("PayloadT")
ResultT = TypeVar("ResultT")
StepT = TypeVar("StepT")
TaskHandler: TypeAlias = Callable[["Task", PayloadT], Awaitable[ResultT]]


class TransactionCursor(Protocol):
    async def fetchone(self) -> tuple[str, bool] | None: ...


class TransactionConnection(Protocol):
    async def execute(self, query: str, params: tuple[Any, ...]) -> TransactionCursor: ...


@dataclass(frozen=True)
class EnqueueRequest(Generic[ResultT]):
    task_name: str
    payload: JSONValue
    queue_name: str = "default"
    handler_version: int = 1
    run_at: datetime | None = None
    priority: int = 0
    max_attempts: int = 5
    idempotency_key: str | None = None
    headers: dict[str, JSONValue] = field(default_factory=dict)


def _request_value(request: EnqueueRequest[Any]) -> dict[str, Any]:
    headers = dict(request.headers)
    inject(cast(dict[str, str], headers))
    return {
        "task_name": request.task_name,
        "payload": request.payload,
        "queue_name": request.queue_name,
        "handler_version": request.handler_version,
        "run_at": request.run_at.isoformat() if request.run_at is not None else None,
        "priority": request.priority,
        "max_attempts": request.max_attempts,
        "idempotency_key": request.idempotency_key,
        "headers": headers,
    }


@dataclass(frozen=True)
class TaskDefinition(Generic[PayloadT, ResultT]):
    name: str
    queue_name: str
    handler: TaskHandler[PayloadT, ResultT] = field(repr=False)
    handler_version: int = 1
    retry_delay: float | None = 1.0

    def request(
        self,
        payload: PayloadT,
        *,
        run_at: datetime | None = None,
        priority: int = 0,
        max_attempts: int = 5,
        idempotency_key: str | None = None,
        headers: dict[str, JSONValue] | None = None,
    ) -> EnqueueRequest[ResultT]:
        return EnqueueRequest(
            task_name=self.name,
            payload=cast(JSONValue, payload),
            queue_name=self.queue_name,
            handler_version=self.handler_version,
            run_at=run_at,
            priority=priority,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            headers={} if headers is None else headers,
        )


class TaskRegistry:
    def __init__(self, queue_name: str = "default") -> None:
        self.queue_name = queue_name
        self._definitions: dict[tuple[str, int], TaskDefinition[Any, Any]] = {}

    def task(
        self,
        name: str,
        *,
        handler_version: int = 1,
        retry_delay: float | None = 1.0,
    ) -> Callable[[TaskHandler[PayloadT, ResultT]], TaskDefinition[PayloadT, ResultT]]:
        def decorator(handler: TaskHandler[PayloadT, ResultT]) -> TaskDefinition[PayloadT, ResultT]:
            definition = TaskDefinition(name, self.queue_name, handler, handler_version, retry_delay)
            key = (definition.name, definition.handler_version)
            if key in self._definitions:
                raise ValueError(f"task {definition.name!r} version {definition.handler_version} is already registered")
            self._definitions[key] = definition
            return definition

        return decorator

    @property
    def definitions(self) -> tuple[TaskDefinition[Any, Any], ...]:
        return tuple(self._definitions.values())


@dataclass(frozen=True)
class TaskResult(Generic[ResultT]):
    state: TaskState
    result: ResultT | None
    error: JSONValue
    completed_at: datetime | None

    @classmethod
    def from_native(cls, value: dict[str, Any]) -> TaskResult[ResultT]:
        completed_at = value["completed_at"]
        return cls(
            state=cast(TaskState, value["state"]),
            result=cast(ResultT | None, value["result"]),
            error=value["error"],
            completed_at=datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            if completed_at is not None
            else None,
        )


@dataclass(frozen=True)
class Task:
    id: str
    parent_task_id: str | None
    queue_name: str
    task_name: str
    handler_version: int
    payload: JSONValue
    headers: dict[str, JSONValue]
    state: TaskState
    attempt: int
    max_attempts: int
    run_at: datetime
    created_at: datetime
    _context: _native.TaskContext = field(repr=False, compare=False)

    @classmethod
    def from_native(cls, value: dict[str, Any], context: _native.TaskContext) -> Task:
        return cls(
            id=str(value["id"]),
            parent_task_id=str(value["parent_task_id"]) if value["parent_task_id"] is not None else None,
            queue_name=str(value["queue_name"]),
            task_name=str(value["task_name"]),
            handler_version=int(value["handler_version"]),
            payload=cast(JSONValue, value["payload"]),
            headers=cast(dict[str, JSONValue], value["headers"]),
            state=cast(TaskState, value["state"]),
            attempt=int(value["attempt"]),
            max_attempts=int(value["max_attempts"]),
            run_at=datetime.fromisoformat(value["run_at"].replace("Z", "+00:00")),
            created_at=datetime.fromisoformat(value["created_at"].replace("Z", "+00:00")),
            _context=context,
        )

    async def step(self, name: str, operation: Callable[[], Awaitable[StepT]], occurrence: int = 0) -> StepT:
        return cast(StepT, await self._context.step(name, occurrence, operation))

    async def sleep_for(self, name: str, seconds: float, occurrence: int = 0) -> None:
        await self._context.sleep_for(name, occurrence, seconds)

    async def sleep_until(self, name: str, wake_at: datetime, occurrence: int = 0) -> None:
        await self._context.sleep_until(name, occurrence, wake_at.isoformat())

    async def wait_for_signal(
        self,
        step_name: str,
        signal_name: str,
        *,
        occurrence: int = 0,
        signal_occurrence: int = 0,
        timeout: float | None = None,
    ) -> JSONValue:
        return cast(
            JSONValue,
            await self._context.wait_for_signal(
                step_name,
                occurrence,
                signal_name,
                signal_occurrence,
                timeout,
            ),
        )

    async def spawn(self, step_name: str, request: EnqueueRequest[Any], occurrence: int = 0) -> str:
        return await self._context.spawn(step_name, occurrence, _request_value(request))

    async def wait_for_result(
        self,
        step_name: str,
        task_id: str,
        *,
        occurrence: int = 0,
        timeout: float | None = None,
    ) -> JSONValue:
        return cast(JSONValue, await self._context.wait_for_result(step_name, occurrence, task_id, timeout))


_current_task: ContextVar[Task | None] = ContextVar("pgtask_current_task", default=None)


def get_current_task() -> Task | None:
    """The task running in this call chain, or `None` outside a handler.

    Reach for this when a frame between your handler and the code that needs the task cannot pass it
    down, which is the usual shape when a framework calls you back.
    """
    return _current_task.get()


@dataclass(frozen=True)
class TaskHandle(Generic[ResultT]):
    id: str
    _client: Client = field(repr=False, compare=False)

    async def inspect(self) -> TaskResult[ResultT] | None:
        return await self._client.task_result(self.id)

    async def result(self, timeout: float | None = None) -> TaskResult[ResultT] | None:
        return await self._client.wait_result(self.id, timeout)

    async def signal(self, name: str, value: JSONValue, occurrence: int = 0) -> JSONValue:
        return await self._client.emit_signal(self.id, name, value, occurrence)

    async def cancel(self) -> bool:
        return await self._client.cancel(self.id)


class Client:
    def __init__(self, native: _native.Client) -> None:
        self._native = native

    @classmethod
    async def connect(
        cls,
        database_url: str,
        *,
        listener_url: str | None = None,
        max_query_connections: int = 10,
        max_listener_connections: int = 1,
    ) -> Client:
        return cls(
            await _native.Client.connect(
                database_url,
                listener_url=listener_url,
                max_query_connections=max_query_connections,
                max_listener_connections=max_listener_connections,
            )
        )

    async def migrate(self) -> None:
        await self._native.migrate()

    async def enqueue(self, request: EnqueueRequest[ResultT]) -> TaskHandle[ResultT]:
        task_id, _ = await self._enqueue(request)
        return TaskHandle(task_id, self)

    def task(self, task_id: str) -> TaskHandle[JSONValue]:
        return TaskHandle(task_id, self)

    async def _enqueue(self, request: EnqueueRequest[Any]) -> tuple[str, bool]:
        return await self._native.enqueue(_request_value(request))

    async def task_result(self, task_id: str) -> TaskResult[Any] | None:
        result = await self._native.task_result(task_id)
        return TaskResult.from_native(result) if result is not None else None

    async def wait_result(self, task_id: str, timeout: float | None = None) -> TaskResult[Any] | None:
        result = await self._native.wait_result(task_id, timeout)
        return TaskResult.from_native(result) if result is not None else None

    async def emit_signal(self, task_id: str, name: str, value: JSONValue, occurrence: int = 0) -> JSONValue:
        return cast(JSONValue, await self._native.emit_signal(task_id, name, occurrence, value))

    async def cancel(self, task_id: str) -> bool:
        return await self._native.cancel(task_id)

    @staticmethod
    async def enqueue_on(connection: TransactionConnection, request: EnqueueRequest[Any]) -> tuple[str, bool]:
        from psycopg.types.json import Jsonb

        cursor = await connection.execute(
            """
            SELECT task_id::text, created
            FROM pgtask.enqueue(%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                request.task_name,
                Jsonb(request.payload),
                request.queue_name,
                request.handler_version,
                request.run_at,
                request.priority,
                request.max_attempts,
                request.idempotency_key,
                Jsonb(request.headers),
            ),
        )
        row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("pgtask.enqueue returned no result")
        return row


class Worker:
    def __init__(
        self,
        database_url: str,
        registry: TaskRegistry | Sequence[TaskRegistry],
        *,
        concurrency: int = 10,
        poll_interval: float = 30.0,
        lease_duration: float = 30.0,
        health_address: str | None = None,
        listener_url: str | None = None,
        max_query_connections: int = 10,
        max_listener_connections: int = 1,
    ) -> None:
        registries = [registry] if isinstance(registry, TaskRegistry) else list(registry)
        if not registries:
            raise ValueError("at least one registry is required")
        queue_names = [entry.queue_name for entry in registries]
        if len(set(queue_names)) != len(queue_names):
            raise ValueError("registries must target distinct queues")
        self._native = _native.Worker(
            database_url,
            queue_names,
            {
                "concurrency": concurrency,
                "poll_interval": poll_interval,
                "lease_duration": lease_duration,
                "health_address": health_address,
                "listener_url": listener_url,
                "max_query_connections": max_query_connections,
                "max_listener_connections": max_listener_connections,
            },
        )
        definitions = [definition for entry in registries for definition in entry.definitions]
        for definition in definitions:

            async def adapter(
                value: dict[str, Any],
                context: _native.TaskContext,
                registered: TaskDefinition[Any, Any] = definition,
            ) -> JSONValue:
                task = Task.from_native(value, context)
                token = attach(extract(cast(dict[str, str], task.headers)))
                current = _current_task.set(task)
                try:
                    return cast(JSONValue, await registered.handler(task, task.payload))
                finally:
                    _current_task.reset(current)
                    detach(token)

            self._native.register(
                definition.name,
                adapter,
                definition.handler_version,
                definition.retry_delay,
            )

    async def run(self) -> None:
        await self._native.run()

    def shutdown(self) -> None:
        self._native.shutdown()
