from __future__ import annotations

import asyncio
import inspect
import os
import traceback
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, cast

import anyio
import pgtask
import pytest
from opentelemetry.context import attach as attach_context, detach as detach_context
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    TraceState,
    get_current_span,
    set_span_in_context,
)
from pgtask import (
    BatchTransactionConnection,
    Client,
    EnqueueRequest,
    JSONValue,
    Task,
    TaskRegistry,
    TransactionConnection,
    Worker,
)
from psycopg import AsyncConnection


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_public_python_contract() -> None:
    assert pgtask.__all__ == [
        "BatchTransactionConnection",
        "Client",
        "EnqueueRequest",
        "JSONValue",
        "NonRetryableError",
        "Task",
        "TaskDefinition",
        "TaskHandle",
        "TaskHandler",
        "TaskRegistry",
        "TaskResult",
        "TaskState",
        "TaskSuspended",
        "TransactionConnection",
        "Worker",
        "get_current_task",
    ]
    assert issubclass(pgtask.TaskSuspended, Exception)
    assert pgtask.TaskSuspended is pgtask._native.TaskSuspended
    assert issubclass(pgtask.NonRetryableError, Exception)
    assert pgtask.NonRetryableError is pgtask._native.NonRetryableError
    assert tuple(inspect.signature(TaskRegistry.task).parameters) == (
        "self",
        "name",
        "handler_version",
        "retry_delay",
    )
    assert tuple(inspect.signature(Client.connect).parameters) == (
        "database_url",
        "listener_url",
        "max_query_connections",
        "max_listener_connections",
    )
    assert tuple(inspect.signature(Worker).parameters) == (
        "database_url",
        "registry",
        "concurrency",
        "poll_interval",
        "lease_duration",
        "health_address",
        "listener_url",
        "max_query_connections",
        "max_listener_connections",
        "retention_enabled",
        "retention_batch_size",
        "retention_interval",
        "shutdown_grace",
    )


@pytest.mark.anyio
async def test_python_worker_executes_a_registered_async_handler() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    queue_name = f"python-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    attempts: list[tuple[int, int]] = []

    @registry.task("python.echo")
    async def echo(task: Task, payload: dict[str, int]) -> JSONValue:
        assert task.queue_name == queue_name
        assert task.task_name == "python.echo"
        assert task.handler_version == 1
        assert task.headers == {}
        assert task.state == "running"
        assert task.max_attempts == 5
        assert task.run_at >= task.created_at
        return {"echo": cast(JSONValue, payload), "attempt": task.attempt}

    @registry.task("python.retry", retry_delay=0.001)
    async def retry(task: Task, payload: dict[str, JSONValue]) -> JSONValue:
        assert payload == {}
        attempts.append((task.attempt, task.failed_attempts))
        if task.attempt == 1:
            raise RuntimeError("retry once")
        return {"attempt": task.attempt}

    worker = Worker(database_url, registry, concurrency=2, poll_interval=30.0)
    task = await client.enqueue(echo.request({"value": 42}))
    retry_task = await client.enqueue(retry.request({}))
    pending = await task.inspect()
    assert pending is not None
    assert pending.completed_at is None
    running = asyncio.create_task(worker.run())
    result = await task.result(timeout=2.0)
    assert result is not None
    assert result.state == "succeeded"
    assert result.result == {"echo": {"value": 42}, "attempt": 1}
    assert result.error is None
    assert result.completed_at is not None
    retried = await retry_task.result(timeout=2.0)
    assert retried is not None
    assert retried.result == {"attempt": 2}
    assert attempts == [(1, 0), (2, 1)]
    worker.shutdown()
    await running


async def wait_for_claim_batches(
    connection: AsyncConnection[Any], task_ids: list[str], expected_claimed: int
) -> list[int]:
    async def poll() -> list[int]:
        while True:
            cursor = await connection.execute(
                """
                SELECT count(*)::integer
                FROM pgtask.attempt_view
                WHERE task_id = ANY(%s::uuid[])
                GROUP BY started_at
                ORDER BY started_at
                """,
                (task_ids,),
            )
            batches = [row[0] for row in await cursor.fetchall()]
            if sum(batches) == expected_claimed:
                return batches
            await asyncio.sleep(0.005)

    return await asyncio.wait_for(poll(), timeout=2)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("concurrency", "task_count", "expected_batches", "pending"),
    [(12, 12, [12], 0), (3, 9, [3], 6)],
)
async def test_python_worker_matches_claim_batches_to_concurrency(
    concurrency: int, task_count: int, expected_batches: list[int], pending: int
) -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    queue_name = f"python-claim-batch-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    release = asyncio.Event()

    @registry.task("python.claim-batch")
    async def block(_task: Task, payload: None) -> None:
        assert payload is None
        await release.wait()

    tasks = [await client.enqueue(block.request(None)) for _ in range(task_count)]
    worker = Worker(database_url, registry, concurrency=concurrency, poll_interval=30.0)
    running = asyncio.create_task(worker.run())
    connection = await AsyncConnection.connect(database_url)
    try:
        assert (
            await wait_for_claim_batches(connection, [task.id for task in tasks], task_count - pending)
            == expected_batches
        )
        cursor = await connection.execute(
            "SELECT count(*)::integer FROM pgtask.task_view WHERE id = ANY(%s::uuid[]) AND state = 'pending'",
            ([task.id for task in tasks],),
        )
        assert (await cursor.fetchone()) == (pending,)
    finally:
        await connection.close()
        worker.shutdown()
        release.set()
        await running


@pytest.mark.anyio
async def test_client_timeout_absence_signal_and_transactional_rollback() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    queue_name = f"python-client-{os.urandom(8).hex()}"
    request: EnqueueRequest[JSONValue] = EnqueueRequest("python.pending", {}, queue_name=queue_name)
    task = await client.enqueue(request)
    batch_requests: list[EnqueueRequest[JSONValue]] = [
        EnqueueRequest("python.pending", {"batch": 1}, queue_name=queue_name, idempotency_key=f"{queue_name}:1"),
        EnqueueRequest("python.pending", {"batch": 2}, queue_name=queue_name, idempotency_key=f"{queue_name}:2"),
    ]
    expected_batch = [await client.enqueue(batch_request) for batch_request in batch_requests]
    batched = await client.enqueue_many(batch_requests)
    assert [handle.id for handle in batched] == [handle.id for handle in expected_batch]
    assert await task.result(timeout=0.001) is None
    assert await client.task("00000000-0000-0000-0000-000000000000").inspect() is None
    assert await task.signal("approval", {"approved": True}) == {"approved": True}

    connection = await AsyncConnection.connect(database_url)
    try:
        await connection.execute("BEGIN")
        rolled_back_id, created = await Client.enqueue_on(cast(TransactionConnection, connection), request)
        assert created
        rolled_back_batch = await Client.enqueue_many_on(
            cast(BatchTransactionConnection, connection),
            [
                EnqueueRequest("python.pending", {"batch": 3}, queue_name=queue_name),
                EnqueueRequest("python.pending", {"batch": 4}, queue_name=queue_name),
            ],
        )
        assert len(rolled_back_batch) == 2
        assert all(created for _, created in rolled_back_batch)
        await connection.rollback()
    finally:
        await connection.close()
    assert await client.task_result(rolled_back_id) is None
    for task_id, _ in rolled_back_batch:
        assert await client.task_result(task_id) is None


class EmptyCursor:
    async def fetchone(self) -> tuple[str, bool] | None:
        return None


class EmptyConnection:
    async def execute(self, query: str, params: tuple[Any, ...]) -> EmptyCursor:
        assert "pgtask.enqueue" in query
        assert params
        return EmptyCursor()


class BatchCursor:
    def __init__(self, rows: list[tuple[int, str, bool]]) -> None:
        self.rows = rows

    async def fetchall(self) -> list[tuple[int, str, bool]]:
        return self.rows


class BatchConnection:
    def __init__(self, rows: list[tuple[int, str, bool]]) -> None:
        self.rows = rows

    async def execute(self, query: str, params: tuple[Any, ...]) -> BatchCursor:
        assert "pgtask.enqueue_many" in query
        assert params
        return BatchCursor(self.rows)


@pytest.mark.anyio
async def test_worker_configuration_rejects_invalid_values() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    with pytest.raises(RuntimeError):
        await Client.connect("not-a-database-url")
    with pytest.raises(RuntimeError):
        await Client.connect(database_url, listener_url="not-a-database-url")
    with pytest.raises(ValueError, match="max_query_connections must be positive"):
        await Client.connect(database_url, max_query_connections=0)
    with pytest.raises(ValueError, match="max_listener_connections must be positive"):
        await Client.connect(database_url, max_listener_connections=0)
    empty = TaskRegistry()
    with pytest.raises(ValueError, match="concurrency must be positive"):
        Worker(database_url, empty, concurrency=0)
    with pytest.raises(ValueError, match="max_listener_connections must be positive"):
        Worker(database_url, empty, max_listener_connections=0)
    with pytest.raises(ValueError, match="queue name must not be empty"):
        Worker(database_url, TaskRegistry(""))
    with pytest.raises(ValueError, match="at least one registry is required"):
        Worker(database_url, [])
    with pytest.raises(ValueError, match="registries must target distinct queues"):
        Worker(database_url, [TaskRegistry("reports"), TaskRegistry("reports")])
    with pytest.raises(ValueError):
        Worker(database_url, empty, poll_interval=float("nan"))
    with pytest.raises(ValueError):
        Worker(database_url, empty, lease_duration=-1)
    with pytest.raises(ValueError):
        Worker(database_url, empty, health_address="not-an-address")
    with pytest.raises(ValueError, match="retention_batch_size must be positive"):
        Worker(database_url, empty, retention_batch_size=0)
    with pytest.raises(ValueError, match="retention_interval must be positive"):
        Worker(database_url, empty, retention_interval=0)
    with pytest.raises(ValueError):
        Worker(database_url, empty, retention_interval=float("nan"))
    with pytest.raises(ValueError):
        Worker(database_url, empty, shutdown_grace=-1)

    async def handler(task: Task, payload: None) -> None:
        assert task
        assert payload is None

    await handler(cast(Task, object()), None)
    invalid_version = TaskRegistry()
    invalid_version.task("invalid.version", handler_version=0)(handler)
    with pytest.raises(ValueError, match="handler_version must be positive"):
        Worker(database_url, invalid_version)

    invalid_name = TaskRegistry()
    invalid_name.task("invalid name")(handler)
    with pytest.raises(ValueError, match="unsupported character"):
        Worker(database_url, invalid_name)

    invalid_retry = TaskRegistry()
    invalid_retry.task("invalid.retry", retry_delay=-1)(handler)
    with pytest.raises(ValueError):
        Worker(database_url, invalid_retry)


@pytest.mark.anyio
async def test_worker_rejects_an_empty_registry_and_invalid_handler_results() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    with pytest.raises(ValueError, match="at least one handler"):
        await Worker(database_url, TaskRegistry()).run()

    client = await Client.connect(database_url)
    registry = TaskRegistry(f"python-invalid-handler-{os.urandom(8).hex()}")

    @registry.task("python.invalid-step", retry_delay=None)
    async def invalid_step(task: Task, payload: None) -> JSONValue:
        assert payload is None
        return await task.step("invalid-operation", cast(Any, 42))

    @registry.task("python.invalid-result", retry_delay=None)
    async def invalid_result(task: Task, payload: None) -> JSONValue:
        assert task
        assert payload is None
        return cast(JSONValue, object())

    @registry.task("python.failed-step", retry_delay=None)
    async def failed_step(task: Task, payload: None) -> JSONValue:
        assert payload is None

        async def fail() -> JSONValue:
            raise RuntimeError("step failed")

        return await task.step("failed-operation", fail)

    step_task = await client.enqueue(invalid_step.request(None, max_attempts=1))
    result_task = await client.enqueue(invalid_result.request(None, max_attempts=1))
    failed_step_task = await client.enqueue(failed_step.request(None, max_attempts=1))
    worker = Worker(database_url, registry, concurrency=3)
    running = asyncio.create_task(worker.run())
    step_failure = await step_task.result(timeout=2)
    assert step_failure is not None
    assert "step operation must be callable" in cast(dict[str, str], step_failure.error)["message"]
    result_failure = await result_task.result(timeout=2)
    assert result_failure is not None
    assert "invalid JSON" in cast(dict[str, str], result_failure.error)["message"]
    failed_step_result = await failed_step_task.result(timeout=2)
    assert failed_step_result is not None
    assert "step failed" in cast(dict[str, str], failed_step_result.error)["message"]
    worker.shutdown()
    await running


@pytest.mark.anyio
async def test_transactional_enqueue_rejects_an_empty_database_response() -> None:
    with pytest.raises(RuntimeError, match="returned no result"):
        await Client.enqueue_on(EmptyConnection(), EnqueueRequest("python.empty", {}))
    with pytest.raises(RuntimeError, match="invalid result set"):
        await Client.enqueue_many_on(BatchConnection([]), [EnqueueRequest("python.empty", {})])
    with pytest.raises(RuntimeError, match="invalid result set"):
        await Client.enqueue_many_on(
            BatchConnection([(1, "00000000-0000-0000-0000-000000000001", True)]),
            [EnqueueRequest("python.empty", {})],
        )


@pytest.mark.anyio
async def test_definitions_cover_defer_deduplication_versions_and_failures() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    queue_name = f"python-definitions-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)

    @registry.task("math.add", handler_version=2)
    async def add(task: Task, payload: dict[str, int]) -> int:
        assert task.handler_version == 2
        assert task.headers["source"] == "test"
        return payload["left"] + payload["right"]

    @registry.task("math.fail", retry_delay=None)
    async def fail(task: Task, payload: None) -> JSONValue:
        assert payload is None
        raise RuntimeError("expected failure")

    with pytest.raises(ValueError, match="already registered"):
        registry.task("math.add", handler_version=2)(add.handler)

    idempotency_key = f"sum-{os.urandom(8).hex()}"
    request = add.request(
        {"left": 20, "right": 22},
        run_at=datetime.now(timezone.utc) + timedelta(milliseconds=1),
        priority=4,
        max_attempts=3,
        idempotency_key=idempotency_key,
        headers={"source": "test"},
    )
    task = await client.enqueue(request)
    duplicate = await client.enqueue(request)
    assert duplicate.id == task.id
    failed = await client.enqueue(fail.request(None))
    worker = Worker(database_url, registry, concurrency=2, poll_interval=30.0)
    running = asyncio.create_task(worker.run())
    result = await task.result(timeout=2)
    assert result is not None
    assert result.state == "succeeded"
    assert result.result == 42
    failure = await failed.result(timeout=2)
    assert failure is not None
    assert failure.state == "failed"
    assert "expected failure" in cast(dict[str, str], failure.error)["message"]
    worker.shutdown()
    await running


class InvalidPayload(pgtask.NonRetryableError):
    pass


@pytest.mark.anyio
async def test_non_retryable_errors_fail_the_task_without_retrying() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    registry = TaskRegistry(f"python-non-retryable-{os.urandom(8).hex()}")
    attempts: dict[str, list[int]] = {}
    surfaced: list[type[BaseException]] = []

    def record(task: Task) -> None:
        attempts.setdefault(task.task_name, []).append(task.attempt)

    @registry.task("non-retryable.direct", retry_delay=0.001)
    async def direct(task: Task, payload: None) -> JSONValue:
        record(task)
        raise pgtask.NonRetryableError("payload is invalid")

    @registry.task("non-retryable.subclass", retry_delay=0.001)
    async def subclass(task: Task, payload: None) -> JSONValue:
        record(task)
        raise InvalidPayload("missing field")

    @registry.task("non-retryable.step", retry_delay=0.001)
    async def in_step(task: Task, payload: None) -> JSONValue:
        record(task)

        async def lookup() -> JSONValue:
            raise InvalidPayload("resource was deleted")

        try:
            return await task.step("lookup", lookup)
        except Exception as error:
            surfaced.append(type(error))
            raise

    @registry.task("non-retryable.cause", retry_delay=0.001)
    async def caused(task: Task, payload: None) -> JSONValue:
        record(task)
        try:
            raise pgtask.NonRetryableError("permission denied")
        except pgtask.NonRetryableError as error:
            raise RuntimeError("could not load the report") from error

    @registry.task("non-retryable.ordinary", retry_delay=0.001)
    async def ordinary(task: Task, payload: None) -> JSONValue:
        record(task)

        async def flaky() -> JSONValue:
            raise RuntimeError("try again")

        return await task.step("flaky", flaky)

    @registry.task("non-retryable.cycle", retry_delay=0.001)
    async def cycle(task: Task, payload: None) -> JSONValue:
        record(task)
        error = RuntimeError("caused by itself")
        raise error from error

    async def run_together(*members: Callable[[], Coroutine[Any, Any, JSONValue]]) -> JSONValue:
        async with anyio.create_task_group() as group:
            for member in members:
                group.start_soon(member)
        raise AssertionError("every member fails, so the group raises")  # pragma: no cover

    async def fail_with(error: Exception) -> JSONValue:
        raise error

    async def caused_failure() -> JSONValue:
        try:
            raise pgtask.NonRetryableError("permission denied")
        except pgtask.NonRetryableError as error:
            raise RuntimeError("could not load the report") from error

    @registry.task("non-retryable.group", retry_delay=0.001)
    async def group(task: Task, payload: None) -> JSONValue:
        record(task)
        return await run_together(
            partial(fail_with, RuntimeError("unrelated")),
            partial(fail_with, InvalidPayload("resource was deleted")),
        )

    @registry.task("non-retryable.nested-group", retry_delay=0.001)
    async def nested_group(task: Task, payload: None) -> JSONValue:
        record(task)
        return await run_together(partial(run_together, caused_failure, partial(fail_with, ValueError("unrelated"))))

    @registry.task("non-retryable.ordinary-group", retry_delay=0.001)
    async def ordinary_group(task: Task, payload: None) -> JSONValue:
        record(task)
        return await run_together(
            partial(fail_with, RuntimeError("try again")), partial(fail_with, ValueError("and again"))
        )

    handles = {
        "non-retryable.direct": await client.enqueue(direct.request(None)),
        "non-retryable.subclass": await client.enqueue(subclass.request(None)),
        "non-retryable.step": await client.enqueue(in_step.request(None)),
        "non-retryable.cause": await client.enqueue(caused.request(None)),
        "non-retryable.ordinary": await client.enqueue(ordinary.request(None, max_attempts=2)),
        "non-retryable.cycle": await client.enqueue(cycle.request(None, max_attempts=2)),
        "non-retryable.group": await client.enqueue(group.request(None, max_attempts=2)),
        "non-retryable.nested-group": await client.enqueue(nested_group.request(None, max_attempts=2)),
        "non-retryable.ordinary-group": await client.enqueue(ordinary_group.request(None, max_attempts=2)),
    }
    worker = Worker(database_url, registry, concurrency=9)
    running = asyncio.create_task(worker.run())
    errors: dict[str, dict[str, JSONValue]] = {}
    for name, handle in handles.items():
        result = await handle.result(timeout=5)
        assert result is not None
        assert result.state == "failed"
        errors[name] = cast(dict[str, JSONValue], result.error)
    worker.shutdown()
    await running

    assert attempts == {
        "non-retryable.direct": [1],
        "non-retryable.subclass": [1],
        "non-retryable.step": [1],
        "non-retryable.cause": [1],
        "non-retryable.ordinary": [1, 2],
        "non-retryable.cycle": [1, 2],
        "non-retryable.group": [1],
        "non-retryable.nested-group": [1],
        "non-retryable.ordinary-group": [1, 2],
    }
    assert surfaced == [InvalidPayload]
    assert errors["non-retryable.direct"] == {
        "type": "handler_error",
        "message": "NonRetryableError: payload is invalid",
        "retryable": False,
    }
    assert errors["non-retryable.subclass"]["message"] == "InvalidPayload: missing field"
    assert errors["non-retryable.step"]["message"] == "InvalidPayload: resource was deleted"
    assert errors["non-retryable.cause"]["message"] == "RuntimeError: could not load the report"
    for name in [
        "non-retryable.subclass",
        "non-retryable.step",
        "non-retryable.cause",
        "non-retryable.group",
        "non-retryable.nested-group",
    ]:
        assert errors[name]["retryable"] is False
    assert errors["non-retryable.ordinary"] == {
        "type": "handler_error",
        "message": "RuntimeError: try again",
        "retryable": True,
    }
    assert errors["non-retryable.cycle"]["retryable"] is True
    assert errors["non-retryable.ordinary-group"]["retryable"] is True


class LookupFailed(Exception):
    def __init__(self, message: str, *, status: int) -> None:
        super().__init__(message)
        self.status = status


@pytest.mark.anyio
async def test_step_raises_the_operations_own_exception() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    registry = TaskRegistry(f"python-step-exception-{os.urandom(8).hex()}")
    root = KeyError("report-123")

    async def lookup() -> JSONValue:
        raise LookupFailed("report is gone", status=404) from root

    @registry.task("step.original", retry_delay=None)
    async def original(task: Task, payload: None) -> JSONValue:
        with pytest.raises(LookupFailed) as caught:
            await task.step("lookup", lookup)
        return {
            "status": caught.value.status,
            "same_cause": caught.value.__cause__ is root,
            "frames": [frame.name for frame in traceback.extract_tb(caught.value.__traceback__)],
        }

    async def suspend() -> JSONValue:
        raise pgtask.TaskSuspended("not a durable call")

    @registry.task("step.suspended", retry_delay=None)
    async def suspended(task: Task, payload: None) -> JSONValue:
        with pytest.raises(RuntimeError) as caught:
            await task.step("suspend", suspend)
        return str(caught.value)

    first = await client.enqueue(original.request(None))
    second = await client.enqueue(suspended.request(None))
    worker = Worker(database_url, registry, concurrency=2)
    running = asyncio.create_task(worker.run())
    outcomes = [await first.result(timeout=5), await second.result(timeout=5)]
    worker.shutdown()
    await running

    assert outcomes[0] is not None
    assert outcomes[0].state == "succeeded"
    result = cast(dict[str, JSONValue], outcomes[0].result)
    assert result["status"] == 404
    assert result["same_cause"] is True
    assert "lookup" in cast(list[str], result["frames"])
    assert outcomes[1] is not None
    assert outcomes[1].result == "TaskSuspended: not a durable call"


@pytest.mark.anyio
async def test_python_handler_receives_trace_context_and_cancellation() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    queue_name = f"python-cancel-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    started = asyncio.Event()
    cancelled = asyncio.Event()
    trace_id = 0x4BF92F3577B34DA6A3CE929D0E0E4736

    @registry.task("python.cancel")
    async def block(task: Task, payload: dict[str, JSONValue]) -> JSONValue:
        assert task.id
        assert payload == {}
        assert get_current_span().get_span_context().trace_id == trace_id
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return None  # pragma: no cover - cancellation is the behavior under test

    worker = Worker(
        database_url,
        registry,
        concurrency=1,
        poll_interval=30.0,
        lease_duration=0.15,
    )
    running = asyncio.create_task(worker.run())
    span = NonRecordingSpan(
        SpanContext(
            trace_id=trace_id,
            span_id=0x00F067AA0BA902B7,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )
    )
    token = attach_context(set_span_in_context(span))
    try:
        task = await client.enqueue(block.request({}))
    finally:
        detach_context(token)
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await task.cancel()
    await asyncio.wait_for(cancelled.wait(), timeout=1)
    result = await task.result(timeout=1)
    assert result is not None
    assert result.state == "cancelled"
    worker.shutdown()
    await running


@pytest.mark.anyio
async def test_python_handler_uses_durable_workflow_operations() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    queue_name = f"python-durable-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    waiting = asyncio.Event()
    step_calls = 0
    runs: list[tuple[int, int]] = []

    @registry.task("python.durable-child")
    async def child(task: Task, payload: dict[str, int]) -> int:
        assert task.attempt == 1
        assert task.parent_task_id is not None
        return payload["value"] * 2

    @registry.task("python.durable-parent")
    async def parent(task: Task, payload: dict[str, int]) -> JSONValue:
        assert task.parent_task_id is None
        runs.append((task.attempt, task.failed_attempts))

        async def checkpointed_value() -> int:
            nonlocal step_calls
            step_calls += 1
            return payload["value"]

        value = await task.step("read-value", checkpointed_value)
        await task.sleep_for("brief-delay", 0.01)
        await task.sleep_until("absolute-delay", datetime.now(timezone.utc) + timedelta(milliseconds=10))
        waiting.set()
        approval = await task.wait_for_signal("approval-wait", "approval", timeout=1.0)
        child_id = await task.spawn("spawn-child", child.request({"value": value}))
        child_result = cast(
            dict[str, JSONValue],
            await task.wait_for_result("wait-for-child", child_id, timeout=1.0),
        )
        return {"approval": approval, "child": child_result}

    worker = Worker(database_url, registry, concurrency=1, poll_interval=30.0)
    running = asyncio.create_task(worker.run())
    task = await client.enqueue(parent.request({"value": 21}))
    await asyncio.wait_for(waiting.wait(), timeout=2)
    assert await task.signal("approval", {"approved": True}) == {"approved": True}
    result = await task.result(timeout=3)
    assert result is not None
    assert result.state == "succeeded"
    assert result.result == {
        "approval": {"approved": True},
        "child": {"error": None, "result": 42, "state": "succeeded"},
    }
    assert step_calls == 1
    # Every resume is a new claim with a higher attempt, and none of them is a failure.
    assert [attempt for attempt, _ in runs] == list(range(1, len(runs) + 1))
    assert len(runs) > 1
    assert {failed for _, failed in runs} == {0}
    worker.shutdown()
    await running


@pytest.mark.anyio
async def test_ambient_task_is_reachable_from_any_frame_below_the_handler() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    queue_name = f"python-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    started = asyncio.Event()
    observed: dict[str, tuple[str, str]] = {}

    async def deep_frame() -> str:
        ambient = pgtask.get_current_task()
        assert ambient is not None
        return ambient.id

    @registry.task("python.ambient")
    async def ambient(task: Task, payload: dict[str, JSONValue]) -> JSONValue:
        if payload["wait"]:
            started.set()
            await asyncio.sleep(0.2)
        observed[task.id] = (await deep_frame(), await task.step("inside-step", deep_frame))
        return {"id": task.id}

    assert pgtask.get_current_task() is None
    worker = Worker(database_url, registry, concurrency=2, poll_interval=30.0)
    slow = await client.enqueue(ambient.request({"wait": True}))
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), timeout=5)
    fast = await client.enqueue(ambient.request({"wait": False}))
    for handle in (fast, slow):
        result = await handle.result(timeout=10)
        assert result is not None
        assert result.state == "succeeded"
    assert observed == {fast.id: (fast.id, fast.id), slow.id: (slow.id, slow.id)}
    assert pgtask.get_current_task() is None
    worker.shutdown()
    await running


@pytest.mark.anyio
@pytest.mark.parametrize("retention_enabled", [True, False])
async def test_python_worker_retention_options_are_honoured(retention_enabled: bool) -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    queue_name = f"python-retention-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    ran: list[str] = []

    @registry.task("python.retained")
    async def retained(task: Task, payload: None) -> None:
        assert payload is None
        ran.append(task.id)

    async with await AsyncConnection.connect(database_url, autocommit=True) as connection:

        async def count(state: str | None = None) -> int:
            cursor = await connection.execute(
                "SELECT count(*)::integer FROM pgtask.task_view WHERE queue_name = %s AND state = COALESCE(%s, state)",
                (queue_name, state),
            )
            row = await cursor.fetchone()
            assert row is not None
            return cast(int, row[0])

        async def until(state: str | None, expected: int) -> None:
            while await count(state) != expected:
                await asyncio.sleep(0.01)

        # Terminal tasks in this queue expire as soon as they finish. Their results are never read,
        # because retention may delete a task before anyone looks at it.
        await connection.execute("SELECT pgtask.put_queue(%s, 0, 0, NULL, 300)", (queue_name,))
        for _ in range(3):
            await client.enqueue(retained.request(None))
        assert await count() == 3
        worker = Worker(
            database_url,
            registry,
            retention_enabled=retention_enabled,
            retention_batch_size=1,
            retention_interval=0.05,
        )
        running = asyncio.create_task(worker.run())
        try:
            if retention_enabled:
                # Batches of one, drained in a tick, remove all three finished tasks.
                await asyncio.wait_for(until(None, 0), timeout=5)
                assert len(ran) == 3
            else:
                await asyncio.wait_for(until("succeeded", 3), timeout=5)
                # Several retention intervals pass and nothing is deleted.
                await asyncio.sleep(0.3)
                assert await count("succeeded") == 3
        finally:
            worker.shutdown()
            await running


@pytest.mark.anyio
@pytest.mark.parametrize("stop", ["shutdown", "cancel"])
async def test_python_worker_releases_unfinished_tasks_after_shutdown_grace(stop: str) -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    queue_name = f"python-release-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    started = asyncio.Event()

    @registry.task("python.slow")
    async def slow(task: Task, payload: None) -> None:
        assert payload is None
        started.set()
        await asyncio.sleep(60)

    handle = await client.enqueue(slow.request(None, max_attempts=1))
    worker = Worker(database_url, registry, lease_duration=60.0, shutdown_grace=0.2)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), timeout=5)
    stopping = asyncio.get_running_loop().time()
    if stop == "shutdown":
        worker.shutdown()
        await asyncio.wait_for(running, timeout=5)
    else:
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(running, timeout=5)
    # The grace period, not the 30 second default, bounded the stop.
    assert asyncio.get_running_loop().time() - stopping < 5

    released = await handle.inspect()
    assert released is not None
    assert released.state == "pending"
    connection = await AsyncConnection.connect(database_url)
    cursor = await connection.execute(
        "SELECT failed_attempts, (SELECT state FROM pgtask.attempt_view WHERE task_id = tasks.id) "
        "FROM pgtask.tasks WHERE id = %s",
        (handle.id,),
    )
    assert await cursor.fetchone() == (0, "released")
    await connection.close()
