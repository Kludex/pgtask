from __future__ import annotations

import asyncio
import inspect
import os
from datetime import datetime, timedelta, timezone
from typing import Any, cast

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
from pgtask import Client, EnqueueRequest, JSONValue, Task, TaskRegistry, TransactionConnection, Worker
from psycopg import AsyncConnection


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_public_python_contract() -> None:
    assert pgtask.__all__ == [
        "Client",
        "EnqueueRequest",
        "JSONValue",
        "Task",
        "TaskDefinition",
        "TaskHandle",
        "TaskHandler",
        "TaskRegistry",
        "TaskResult",
        "TaskState",
        "TransactionConnection",
        "Worker",
        "get_current_task",
    ]
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
    )


@pytest.mark.anyio
async def test_python_worker_executes_a_registered_async_handler() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    queue_name = f"python-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    attempts: list[int] = []

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
        attempts.append(task.attempt)
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
    assert attempts == [1, 2]
    worker.shutdown()
    await running


@pytest.mark.anyio
async def test_python_worker_claims_up_to_its_configured_concurrency() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    queue_name = f"python-claim-batch-{os.urandom(8).hex()}"
    registry = TaskRegistry(queue_name)
    started = asyncio.Event()
    release = asyncio.Event()
    invocations = 0

    @registry.task("python.claim-batch")
    async def block(task: Task, payload: None) -> None:
        nonlocal invocations
        assert task
        assert payload is None
        invocations += 1
        started.set()
        await release.wait()

    for _ in range(12):
        await client.enqueue(block.request(None))
    worker = Worker(database_url, registry, concurrency=12, poll_interval=30.0)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), timeout=2)
    worker.shutdown()
    await asyncio.sleep(0.05)
    release.set()
    await running
    assert invocations == 12


@pytest.mark.anyio
async def test_client_timeout_absence_signal_and_transactional_rollback() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    queue_name = f"python-client-{os.urandom(8).hex()}"
    request: EnqueueRequest[JSONValue] = EnqueueRequest("python.pending", {}, queue_name=queue_name)
    task = await client.enqueue(request)
    assert await task.result(timeout=0.001) is None
    assert await client.task("00000000-0000-0000-0000-000000000000").inspect() is None
    assert await task.signal("approval", {"approved": True}) == {"approved": True}

    connection = await AsyncConnection.connect(database_url)
    try:
        await connection.execute("BEGIN")
        rolled_back_id, created = await Client.enqueue_on(cast(TransactionConnection, connection), request)
        assert created
        await connection.rollback()
    finally:
        await connection.close()
    assert await client.task_result(rolled_back_id) is None


class EmptyCursor:
    async def fetchone(self) -> tuple[str, bool] | None:
        return None


class EmptyConnection:
    async def execute(self, query: str, params: tuple[Any, ...]) -> EmptyCursor:
        assert "pgtask.enqueue" in query
        assert params
        return EmptyCursor()


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

    @registry.task("python.durable-child")
    async def child(task: Task, payload: dict[str, int]) -> int:
        assert task.attempt == 1
        assert task.parent_task_id is not None
        return payload["value"] * 2

    @registry.task("python.durable-parent")
    async def parent(task: Task, payload: dict[str, int]) -> JSONValue:
        assert task.parent_task_id is None

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
