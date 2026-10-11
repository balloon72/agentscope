"""Redis acceptance coverage for :mod:`agentscope.agent._durable_subagent`.

The in-process cases use ``fakeredis`` (and therefore run in the normal unit
test suite).  ``test_real_redis_two_process_handoff_and_fencing`` is an
optional Linux acceptance test: when ``redis-server`` is installed it starts a
throw-away server and exercises the same protocol from two *spawned* Python
processes.  Set ``AGENTSCOPE_REDIS_URL`` to use an already-running Redis
instead.  The test intentionally does not claim Kubernetes coverage; it only
checks the shared Redis/fencing contract used by any deployment.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import shutil
import socket
import subprocess
import time
import types
from collections.abc import Iterator

import fakeredis.aioredis
import pytest
import pytest_asyncio

from agentscope.agent import Agent
from agentscope.agent._durable_subagent import (
    DurableSubagentRunner,
    LostOwnershipError,
    RedisSubagentStore,
    RunBusyError,
    RunStatus,
)
from agentscope.event import (
    ReplyEndEvent,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.message import Msg, ToolCallBlock
from agentscope.state import AgentState
from agentscope.types import ReplyFinishedReason


class _RedisAgent(Agent):
    """A deterministic restartable agent used by the Redis tests."""

    def __init__(self, state: AgentState):
        self.state = state
        self.reply_stream = types.MethodType(_redis_reply_stream, self)


async def _redis_reply_stream(self: _RedisAgent, inputs):
    if isinstance(inputs, Msg):
        self.state.middle_context["started"] = (
            self.state.middle_context.get("started", 0) + 1
        )
        yield RequireUserConfirmEvent(
            reply_id="redis-reply-1",
            tool_calls=[ToolCallBlock(id="redis-call-1", name="confirm", input="{}")],
        )
        return
    self.state.middle_context["resumed"] = (
        self.state.middle_context.get("resumed", 0) + 1
    )
    yield ReplyEndEvent(
        session_id=self.state.session_id,
        reply_id="redis-reply-1",
        finished_reason=ReplyFinishedReason.COMPLETED,
    )


def _redis_factory(state: AgentState) -> _RedisAgent:
    return _RedisAgent(state)


@pytest_asyncio.fixture
async def fake_store() -> Iterator[RedisSubagentStore]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield RedisSubagentStore(client, prefix="tests:durable-redis")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_redis_handoff_and_delivery_replay_are_idempotent(fake_store):
    runner = DurableSubagentRunner(fake_store, _redis_factory, lease_seconds=1)

    parked = await runner.start("r", "task", "delivery-start")
    assert parked.status is RunStatus.WAITING
    assert parked.pending_reply_id == "redis-reply-1"

    duplicate_start = await runner.start("r", "task", "delivery-start")
    assert duplicate_start.duplicate
    assert duplicate_start.events == parked.events

    resumed = await runner.resume(
        "r",
        UserConfirmResultEvent(reply_id="redis-reply-1", confirm_results=[]),
        "delivery-resume",
    )
    assert resumed.status is RunStatus.COMPLETED

    duplicate_resume = await runner.resume(
        "r",
        UserConfirmResultEvent(reply_id="redis-reply-1", confirm_results=[]),
        "delivery-resume",
    )
    assert duplicate_resume.duplicate
    record = await runner.get("r")
    assert record.status is RunStatus.COMPLETED
    assert record.state.middle_context == {"started": 1, "resumed": 1}


@pytest.mark.asyncio
async def test_redis_concurrent_claims_allow_one_worker_only(fake_store):
    async def claim(index: int):
        try:
            return await fake_store.claim(
                "r",
                "task",
                "factory",
                f"delivery-{index}",
                f"worker-{index}",
                60,
            )
        except RunBusyError as exc:
            return exc

    outcomes = await asyncio.gather(*(claim(index) for index in range(12)))
    assert sum(getattr(item, "kind", None) == "claimed" for item in outcomes) == 1
    assert sum(isinstance(item, RunBusyError) for item in outcomes) == 11


@pytest.mark.asyncio
async def test_redis_cancel_fences_active_worker_and_is_terminal(fake_store):
    claimed = await fake_store.claim("r", "task", "factory", "d1", "worker-a", 60)
    cancelled = await fake_store.cancel("r", "parent stopped")
    assert cancelled.status is RunStatus.CANCELLED
    assert (await fake_store.get("r")).status is RunStatus.CANCELLED

    with pytest.raises(LostOwnershipError):
        await fake_store.commit(
            "r",
            claimed.owner,
            claimed.fence,
            claimed.record.state,
            RunStatus.COMPLETED,
            None,
            None,
            [],
            "d1",
        )

    replay = await fake_store.cancel("r", "a different reason")
    assert replay.duplicate
    assert replay.status is RunStatus.CANCELLED
    assert replay.error == "parent stopped"


@pytest.mark.asyncio
async def test_redis_lease_expiry_takeover_increments_fence_and_rejects_late_commit(
    fake_store,
):
    first = await fake_store.claim("r", "task", "factory", "d1", "worker-a", 0.05)
    await asyncio.sleep(0.08)
    second = await fake_store.claim("r", "task", "factory", "d2", "worker-b", 60)
    assert second.fence > first.fence

    with pytest.raises(LostOwnershipError):
        await fake_store.commit(
            "r",
            first.owner,
            first.fence,
            first.record.state,
            RunStatus.FAILED,
            None,
            None,
            [],
            "d1",
            "late worker",
        )

    committed = await fake_store.commit(
        "r",
        second.owner,
        second.fence,
        second.record.state,
        RunStatus.COMPLETED,
        None,
        None,
        [],
        "d2",
    )
    assert committed.status is RunStatus.COMPLETED


@pytest.mark.asyncio
async def test_redis_cancel_after_handoff_does_not_consume_stale_hitl(fake_store):
    runner = DurableSubagentRunner(fake_store, _redis_factory)
    parked = await runner.start("r", "task", "d1")
    assert parked.status is RunStatus.WAITING
    cancelled = await runner.cancel("r", "timeout waiting for parent")
    assert cancelled.status is RunStatus.CANCELLED
    with pytest.raises(ValueError, match="status"):
        await runner.resume(
            "r",
            UserConfirmResultEvent(reply_id="redis-reply-1", confirm_results=[]),
            "d2",
        )
    assert (await runner.get("r")).status is RunStatus.CANCELLED


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def redis_endpoint() -> Iterator[str]:
    """Yield a real Redis endpoint, or skip when this host has none."""

    configured = os.environ.get("AGENTSCOPE_REDIS_URL")
    if configured:
        yield configured
        return
    executable = shutil.which("redis-server")
    if executable is None:
        pytest.skip("redis-server is not installed (set AGENTSCOPE_REDIS_URL to opt in)")
    port = _free_tcp_port()
    process = subprocess.Popen(
        [
            executable,
            "--bind",
            "127.0.0.1",
            "--port",
            str(port),
            "--save",
            "",
            "--appendonly",
            "no",
            "--loglevel",
            "warning",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    endpoint = f"redis://127.0.0.1:{port}/0"
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            if process.poll() is not None:
                pytest.fail("redis-server exited before accepting connections")
            time.sleep(0.05)
    else:
        process.terminate()
        process.wait(timeout=5)
        pytest.fail("redis-server did not start within five seconds")
    try:
        yield endpoint
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _process_redis_start(endpoint: str, prefix: str, queue) -> None:
    """Process A: create and park a HITL run."""

    import redis.asyncio as redis

    async def run() -> None:
        client = redis.Redis.from_url(endpoint, decode_responses=True)
        try:
            store = RedisSubagentStore(client, prefix=prefix)
            result = await DurableSubagentRunner(
                store, _redis_factory, lease_seconds=1
            ).start("run", "task", "d1")
            queue.put((result.status.value, result.pending_reply_id))
        finally:
            await client.aclose()

    asyncio.run(run())


def _process_redis_resume(endpoint: str, prefix: str, queue) -> None:
    """Process B: resume A's parked HITL request without sticky state."""

    import redis.asyncio as redis

    async def run() -> None:
        client = redis.Redis.from_url(endpoint, decode_responses=True)
        try:
            store = RedisSubagentStore(client, prefix=prefix)
            result = await DurableSubagentRunner(store, _redis_factory).resume(
                "run",
                UserConfirmResultEvent(
                    reply_id="redis-reply-1", confirm_results=[]
                ),
                "d2",
            )
            record = await store.get("run")
            queue.put((result.status.value, record.state.middle_context))
        finally:
            await client.aclose()

    asyncio.run(run())


def _process_redis_stale_worker(endpoint: str, prefix: str, ready, control, done) -> None:
    """Hold an expired lease, then attempt a deliberately late commit."""

    import redis.asyncio as redis

    async def run() -> None:
        client = redis.Redis.from_url(endpoint, decode_responses=True)
        try:
            store = RedisSubagentStore(client, prefix=prefix)
            claimed = await store.claim("stale", "task", "factory", "d1", "worker-a", 0.2)
            ready.put(claimed.fence)
            await asyncio.to_thread(control.get)
            try:
                await store.commit(
                    "stale",
                    claimed.owner,
                    claimed.fence,
                    claimed.record.state,
                    RunStatus.FAILED,
                    None,
                    None,
                    [],
                    "d1",
                    "late worker",
                )
            except LostOwnershipError as exc:
                done.put(type(exc).__name__)
            else:
                done.put("committed")
        finally:
            await client.aclose()

    asyncio.run(run())


def _process_redis_takeover(
    endpoint: str, prefix: str, queue, run_id: str = "stale"
) -> None:
    """Process B: take over an expired lease and commit a terminal result."""

    import redis.asyncio as redis

    async def run() -> None:
        client = redis.Redis.from_url(endpoint, decode_responses=True)
        try:
            store = RedisSubagentStore(client, prefix=prefix)
            claimed = await store.claim(run_id, "task", "factory", "d2", "worker-b", 1)
            result = await store.commit(
                run_id,
                claimed.owner,
                claimed.fence,
                claimed.record.state,
                RunStatus.COMPLETED,
                None,
                None,
                [],
                "d2",
            )
            queue.put((result.status.value, claimed.fence))
        finally:
            await client.aclose()

    asyncio.run(run())


def _process_redis_crash_worker(endpoint: str, prefix: str, ready) -> None:
    """Process A: claim a run and block until the parent kills this process."""

    import redis.asyncio as redis

    async def run() -> None:
        client = redis.Redis.from_url(endpoint, decode_responses=True)
        try:
            store = RedisSubagentStore(client, prefix=prefix)
            claimed = await store.claim("crashed", "task", "factory", "d1", "worker-a", 0.2)
            ready.put(claimed.fence)
            await asyncio.sleep(60)
        finally:
            await client.aclose()

    asyncio.run(run())


def test_real_redis_two_process_handoff_and_fencing(redis_endpoint: str) -> None:
    """A/B handoff, lease timeout, and late commit rejection on real Redis."""

    # Importing redis is intentionally local so unit tests remain usable when
    # the optional ``storage-redis`` extra is not installed.
    pytest.importorskip("redis.asyncio")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    prefix = f"tests:durable-acceptance:{os.getpid()}"

    first = ctx.Process(target=_process_redis_start, args=(redis_endpoint, prefix, queue))
    first.start()
    first.join(20)
    assert first.exitcode == 0
    assert queue.get(timeout=2) == (RunStatus.WAITING.value, "redis-reply-1")

    second = ctx.Process(target=_process_redis_resume, args=(redis_endpoint, prefix, queue))
    second.start()
    second.join(20)
    assert second.exitcode == 0
    assert queue.get(timeout=2) == (
        RunStatus.COMPLETED.value,
        {"started": 1, "resumed": 1},
    )

    ready = ctx.Queue()
    control = ctx.Queue()
    stale_result = ctx.Queue()
    stale = ctx.Process(
        target=_process_redis_stale_worker,
        args=(redis_endpoint, prefix, ready, control, stale_result),
    )
    stale.start()
    first_fence = ready.get(timeout=5)
    time.sleep(0.35)  # let worker-a's 200 ms lease expire

    takeover_result = ctx.Queue()
    takeover = ctx.Process(
        target=_process_redis_takeover,
        args=(redis_endpoint, prefix, takeover_result),
    )
    takeover.start()
    takeover.join(20)
    assert takeover.exitcode == 0
    status, second_fence = takeover_result.get(timeout=2)
    assert status == RunStatus.COMPLETED.value
    assert second_fence > first_fence

    control.put("commit")
    stale.join(20)
    assert stale.exitcode == 0
    assert stale_result.get(timeout=2) == "LostOwnershipError"

    # A worker that dies after claiming leaves only an expiring lease in
    # Redis.  A fresh process can take it over and complete the run.
    crash_ready = ctx.Queue()
    crashed = ctx.Process(
        target=_process_redis_crash_worker,
        args=(redis_endpoint, prefix, crash_ready),
    )
    crashed.start()
    # ``spawn`` can take several seconds on a loaded CI worker while the
    # earlier takeover process is being reaped.  This is a readiness bound,
    # not a lease bound; keep it generous so the test does not report a
    # false failure before the child reaches Redis.
    crash_ready.get(timeout=15)
    crashed.terminate()
    crashed.join(10)
    assert crashed.exitcode is not None and crashed.exitcode != 0
    time.sleep(0.35)
    crash_takeover_result = ctx.Queue()
    crash_takeover = ctx.Process(
        target=_process_redis_takeover,
        args=(redis_endpoint, prefix, crash_takeover_result, "crashed"),
    )
    crash_takeover.start()
    crash_takeover.join(20)
    assert crash_takeover.exitcode == 0
    assert crash_takeover_result.get(timeout=2)[0] == RunStatus.COMPLETED.value

