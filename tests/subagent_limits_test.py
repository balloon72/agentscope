"""Resource-limit coverage for the resumable subagent bridge."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agentscope._execution_context import SubagentExecutionContext
from agentscope.tool import SubagentTool


class _BlockedSession:
    def __init__(self, _factory: Any) -> None:
        self.finished = False
        self.reply_end_seen = False
        self.release = asyncio.Event()

    async def start(self, _task: str, **_: Any):
        await self.release.wait()
        if False:
            yield None

    async def interrupt(self):
        self.finished = True
        if False:
            yield None


class _ImmediateSession:
    finished = True
    reply_end_seen = True

    def __init__(self, _factory: Any) -> None:
        pass

    async def start(self, _task: str, **_: Any):
        if False:
            yield None

    async def interrupt(self):
        if False:
            yield None


class _ResolverWorker:
    async def reply_stream(self, _inputs: Any):
        if False:
            yield None


class _ClosableResolverWorker(_ResolverWorker):
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_concurrency_limit_is_scoped_to_parent_context() -> None:
    sessions: list[_BlockedSession] = []

    def session_factory(factory: Any) -> _BlockedSession:
        session = _BlockedSession(factory)
        sessions.append(session)
        return session

    bridge = SubagentTool(
        lambda: object(),
        session_factory=session_factory,
        max_concurrent=1,
        cleanup_ttl=None,
    )
    first = asyncio.create_task(
        bridge.run(
            "one",
            context=SubagentExecutionContext("parent-a", ""),
        ),
    )
    second = asyncio.create_task(
        bridge.run(
            "two",
            context=SubagentExecutionContext("parent-b", ""),
        ),
    )
    while len(bridge._runs) < 2:
        await asyncio.sleep(0)
    blocked = await bridge.run(
        "three",
        context=SubagentExecutionContext("parent-a", ""),
    )
    assert blocked["error_code"] == "concurrency_limit"

    first.cancel()
    second.cancel()
    await asyncio.gather(first, second, return_exceptions=True)
    assert bridge._active_count == 0
    assert bridge._active_by_parent == {}


@pytest.mark.asyncio
async def test_cancelled_resolver_releases_slot_for_next_run() -> None:
    resolver_started = asyncio.Event()
    resolver_release = asyncio.Event()
    calls = 0

    async def resolver(_agent_id: str) -> _ResolverWorker:
        nonlocal calls
        calls += 1
        if calls == 1:
            resolver_started.set()
            await resolver_release.wait()
        return _ResolverWorker()

    bridge = SubagentTool(
        agent_resolver=resolver,
        allowed_agent_ids={"specialist"},
        max_concurrent=1,
        session_factory=_ImmediateSession,
        cleanup_ttl=None,
    )
    cancelled = asyncio.create_task(bridge.run("blocked", agent_id="specialist"))
    await resolver_started.wait()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    assert bridge._active_count == 0
    assert bridge._active_by_parent == {}
    result = await bridge.run("retry", agent_id="specialist")
    assert result["status"] == "completed"
    assert bridge._active_count == 0


@pytest.mark.asyncio
async def test_cancelled_resolver_closes_unhanded_worker() -> None:
    worker = _ClosableResolverWorker()
    resolver_returned = asyncio.Event()

    async def resolver(_agent_id: str) -> _ClosableResolverWorker:
        resolver_returned.set()
        return worker

    bridge = SubagentTool(
        agent_resolver=resolver,
        allowed_agent_ids={"specialist"},
        max_concurrent=1,
        session_factory=_ImmediateSession,
        cleanup_ttl=None,
    )
    await bridge._worker_identity_lock.acquire()
    cancelled = asyncio.create_task(bridge.run("blocked", agent_id="specialist"))
    await resolver_returned.wait()
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    bridge._worker_identity_lock.release()

    assert worker.closed
    assert bridge._active_count == 0
