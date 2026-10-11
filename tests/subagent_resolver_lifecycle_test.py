"""Resolver lifecycle and Agent-instance isolation tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from agentscope._execution_context import SubagentExecutionContext
from agentscope.event import ReplyEndEvent
from agentscope.message import Msg, TextBlock
from agentscope.tool import SubagentTool
from agentscope.types import ReplyFinishedReason


class _Worker:
    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id

    async def reply_stream(self, _inputs: Any) -> AsyncIterator[Any]:
        await asyncio.sleep(0)
        yield Msg(
            name=self.worker_id,
            role="assistant",
            content=[TextBlock(text="done")],
        )
        yield ReplyEndEvent(
            session_id=f"session-{self.worker_id}",
            reply_id=f"reply-{self.worker_id}",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )


@pytest.mark.asyncio
async def test_resolver_keeps_agent_instance_compatibility_but_rejects_reuse() -> None:
    cached = _Worker("cached")
    bridge = SubagentTool(
        agent_resolver=lambda _agent_id: cached,
        allowed_agent_ids={"specialist"},
        cleanup_ttl=None,
    )

    first = await bridge.run("first", agent_id="specialist")
    second = await bridge.run("second", agent_id="specialist")

    assert first["status"] == "completed"
    assert second["status"] == "error"
    assert second["error_code"] == "agent_instance_reused"
    assert "fresh" in second["error"]


@pytest.mark.asyncio
async def test_cached_agent_is_rejected_when_resolver_calls_overlap() -> None:
    cached = _Worker("cached")

    async def resolver(_agent_id: str) -> _Worker:
        await asyncio.sleep(0)
        return cached

    bridge = SubagentTool(
        agent_resolver=resolver,
        allowed_agent_ids={"specialist"},
        cleanup_ttl=None,
    )
    first, second = await asyncio.gather(
        bridge.run("first", agent_id="specialist"),
        bridge.run("second", agent_id="specialist"),
    )

    assert sorted(result["status"] for result in (first, second)) == [
        "completed",
        "error",
    ]
    error = next(result for result in (first, second) if result["status"] == "error")
    assert error["error_code"] == "agent_instance_reused"


@pytest.mark.asyncio
async def test_reused_workers_do_not_drop_later_live_refs() -> None:
    """A rejected A must not evict B from the identity registry."""
    workers = {"a": _Worker("a"), "b": _Worker("b")}
    bridge = SubagentTool(
        agent_resolver=lambda agent_id: workers[agent_id],
        allowed_agent_ids=set(workers),
        cleanup_ttl=None,
    )

    first_a = await bridge.run("first A", agent_id="a")
    first_b = await bridge.run("first B", agent_id="b")
    reused_a = await bridge.run("reused A", agent_id="a")
    reused_b = await bridge.run("reused B", agent_id="b")

    assert first_a["status"] == "completed"
    assert first_b["status"] == "completed"
    assert reused_a["status"] == "error"
    assert reused_a["error_code"] == "agent_instance_reused"
    assert reused_b["status"] == "error"
    assert reused_b["error_code"] == "agent_instance_reused"


@pytest.mark.asyncio
async def test_run_id_collision_is_reserved_before_blocked_resolver() -> None:
    """A resolver await must not leave a caller-provided ID unclaimed."""
    resolver_entered = asyncio.Event()
    resolver_barrier = asyncio.Event()
    calls = 0
    worker = _Worker("fresh")

    async def resolver(_agent_id: str) -> _Worker:
        nonlocal calls
        calls += 1
        resolver_entered.set()
        await resolver_barrier.wait()
        return worker

    bridge = SubagentTool(
        agent_resolver=resolver,
        allowed_agent_ids={"specialist"},
        cleanup_ttl=None,
    )
    context = SubagentExecutionContext("parent", "same-run")
    first = asyncio.create_task(
        bridge.run("first", agent_id="specialist", context=context),
    )
    await resolver_entered.wait()

    try:
        second = await asyncio.wait_for(
            bridge.run(
                "second",
                agent_id="specialist",
                context=context,
            ),
            timeout=1.0,
        )
    finally:
        # Also unblock the first resolver when this regression fails against
        # an implementation that lets the second call enter it.
        resolver_barrier.set()
    assert second["status"] == "error"
    assert second["error_code"] == "run_id_in_use"
    assert calls == 1

    result = await first
    assert result["status"] == "completed"


@pytest.mark.asyncio
async def test_resolver_fresh_factory_can_run_concurrently() -> None:
    created: list[_Worker] = []

    def resolver(agent_id: str):
        def factory() -> _Worker:
            worker = _Worker(f"{agent_id}-{len(created)}")
            created.append(worker)
            return worker

        return factory

    bridge = SubagentTool(
        agent_resolver=resolver,
        allowed_agent_ids={"specialist"},
        cleanup_ttl=None,
    )
    results = await asyncio.gather(
        bridge.run("first", agent_id="specialist"),
        bridge.run("second", agent_id="specialist"),
    )

    assert [result["status"] for result in results] == ["completed", "completed"]
    assert len(created) == 2

