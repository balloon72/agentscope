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
