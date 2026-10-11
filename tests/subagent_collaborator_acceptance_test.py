"""Acceptance coverage for ephemeral collaborator subagent runs.

These tests intentionally use a tiny in-process worker instead of a model
provider.  They exercise the public :class:`SubagentTool` protocol end to end:
resolver routing, per-run event queues, HITL resume, cancellation cleanup and
the status contract shared with ``DurableSubagentRunner``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from agentscope.agent._durable_subagent import (
    DurableSubagentRunner,
    RunStatus,
    SQLiteSubagentStore,
)
from agentscope.event import (
    ReplyEndEvent,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.message import Msg, TextBlock
from agentscope.state import AgentState
from agentscope.tool import SubagentTool
from agentscope.types import ReplyFinishedReason


class _Collaborator:
    """Deterministic worker used by both ephemeral and durable runners."""

    def __init__(
        self,
        collaborator_id: str,
        *,
        state: AgentState | None = None,
        require_confirmation: bool = False,
        block_forever: bool = False,
    ) -> None:
        self.collaborator_id = collaborator_id
        self.state = state or AgentState()
        self.require_confirmation = require_confirmation
        self.block_forever = block_forever
        self.closed = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def reply_stream(self, inputs: Any) -> AsyncIterator[Any]:
        self.started.set()
        if isinstance(inputs, Msg):
            self.state.middle_context["task"] = inputs.content[0].text
            if self.block_forever:
                try:
                    await self.release.wait()
                finally:
                    self.closed = True
                return
            if self.require_confirmation:
                yield RequireUserConfirmEvent(
                    reply_id=f"{self.collaborator_id}-reply",
                    tool_calls=[],
                )
                return
            yield Msg(
                name=self.collaborator_id,
                role="assistant",
                content=[TextBlock(text=f"{self.collaborator_id}:done")],
            )
            yield ReplyEndEvent(
                session_id=self.state.session_id,
                reply_id=f"{self.collaborator_id}-reply",
                finished_reason=ReplyFinishedReason.COMPLETED,
            )
            return

        assert isinstance(inputs, UserConfirmResultEvent)
        self.state.middle_context["confirmed"] = True
        yield Msg(
            name=self.collaborator_id,
            role="assistant",
            content=[TextBlock(text=f"{self.collaborator_id}:confirmed")],
        )
        yield ReplyEndEvent(
            session_id=self.state.session_id,
            reply_id=f"{self.collaborator_id}-reply",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )

    async def close(self) -> None:
        self.closed = True
        self.release.set()


def _worker_resolver(
    workers: dict[str, list[_Collaborator]],
    *,
    hitl: set[str] | None = None,
    blocked: set[str] | None = None,
):
    hitl = hitl or set()
    blocked = blocked or set()

    def resolve(agent_id: str) -> _Collaborator:
        worker = _Collaborator(
            agent_id,
            require_confirmation=agent_id in hitl,
            block_forever=agent_id in blocked,
        )
        workers.setdefault(agent_id, []).append(worker)
        return worker

    return resolve


async def _collect_run_events(
    bridge: SubagentTool,
    run_id: str,
    resume_hitl: bool = False,
) -> list[Any]:
    """Consume one run's queue, optionally resuming its HITL request."""
    events: list[Any] = []
    async for wrapped in bridge.events(run_id):
        events.append(wrapped)
        if resume_hitl and wrapped.requires_input:
            await bridge.resume(
                run_id,
                UserConfirmResultEvent(
                    reply_id=wrapped.reply_id,
                    confirm_results=[],
                ),
            )
    return events


@pytest.mark.asyncio
async def test_two_collaborators_and_concurrent_child_isolation() -> None:
    workers: dict[str, list[_Collaborator]] = {}
    bridge = SubagentTool(
        agent_resolver=_worker_resolver(workers),
        allowed_agent_ids={"researcher", "writer"},
        cleanup_ttl=None,
    )

    first = asyncio.create_task(
        bridge.run("research", agent_id="researcher"),
    )
    second = asyncio.create_task(
        bridge.run("write", agent_id="writer"),
    )
    await asyncio.sleep(0)
    run_ids = list(bridge._runs)
    assert len(run_ids) == 2

    results = await asyncio.gather(first, second)
    by_agent = {result["agent_id"]: result for result in results}
    assert set(by_agent) == {"researcher", "writer"}
    assert all(result["status"] == "completed" for result in results)
    assert by_agent["researcher"]["run_id"] != by_agent["writer"]["run_id"]

    # The shared stream carries both children, but keeps their routing tags.
    global_events = [event async for event in bridge.events()]
    assert {event.agent_id for event in global_events} == {
        "researcher",
        "writer",
    }
    assert {event.run_id for event in global_events} == set(run_ids)

    # A run-specific stream never receives a sibling's events.
    for run_id in run_ids:
        events = await _collect_run_events(bridge, run_id)
        assert events
        assert {event.run_id for event in events} == {run_id}
        names = {
            block.text
            for wrapped in events
            if isinstance(wrapped.event, Msg)
            for block in wrapped.event.content
            if isinstance(block, TextBlock)
        }
        assert len(names) == 1
        owner = next(
            agent for agent, result in by_agent.items() if result["run_id"] == run_id
        )
        assert all(owner in text for text in names)

    assert len(workers["researcher"]) == len(workers["writer"]) == 1
    assert all(worker.closed for entries in workers.values() for worker in entries)


@pytest.mark.asyncio
async def test_collaborator_hitl_pause_and_resume_routes_by_run_and_reply() -> None:
    workers: dict[str, list[_Collaborator]] = {}
    bridge = SubagentTool(
        agent_resolver=_worker_resolver(workers, hitl={"reviewer"}),
        allowed_agent_ids={"reviewer"},
        cleanup_ttl=None,
    )
    task = asyncio.create_task(bridge.run("review", agent_id="reviewer"))
    while not bridge._runs:
        await asyncio.sleep(0)
    run_id = next(iter(bridge._runs))

    collector = asyncio.create_task(
        _collect_run_events(bridge, run_id, resume_hitl=False),
    )
    while bridge._runs[run_id].pending is None:
        await asyncio.sleep(0)
    pending = bridge._runs[run_id].pending
    assert pending is not None
    assert isinstance(pending.event, RequireUserConfirmEvent)
    with pytest.raises(ValueError, match="reply_id"):
        await bridge.resume(
            run_id,
            UserConfirmResultEvent(reply_id="wrong", confirm_results=[]),
        )
    await bridge.resume(
        run_id,
        UserConfirmResultEvent(reply_id=pending.reply_id, confirm_results=[]),
    )

    result = await task
    events = await collector
    assert result["status"] == "completed"
    assert any(isinstance(event.event, RequireUserConfirmEvent) for event in events)
    assert any(
        isinstance(event.event, ReplyEndEvent)
        for event in events
    )
    assert workers["reviewer"][0].state.middle_context["confirmed"] is True


@pytest.mark.asyncio
async def test_parent_cancellation_closes_ephemeral_collaborator() -> None:
    workers: dict[str, list[_Collaborator]] = {}
    bridge = SubagentTool(
        agent_resolver=_worker_resolver(workers, blocked={"slow"}),
        allowed_agent_ids={"slow"},
        cleanup_ttl=None,
    )
    task = asyncio.create_task(bridge.run("wait", agent_id="slow"))
    while not bridge._runs:
        await asyncio.sleep(0)
    run_id = next(iter(bridge._runs))
    await workers["slow"][0].started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    result = bridge.get_run(run_id)
    assert result is not None
    assert result["status"] == "cancelled"
    run = bridge._runs[run_id]
    assert run.waiter is None
    assert run.session.finished
    assert workers["slow"][0].closed


@pytest.mark.asyncio
async def test_ephemeral_collaborators_do_not_create_team_storage() -> None:
    workers: dict[str, list[_Collaborator]] = {}
    bridge = SubagentTool(
        agent_resolver=_worker_resolver(workers),
        allowed_agent_ids={"one", "two"},
        cleanup_ttl=None,
    )
    await asyncio.gather(
        bridge.run("one", agent_id="one"),
        bridge.run("two", agent_id="two"),
    )

    # The bridge owns only per-turn worker sessions.  It does not create an
    # Agent Team record or retain a persistent member registry.
    assert not hasattr(bridge, "team_id")
    assert not hasattr(bridge, "team")
    assert all(worker.closed for entries in workers.values() for worker in entries)
    assert all(run.session._worker is None for run in bridge._runs.values())


@pytest.mark.asyncio
async def test_durable_and_ephemeral_runs_share_status_semantics(
    tmp_path: Path,
) -> None:
    workers: dict[str, list[_Collaborator]] = {}
    bridge = SubagentTool(
        agent_resolver=_worker_resolver(workers, hitl={"durable"}),
        allowed_agent_ids={"durable"},
        cleanup_ttl=None,
    )
    ephemeral_task = asyncio.create_task(
        bridge.run("same task", agent_id="durable"),
    )
    while not bridge._runs:
        await asyncio.sleep(0)
    ephemeral_id = next(iter(bridge._runs))
    while bridge._runs[ephemeral_id].pending is None:
        await asyncio.sleep(0)
    pending = bridge._runs[ephemeral_id].pending
    assert pending is not None
    await bridge.resume(
        ephemeral_id,
        UserConfirmResultEvent(reply_id=pending.reply_id, confirm_results=[]),
    )
    ephemeral = await ephemeral_task

    def durable_factory(state: AgentState) -> _Collaborator:
        return _Collaborator("durable", state=state, require_confirmation=True)

    store = SQLiteSubagentStore(str(tmp_path / "runs.sqlite"))
    try:
        runner = DurableSubagentRunner(store, durable_factory, factory_id="acceptance")
        parked = await runner.start("durable-run", "same task", "start")
        assert parked.status is RunStatus.WAITING
        assert parked.pending_reply_id == "durable-reply"
        resumed = await runner.resume(
            "durable-run",
            UserConfirmResultEvent(reply_id="durable-reply", confirm_results=[]),
            "resume",
        )
        assert resumed.status is RunStatus.COMPLETED
        assert ephemeral["status"] == resumed.status.value == "completed"
        assert ephemeral["run_id"] != resumed.run_id
        record = await runner.get("durable-run")
        assert record.status is RunStatus.COMPLETED
        assert record.state.middle_context["confirmed"] is True
    finally:
        await store.aclose()
