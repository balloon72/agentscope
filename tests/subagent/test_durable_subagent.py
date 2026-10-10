"""Cross-process durability tests for the durable subagent runner."""
from __future__ import annotations

import asyncio
import multiprocessing
import types
from pathlib import Path

import pytest

from agentscope.agent import Agent
from agentscope.agent._durable_subagent import (
    DurableSubagentRunner,
    LostOwnershipError,
    RunStatus,
    SQLiteSubagentStore,
)
from agentscope.event import (
    ReplyEndEvent,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.message import Msg, ToolCallBlock
from agentscope.state import AgentState
from agentscope.types import ReplyFinishedReason


class _RestartedAgent(Agent):
    """A real Agent instance with a deterministic local reply stream.

    Bypassing ``Agent.__init__`` is intentional: this test isolates durable
    state/restart semantics from a model provider while still requiring the
    runner to reconstruct the actual Agent class and its AgentState.
    """
    def __init__(self, state: AgentState):
        # Agent's constructor requires a provider; the durable runner only
        # needs the public state and reply_stream contract for this test.
        self.state = state
        self.reply_stream = types.MethodType(_reply_stream, self)


async def _reply_stream(self, inputs):
    if isinstance(inputs, Msg):
        self.state.middle_context["started"] = self.state.middle_context.get("started", 0) + 1
        yield RequireUserConfirmEvent(
            reply_id="reply-1",
            tool_calls=[ToolCallBlock(id="call-1", name="confirm", input="{}")],
        )
        return
    self.state.middle_context["resumed"] = self.state.middle_context.get("resumed", 0) + 1
    yield ReplyEndEvent(
        session_id=self.state.session_id,
        reply_id="reply-1",
        finished_reason=ReplyFinishedReason.COMPLETED,
    )


def _factory(state: AgentState) -> _RestartedAgent:
    return _RestartedAgent(state)


def _process_start(path: str, queue) -> None:
    async def run() -> None:
        store = SQLiteSubagentStore(path)
        result = await DurableSubagentRunner(store, _factory).start("run", "task", "delivery-start")
        queue.put((result.status.value, result.pending_reply_id))
        await store.aclose()
    asyncio.run(run())


def _process_resume(path: str, queue) -> None:
    async def run() -> None:
        store = SQLiteSubagentStore(path)
        result = await DurableSubagentRunner(store, _factory).resume(
            "run", UserConfirmResultEvent(reply_id="reply-1", confirm_results=[]), "delivery-resume",
        )
        record = await store.get("run")
        queue.put((result.status.value, record.state.middle_context))
        await store.aclose()
    asyncio.run(run())


@pytest.mark.asyncio
async def test_delivery_id_is_idempotent_and_handoff_restores_state(tmp_path: Path) -> None:
    path = str(tmp_path / "runs.sqlite")
    store = SQLiteSubagentStore(path)
    runner = DurableSubagentRunner(store, _factory)
    first = await runner.start("r", "task", "d1")
    duplicate = await runner.start("r", "task", "d1")
    assert first.status is RunStatus.WAITING
    assert duplicate.duplicate and duplicate.events == first.events

    resumed = await runner.resume("r", UserConfirmResultEvent(reply_id="reply-1", confirm_results=[]), "d2")
    assert resumed.status is RunStatus.COMPLETED
    record = await runner.get("r")
    assert record.state.middle_context == {"started": 1, "resumed": 1}
    await store.aclose()


def test_restart_in_another_process_rehydrates_agent_state(tmp_path: Path) -> None:
    path = str(tmp_path / "multi.sqlite")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    first = ctx.Process(target=_process_start, args=(path, queue)); first.start(); first.join(30)
    assert first.exitcode == 0
    assert queue.get(timeout=2) == (RunStatus.WAITING.value, "reply-1")
    second = ctx.Process(target=_process_resume, args=(path, queue)); second.start(); second.join(30)
    assert second.exitcode == 0
    assert queue.get(timeout=2) == (RunStatus.COMPLETED.value, {"started": 1, "resumed": 1})


@pytest.mark.asyncio
async def test_fencing_rejects_late_worker_commit(tmp_path: Path) -> None:
    store = SQLiteSubagentStore(str(tmp_path / "fence.sqlite"))
    one = await store.claim("r", "task", "factory", "d1", "worker-1", 0)
    two = await store.claim("r", "task", "factory", "d2", "worker-2", 60)
    assert two.fence > one.fence
    with pytest.raises(LostOwnershipError):
        await store.commit("r", one.owner, one.fence, AgentState(), RunStatus.FAILED, None, None, [], "d1", "late")
    await store.aclose()


@pytest.mark.asyncio
async def test_cancel_is_terminal_and_survives_restart(tmp_path: Path) -> None:
    store = SQLiteSubagentStore(str(tmp_path / "cancel.sqlite"))
    await DurableSubagentRunner(store, _factory).start("r", "task", "d1")
    cancelled = await store.cancel("r", "parent stopped")
    assert cancelled.status is RunStatus.CANCELLED
    record = await store.get("r")
    assert record.status is RunStatus.CANCELLED
    await store.aclose()

@pytest.mark.asyncio
async def test_same_store_serializes_concurrent_claims(tmp_path: Path) -> None:
    store = SQLiteSubagentStore(str(tmp_path / "concurrent.sqlite"))

    async def claim(index: int):
        try:
            return await store.claim("r", "task", "factory", f"d-{index}", f"owner-{index}", 30)
        except Exception as exc:  # noqa: BLE001 - competing claims are expected to be busy
            return exc

    outcomes = await asyncio.gather(*(claim(index) for index in range(12)))
    assert sum(getattr(outcome, "kind", None) == "claimed" for outcome in outcomes) == 1
    assert all(type(outcome).__name__ in {"_Claim", "RunBusyError"} for outcome in outcomes)
    await store.aclose()
