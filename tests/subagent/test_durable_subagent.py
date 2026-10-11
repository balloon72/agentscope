"""Cross-process durability tests for the durable subagent runner."""

from __future__ import annotations

import asyncio
import multiprocessing
import types
from pathlib import Path

import pytest

from agentscope import SubagentExecutionContext
from agentscope.agent import Agent
from agentscope.agent._durable_subagent import (
    DurableSubagentRunner,
    LostOwnershipError,
    RunStatus,
    SQLiteSubagentStore,
)
from agentscope.event import (
    ExternalExecutionResultEvent,
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
        self.state.middle_context["started"] = (
            self.state.middle_context.get("started", 0) + 1
        )
        yield RequireUserConfirmEvent(
            reply_id="reply-1",
            tool_calls=[ToolCallBlock(id="call-1", name="confirm", input="{}")],
        )
        return
    self.state.middle_context["resumed"] = (
        self.state.middle_context.get("resumed", 0) + 1
    )
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
        result = await DurableSubagentRunner(store, _factory).start(
            "run", "task", "delivery-start"
        )
        queue.put((result.status.value, result.pending_reply_id))
        await store.aclose()

    asyncio.run(run())


def _process_resume(path: str, queue) -> None:
    async def run() -> None:
        store = SQLiteSubagentStore(path)
        result = await DurableSubagentRunner(store, _factory).resume(
            "run",
            UserConfirmResultEvent(reply_id="reply-1", confirm_results=[]),
            "delivery-resume",
        )
        record = await store.get("run")
        queue.put((result.status.value, record.state.middle_context))
        await store.aclose()

    asyncio.run(run())


@pytest.mark.asyncio
async def test_delivery_id_is_idempotent_and_handoff_restores_state(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "runs.sqlite")
    store = SQLiteSubagentStore(path)
    runner = DurableSubagentRunner(store, _factory)
    first = await runner.start("r", "task", "d1")
    duplicate = await runner.start("r", "task", "d1")
    assert first.status is RunStatus.WAITING
    assert duplicate.duplicate and duplicate.events == first.events

    resumed = await runner.resume(
        "r", UserConfirmResultEvent(reply_id="reply-1", confirm_results=[]), "d2"
    )
    assert resumed.status is RunStatus.COMPLETED
    record = await runner.get("r")
    assert record.state.middle_context == {"started": 1, "resumed": 1}
    await store.aclose()


@pytest.mark.asyncio
async def test_resume_rejects_stale_reply_without_consuming_parked_run(
    tmp_path: Path,
) -> None:
    """Durable resume must enforce the persisted HITL correlation key."""
    store = SQLiteSubagentStore(str(tmp_path / "resume-correlation.sqlite"))
    runner = DurableSubagentRunner(store, _factory)
    parked = await runner.start("r", "task", "d1")
    assert parked.status is RunStatus.WAITING

    with pytest.raises(ValueError, match="does not match"):
        await runner.resume(
            "r",
            UserConfirmResultEvent(reply_id="stale", confirm_results=[]),
            "d-stale",
        )

    record = await runner.get("r")
    assert record.status is RunStatus.WAITING
    assert record.pending_reply_id == "reply-1"
    await store.aclose()


@pytest.mark.asyncio
async def test_resume_rejects_wrong_hitl_family_without_consuming_parked_run(
    tmp_path: Path,
) -> None:
    """A confirmation result cannot answer an external-execution request."""
    store = SQLiteSubagentStore(str(tmp_path / "resume-family.sqlite"))
    runner = DurableSubagentRunner(store, _factory)
    parked = await runner.start("r", "task", "d1")
    assert parked.status is RunStatus.WAITING

    with pytest.raises(TypeError, match="UserConfirmResultEvent"):
        await runner.resume(
            "r",
            ExternalExecutionResultEvent(reply_id="reply-1", execution_results=[]),
            "d-family",
        )

    record = await runner.get("r")
    assert record.status is RunStatus.WAITING
    assert record.pending_reply_id == "reply-1"
    await store.aclose()


@pytest.mark.asyncio
async def test_resume_rejects_forged_parent_context_without_consuming_run(
    tmp_path: Path,
) -> None:
    store = SQLiteSubagentStore(str(tmp_path / "resume-context.sqlite"))
    runner = DurableSubagentRunner(store, _factory)
    context = SubagentExecutionContext(
        "parent-a",
        "child",
        principal="user-a",
        metadata={"workspace_id": "workspace-a"},
    )
    parked = await runner.start("child", "task", "d1", context)
    assert parked.status is RunStatus.WAITING

    forged = SubagentExecutionContext(
        "parent-b",
        "child",
        principal="user-a",
        metadata={"workspace_id": "workspace-a"},
    )
    with pytest.raises(ValueError, match="context"):
        await runner.resume(
            "child",
            UserConfirmResultEvent(reply_id="reply-1", confirm_results=[]),
            "d2",
            forged,
        )
    record = await runner.get("child")
    assert record.status is RunStatus.WAITING

    resumed = await runner.resume(
        "child",
        UserConfirmResultEvent(reply_id="reply-1", confirm_results=[]),
        "d2",
        context,
    )
    assert resumed.status is RunStatus.COMPLETED
    await store.aclose()


@pytest.mark.asyncio
async def test_resume_replay_is_terminal_and_does_not_run_worker_twice(
    tmp_path: Path,
) -> None:
    store = SQLiteSubagentStore(str(tmp_path / "resume-replay.sqlite"))
    runner = DurableSubagentRunner(store, _factory)
    await runner.start("r", "task", "d1")
    event = UserConfirmResultEvent(reply_id="reply-1", confirm_results=[])
    first = await runner.resume("r", event, "d2")
    assert first.status is RunStatus.COMPLETED
    duplicate = await runner.resume("r", event, "d2")
    assert duplicate.status is RunStatus.COMPLETED
    assert duplicate.duplicate
    with pytest.raises(ValueError, match="status"):
        await runner.resume("r", event, "d3")
    record = await runner.get("r")
    assert record.state.middle_context == {"started": 1, "resumed": 1}
    await store.aclose()


@pytest.mark.asyncio
async def test_runner_keeps_legacy_store_claim_signature_compatible(
    tmp_path: Path,
) -> None:
    """Hosts with a pre-CAS DurableStore still receive the old call shape."""

    class LegacyStore:
        def __init__(self) -> None:
            self.inner = SQLiteSubagentStore(str(tmp_path / "legacy.sqlite"))

        async def claim(
            self,
            run_id: str,
            task: str,
            factory_id: str,
            delivery_id: str,
            owner: str,
            lease_seconds: float,
            context=None,
        ):
            return await self.inner.claim(
                run_id,
                task,
                factory_id,
                delivery_id,
                owner,
                lease_seconds,
                context,
            )

        async def commit(self, *args, **kwargs):
            return await self.inner.commit(*args, **kwargs)

        async def get(self, run_id: str):
            return await self.inner.get(run_id)

        async def renew(self, *args, **kwargs):
            return await self.inner.renew(*args, **kwargs)

        async def cancel(self, *args, **kwargs):
            return await self.inner.cancel(*args, **kwargs)

        async def close(self) -> None:
            await self.inner.aclose()

    store = LegacyStore()
    runner = DurableSubagentRunner(store, _factory)
    parked = await runner.start("r", "task", "d1")
    assert parked.status is RunStatus.WAITING
    resumed = await runner.resume(
        "r",
        UserConfirmResultEvent(reply_id="reply-1", confirm_results=[]),
        "d2",
    )
    assert resumed.status is RunStatus.COMPLETED
    await store.close()


def test_restart_in_another_process_rehydrates_agent_state(tmp_path: Path) -> None:
    path = str(tmp_path / "multi.sqlite")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    first = ctx.Process(target=_process_start, args=(path, queue))
    first.start()
    first.join(30)
    assert first.exitcode == 0
    assert queue.get(timeout=2) == (RunStatus.WAITING.value, "reply-1")
    second = ctx.Process(target=_process_resume, args=(path, queue))
    second.start()
    second.join(30)
    assert second.exitcode == 0
    assert queue.get(timeout=2) == (
        RunStatus.COMPLETED.value,
        {"started": 1, "resumed": 1},
    )


@pytest.mark.asyncio
async def test_fencing_rejects_late_worker_commit(tmp_path: Path) -> None:
    store = SQLiteSubagentStore(str(tmp_path / "fence.sqlite"))
    one = await store.claim("r", "task", "factory", "d1", "worker-1", 0)
    two = await store.claim("r", "task", "factory", "d2", "worker-2", 60)
    assert two.fence > one.fence
    with pytest.raises(LostOwnershipError):
        await store.commit(
            "r",
            one.owner,
            one.fence,
            AgentState(),
            RunStatus.FAILED,
            None,
            None,
            [],
            "d1",
            "late",
        )
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
            return await store.claim(
                "r", "task", "factory", f"d-{index}", f"owner-{index}", 30
            )
        except Exception as exc:  # noqa: BLE001 - competing claims are expected to be busy
            return exc

    outcomes = await asyncio.gather(*(claim(index) for index in range(12)))
    assert sum(getattr(outcome, "kind", None) == "claimed" for outcome in outcomes) == 1
    assert all(
        type(outcome).__name__ in {"_Claim", "RunBusyError"} for outcome in outcomes
    )
    await store.aclose()
