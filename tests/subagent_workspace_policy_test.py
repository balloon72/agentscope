"""Generic collaborator workspace-policy contract tests.

Workspace lifecycle and sandbox construction belong to the host application.
These tests only pin the framework boundary: resolver context is explicit,
durable runs retain that context on resume, and an omitted context never turns
into an implicit parent-workspace request.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from agentscope import SubagentExecutionContext
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


class _PolicyWorker:
    """Tiny worker that records the application-selected workspace policy."""

    def __init__(self, collaborator_id: str, policy: dict[str, Any] | None) -> None:
        self.collaborator_id = collaborator_id
        self.policy = policy
        self.state = AgentState()
        self.closed = False

    async def reply_stream(self, inputs: Any) -> AsyncIterator[Any]:
        assert isinstance(inputs, Msg)
        yield Msg(
            name=self.collaborator_id,
            role="assistant",
            content=[TextBlock(text="workspace policy accepted")],
        )
        yield ReplyEndEvent(
            session_id=self.state.session_id,
            reply_id=f"{self.collaborator_id}-reply",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
@pytest.mark.parametrize("sandbox_mode", ["isolated", "parent", "none"])
async def test_resolver_receives_context_and_application_owns_policy(
    sandbox_mode: str,
) -> None:
    """The SDK forwards policy metadata without interpreting its value."""

    resolved: list[tuple[str, SubagentExecutionContext, _PolicyWorker]] = []

    def resolver(
        agent_id: str,
        context: SubagentExecutionContext | None = None,
    ) -> _PolicyWorker:
        assert context is not None
        # This is deliberately application code: the framework does not
        # construct, inherit, or validate a workspace from these values.
        policy = {
            "workspace_id": context.workspace_id,
            "sandbox_mode": context.sandbox_mode,
        }
        worker = _PolicyWorker(agent_id, policy)
        resolved.append((agent_id, context, worker))
        return worker

    bridge = SubagentTool(
        agent_resolver=resolver,
        allowed_agent_ids={"specialist"},
        cleanup_ttl=None,
    )
    context = SubagentExecutionContext(
        "parent-run",
        "",
        metadata={"workspace_id": "child-workspace", "sandbox_mode": sandbox_mode},
    )

    result = await bridge.run(
        "inspect workspace", agent_id="specialist", context=context
    )

    assert result["status"] == "completed"
    assert len(resolved) == 1
    agent_id, resolved_context, worker = resolved[0]
    assert agent_id == "specialist"
    assert resolved_context.parent_run_id == "parent-run"
    assert resolved_context.workspace_id == "child-workspace"
    assert resolved_context.sandbox_mode == sandbox_mode
    assert worker.policy == {
        "workspace_id": "child-workspace",
        "sandbox_mode": sandbox_mode,
    }
    assert worker.closed


@pytest.mark.asyncio
async def test_missing_context_does_not_infer_parent_workspace() -> None:
    """A resolver sees ``None`` until the application opts into a workspace."""

    seen: list[SubagentExecutionContext | None] = []

    def resolver(
        agent_id: str,
        context: SubagentExecutionContext | None = None,
    ) -> _PolicyWorker:
        seen.append(context)
        return _PolicyWorker(agent_id, None)

    bridge = SubagentTool(
        agent_resolver=resolver,
        allowed_agent_ids={"specialist"},
        cleanup_ttl=None,
    )
    result = await bridge.run("no implicit workspace", agent_id="specialist")

    assert result["status"] == "completed"
    assert seen == [None]


class _ContextDurableWorker:
    """Deterministic HITL worker used to verify context persistence."""

    def __init__(
        self,
        state: AgentState,
        context: SubagentExecutionContext | None,
    ) -> None:
        self.state = state
        self.context = context

    async def reply_stream(self, inputs: Any) -> AsyncIterator[Any]:
        if isinstance(inputs, Msg):
            yield RequireUserConfirmEvent(reply_id="policy-reply", tool_calls=[])
            return
        assert isinstance(inputs, UserConfirmResultEvent)
        yield ReplyEndEvent(
            session_id=self.state.session_id,
            reply_id="policy-reply",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )


@pytest.mark.asyncio
async def test_durable_resume_preserves_workspace_identity(
    tmp_path: Path,
) -> None:
    """A resumed run rebuilds its worker with the persisted context."""

    path = str(tmp_path / "workspace-policy.sqlite")
    expected = SubagentExecutionContext(
        "parent-run",
        "durable-run",
        principal="user-1",
        metadata={"workspace_id": "workspace-7", "sandbox_mode": "isolated"},
    )
    first_contexts: list[SubagentExecutionContext | None] = []
    second_contexts: list[SubagentExecutionContext | None] = []

    def first_factory(
        state: AgentState,
        context: SubagentExecutionContext | None = None,
    ) -> _ContextDurableWorker:
        first_contexts.append(context)
        return _ContextDurableWorker(state, context)

    def second_factory(
        state: AgentState,
        context: SubagentExecutionContext | None = None,
    ) -> _ContextDurableWorker:
        second_contexts.append(context)
        return _ContextDurableWorker(state, context)

    store = SQLiteSubagentStore(path)
    started = await DurableSubagentRunner(store, first_factory).start(
        "durable-run",
        "wait for policy approval",
        "start-delivery",
        context=SubagentExecutionContext(
            expected.parent_run_id,
            "",
            principal=expected.principal,
            metadata=dict(expected.metadata),
        ),
    )
    assert started.status is RunStatus.WAITING
    assert first_contexts == [expected]
    await store.aclose()

    # A new runner/node gets no context argument. The durable record supplies
    # exactly the same workspace identity and sandbox mode.
    resumed_store = SQLiteSubagentStore(path)
    resumed = await DurableSubagentRunner(resumed_store, second_factory).resume(
        "durable-run",
        UserConfirmResultEvent(reply_id="policy-reply", confirm_results=[]),
        "resume-delivery",
    )
    record = await resumed_store.get("durable-run")
    assert resumed.status is RunStatus.COMPLETED
    assert second_contexts == [expected]
    assert resumed.context == expected
    assert record.context == expected
    await resumed_store.aclose()
