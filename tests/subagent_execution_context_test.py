"""Tests for explicit parent-to-subagent execution context propagation."""

from __future__ import annotations

import pytest

from agentscope import SubagentExecutionContext
from agentscope._execution_context import call_factory_with_context, context_for_run


def test_context_copies_immutable_metadata_and_exposes_host_scope() -> None:
    metadata = {"workspace_id": "ws-1", "sandbox_mode": "strict"}
    context = SubagentExecutionContext(
        "parent",
        "child",
        principal="user",
        metadata=metadata,
    )
    metadata["workspace_id"] = "mutated"

    assert context.metadata["workspace_id"] == "ws-1"
    assert context.workspace_id == "ws-1"
    assert context.sandbox_mode == "strict"
    with pytest.raises(TypeError):
        context.metadata["new"] = "value"  # type: ignore[index]


def test_factory_adapter_supports_context_only_and_legacy_signatures() -> None:
    context = SubagentExecutionContext("parent", "child")
    seen: list[object] = []

    def context_only(context: SubagentExecutionContext) -> object:
        seen.append(context)
        return context

    def legacy(value: str) -> str:
        return value

    def state_and_context(
        value: str,
        context: SubagentExecutionContext,
    ) -> tuple[str, object]:
        return value, context

    assert call_factory_with_context(context_only, context, "state") is context
    assert call_factory_with_context(legacy, context, "state") == "state"
    assert call_factory_with_context(state_and_context, context, "state") == (
        "state",
        context,
    )
    assert seen == [context]


def test_context_for_run_binds_an_empty_child_id_and_rejects_mismatch() -> None:
    context = SubagentExecutionContext("parent", "")
    bound = context_for_run(context, "child")
    assert bound is not None
    assert bound.subagent_run_id == "child"

    with pytest.raises(ValueError):
        context_for_run(SubagentExecutionContext("parent", "other"), "child")
