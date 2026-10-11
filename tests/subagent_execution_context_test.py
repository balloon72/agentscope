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


def test_context_rejects_non_json_principal_and_metadata_with_stable_error() -> None:
    class Principal:
        pass

    for kwargs in (
        {"principal": Principal()},
        {"metadata": {"opaque": Principal()}},
        {"metadata": {"not-a-number": float("nan")}},
    ):
        with pytest.raises(
            ValueError,
            match="^subagent execution context principal and metadata must be JSON-serializable$",
        ) as exc_info:
            SubagentExecutionContext("parent", "child", **kwargs)
        assert "Principal object" not in str(exc_info.value)


def test_context_json_is_canonical_and_revalidates_nested_mutations() -> None:
    from agentscope.agent._durable_subagent import _context_json

    context = SubagentExecutionContext(
        "parent",
        "child",
        principal={"role": "operator", "tenant": "a"},
        metadata={"z": 2, "a": [1, 2]},
    )
    assert _context_json(context) == (
        '{"metadata":{"a":[1,2],"z":2},"parent_run_id":"parent",'
        '"principal":{"role":"operator","tenant":"a"},"subagent_run_id":"child"}'
    )

    # The mapping itself is read-only, but nested values can still be mutated
    # by a caller holding the original list. Durable serialization validates
    # again at the persistence boundary.
    context.metadata["a"].append(object())  # type: ignore[index]
    with pytest.raises(
        ValueError,
        match="^subagent execution context principal and metadata must be JSON-serializable$",
    ):
        _context_json(context)
