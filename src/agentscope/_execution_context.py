"""Context passed through one parent-owned subagent execution.

The context is deliberately small and framework-neutral.  It carries the
identifiers needed to route events/results back to a parent, an optional
principal supplied by the host application, and immutable metadata that a
resolver or worker may use for tracing.  It is never stored in a process
-global/context variable; callers pass it explicitly at each boundary.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, TypeVar

_CONTEXT_JSON_ERROR = (
    "subagent execution context principal and metadata must be JSON-serializable"
)


def _validate_json_values(principal: Any, metadata: Mapping[str, Any]) -> None:
    """Validate values that cross a durable context boundary.

    Keep this error deliberately stable and free of the offending value's
    representation.  ``json.dumps`` defaults to accepting NaN/Infinity, but
    those are outside the JSON specification and are rejected here.
    """

    try:
        json.dumps(
            {"principal": principal, "metadata": dict(metadata)},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError):
        raise ValueError(_CONTEXT_JSON_ERROR) from None


def canonical_context_json(context: SubagentExecutionContext) -> str:
    """Serialize a context with one canonical JSON representation.

    This helper is shared by durable stores so SQLite and Redis compare and
    persist exactly the same context bytes.  It revalidates on every call in
    case a caller mutated a nested value after construction.
    """

    try:
        return json.dumps(
            context.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError):
        raise ValueError(_CONTEXT_JSON_ERROR) from None


@dataclass(frozen=True, slots=True)
class SubagentExecutionContext:
    """Execution identity and host metadata for one subagent run.

    ``metadata`` is copied at construction time and exposed through a
    read-only mapping so a worker cannot mutate a parent's context by
    retaining a reference to the original dictionary.  ``principal`` and
    metadata values must be JSON-serializable because durable stores persist
    the context across process boundaries.
    """

    parent_run_id: str
    subagent_run_id: str
    principal: Any | None = None
    metadata: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({}),
    )

    def __post_init__(self) -> None:
        if not isinstance(self.parent_run_id, str):
            raise TypeError("parent_run_id must be a string")
        if not isinstance(self.subagent_run_id, str):
            raise TypeError("subagent_run_id must be a string")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("metadata must be a mapping")
        # Always copy, even when the input is already a mapping proxy.  This
        # prevents later mutation of a caller-owned mapping from changing the
        # context observed by a worker.
        copied_metadata = dict(self.metadata)
        _validate_json_values(self.principal, copied_metadata)
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(copied_metadata),
        )

    def with_subagent_run_id(self, subagent_run_id: str) -> SubagentExecutionContext:
        """Return this context with a generated/known child run ID."""
        return replace(self, subagent_run_id=subagent_run_id)

    @property
    def workspace_id(self) -> str | None:
        """Optional host workspace identifier carried in metadata."""
        value = self.metadata.get("workspace_id")
        return value if isinstance(value, str) else None

    @property
    def sandbox_mode(self) -> str | None:
        """Optional host sandbox mode carried in metadata."""
        value = self.metadata.get("sandbox_mode")
        return value if isinstance(value, str) else None

    def to_dict(self) -> dict[str, Any]:
        """Return the structural representation persisted by durable stores."""
        metadata = dict(self.metadata)
        _validate_json_values(self.principal, metadata)
        return {
            "parent_run_id": self.parent_run_id,
            "subagent_run_id": self.subagent_run_id,
            "principal": self.principal,
            "metadata": metadata,
        }


_MISSING = object()


def call_factory_with_context(
    factory: Any,
    context: SubagentExecutionContext | None,
    *legacy_args: Any,
) -> Any:
    """Call a factory while adapting old and context-aware signatures.

    Existing AgentScope factories use no arguments or (for durable workers)
    one ``AgentState`` argument.  New factories may accept ``context`` as a
    positional/keyword argument or add it after the legacy arguments.  The
    signature is inspected before invocation so the factory is called exactly
    once, avoiding duplicate worker creation on a failed speculative call.
    """

    if context is None:
        return factory(*legacy_args)

    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        # Builtins/callables without an inspectable signature retain the
        # historical call shape.  A context-aware implementation can expose a
        # regular Python wrapper if it needs explicit context support.
        return factory(*legacy_args)

    params = list(signature.parameters.values())
    by_name = signature.parameters.get("context")
    if by_name is not None and by_name.kind in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    ):
        positional = [
            p
            for p in params
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        # A one-argument ``factory(context)`` is context-only even when the
        # durable runner has a legacy state value available.  This lets new
        # factories opt into context without accepting an unused state.
        if (
            len(positional) == 1
            and positional[0].name == "context"
            and not any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in params)
        ):
            return factory(context)
        kwargs = {"context": context}
        return factory(*legacy_args, **kwargs)

    has_var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params)
    if has_var_kw:
        return factory(*legacy_args, context=context)

    positional = [
        p
        for p in params
        if p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    # A context-only factory is the common shape for SubagentSession.  A
    # factory with legacy arguments receives context as the next positional
    # argument (e.g. ``factory(state, context)``).
    if len(positional) > len(legacy_args):
        return factory(*legacy_args, context)

    # A callable accepting *args can safely receive context positionally.
    if any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in params):
        return factory(*legacy_args, context)

    # Signature has no room for context.  Preserve the old factory contract.
    return factory(*legacy_args)


T = TypeVar("T")


def context_for_run(
    context: SubagentExecutionContext | None,
    run_id: str,
) -> SubagentExecutionContext | None:
    """Bind a context to ``run_id`` while retaining legacy ``None`` behavior."""
    if context is None:
        return None
    if context.subagent_run_id and context.subagent_run_id != run_id:
        raise ValueError(
            "context.subagent_run_id does not match the subagent run ID",
        )
    if context.subagent_run_id == run_id:
        return context
    return context.with_subagent_run_id(run_id)


def context_payload(context: SubagentExecutionContext | None) -> dict[str, Any] | None:
    """Return an optional result payload without changing legacy results."""
    return None if context is None else context.to_dict()


__all__ = [
    "SubagentExecutionContext",
    "call_factory_with_context",
    "canonical_context_json",
    "context_for_run",
    "context_payload",
]
