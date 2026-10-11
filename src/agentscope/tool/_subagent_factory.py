"""Adapters for registering :class:`SubagentTool` in an app toolkit.

The service layer already accepts a per-session ``extra_agent_tools``
factory. Keeping this small adapter in the tool package lets applications
provide an agent resolver without teaching :mod:`agentscope.app` about a
particular resolver, storage backend, or hosted-worker implementation.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from ._base import ToolBase
from ._subagent_tool import SubagentTool

AgentResolver = Callable[..., Any]
"""Resolver accepted by :class:`~agentscope.tool.SubagentTool`.

The resolver is intentionally typed as a callable with an implementation
defined signature. ``SubagentTool`` supports the resolver forms documented
by its constructor (for example ``resolver(agent_id)`` or
``resolver(agent_id, context)``), while an application may close over its own
request/session context.  Each invocation must return a fresh worker factory
or a fresh Agent instance.  Reusing an already-created Agent across runs is
rejected with ``agent_instance_reused`` because Agent reply state is not safe
to share.
"""


AgentContextFactory = Callable[[str, str, str], Any]
"""Factory for context owned by one assembled parent agent.

The callback receives ``(user_id, parent_agent_id, session_id)`` and may
return a context object directly or an awaitable resolving to one.  The
returned value is kept in the closure of the corresponding tool instance;
it is never put in process-global state.
"""


AgentToolFactory = Callable[
    [str, str, str],
    Awaitable[list[ToolBase]],
]
"""The shape expected by ``create_app(extra_agent_tools=...)``."""


def create_subagent_tool_factory(
    agent_resolver: AgentResolver,
    *,
    context_factory: AgentContextFactory | None = None,
    **tool_kwargs: Any,
) -> AgentToolFactory:
    """Adapt an agent resolver to the app's per-session tool hook.

    ``create_app`` and :class:`~agentscope.app._service.ChatService` already
    invoke ``extra_agent_tools`` for every agent/session assembly. This helper
    returns a factory in that shape and creates a fresh
    :class:`SubagentTool` for each invocation, so run registries and pending
    HITL continuations cannot leak between sessions. The resolver is kept
    application-owned and is passed unchanged to each tool.

    ``agent_resolver`` is invoked by ``SubagentTool`` when a parent chooses a
    worker; it is not called while the toolkit is assembled.  When
    ``context_factory`` is supplied, it is called while each toolkit is
    assembled and its result is merged with the per-run context before the
    resolver is invoked.  The resolver itself is wrapped per toolkit
    invocation, so simultaneous sessions cannot observe one another's
    context.  The resolver must return a fresh factory or Agent instance for
    every invocation; a cached Agent is rejected with
    ``agent_instance_reused``.

    Example::

        app = create_app(
            ...,
            extra_agent_tools=create_subagent_tool_factory(
                resolve_agent,
                allowed_agent_ids={"researcher"},
            ),
        )

    Args:
        agent_resolver: Callable used by ``SubagentTool`` to construct or
            resolve a worker agent. Its accepted call forms are documented by
            ``SubagentTool``.
        context_factory: Optional callback receiving ``(user_id,
            parent_agent_id, session_id)`` and returning context for this
            assembled parent. The callback may be synchronous or asynchronous.
        **tool_kwargs: Additional ``SubagentTool`` constructor options.

    Returns:
        An async ``extra_agent_tools`` factory suitable for ``create_app`` or
        ``ChatService``.
    """
    if not callable(agent_resolver):
        raise TypeError("agent_resolver must be callable")
    if context_factory is not None and not callable(context_factory):
        raise TypeError("context_factory must be callable")

    async def factory(
        user_id: str,
        agent_id: str,
        session_id: str,
    ) -> list[ToolBase]:
        # Preserve the historical path exactly: applications that do not opt
        # into context propagation get the original resolver object and its
        # original call signatures.
        if context_factory is None:
            del user_id, agent_id, session_id
            return [SubagentTool(agent_resolver=agent_resolver, **tool_kwargs)]

        parent_context = context_factory(user_id, agent_id, session_id)
        if inspect.isawaitable(parent_context):
            parent_context = await parent_context
        # Snapshot mappings returned by application code.  A context factory
        # may reuse a scratch dictionary between requests; retaining that
        # object in a resolver closure would let a later session overwrite an
        # earlier session's identity.
        parent_context = _copy_context(parent_context)

        async def scoped_resolver(
            resolved_agent_id: str,
            context: Any = None,
        ) -> Any:
            merged_context = _merge_context(parent_context, context)
            return await _invoke_resolver(
                agent_resolver,
                resolved_agent_id,
                merged_context,
            )

        return [SubagentTool(agent_resolver=scoped_resolver, **tool_kwargs)]

    return factory


__all__ = [
    "AgentContextFactory",
    "AgentResolver",
    "AgentToolFactory",
    "create_subagent_tool_factory",
]


def _merge_context(parent_context: Any, run_context: Any) -> Any:
    """Merge parent-owned and run-owned context without shared mutation.

    Mapping contexts are merged shallowly with run values taking precedence;
    this keeps the run's routing identity authoritative while retaining host
    values such as user and workspace IDs.  ``SubagentExecutionContext`` is
    immutable and is merged through a new instance so its run identity is
    preserved.  For opaque context objects, the run value wins when present.
    """
    if parent_context is None:
        return _copy_context(run_context)
    if run_context is None:
        return _copy_context(parent_context)

    # Keep the framework context type intact for resolver signatures that
    # explicitly ask for SubagentExecutionContext.  Import lazily to avoid a
    # package bootstrap cycle through agentscope.tool.
    from ..agent._execution_context import SubagentExecutionContext

    if isinstance(run_context, SubagentExecutionContext):
        if isinstance(parent_context, SubagentExecutionContext):
            metadata = dict(parent_context.metadata)
            metadata.update(run_context.metadata)
            principal = (
                run_context.principal
                if run_context.principal is not None
                else parent_context.principal
            )
            return SubagentExecutionContext(
                parent_run_id=run_context.parent_run_id,
                subagent_run_id=run_context.subagent_run_id,
                principal=principal,
                metadata=metadata,
            )
        if isinstance(parent_context, Mapping):
            metadata = dict(parent_context)
            metadata.update(run_context.metadata)
            return SubagentExecutionContext(
                parent_run_id=run_context.parent_run_id,
                subagent_run_id=run_context.subagent_run_id,
                principal=run_context.principal,
                metadata=metadata,
            )
        return run_context

    if isinstance(parent_context, SubagentExecutionContext):
        if isinstance(run_context, Mapping):
            metadata = dict(parent_context.metadata)
            metadata.update(run_context)
            return SubagentExecutionContext(
                parent_run_id=parent_context.parent_run_id,
                subagent_run_id=parent_context.subagent_run_id,
                principal=parent_context.principal,
                metadata=metadata,
            )
        return _copy_context(run_context)

    if isinstance(parent_context, Mapping) and isinstance(run_context, Mapping):
        merged = dict(parent_context)
        merged.update(run_context)
        return merged

    return _copy_context(run_context)


def _copy_context(context: Any) -> Any:
    """Copy mapping contexts so resolver code cannot mutate session state."""
    if isinstance(context, Mapping):
        return dict(context)
    return context


async def _invoke_resolver(
    resolver: AgentResolver,
    agent_id: str,
    context: Any,
) -> Any:
    """Invoke a resolver while retaining SubagentTool's legacy signatures."""
    if context is not None and isinstance(context, Mapping):
        try:
            signature = inspect.signature(resolver)
        except (TypeError, ValueError):
            signature = None
        if signature is not None:
            parameters = signature.parameters
            kwargs = {
                key: value
                for key, value in context.items()
                if key in parameters
                or any(
                    parameter.kind is inspect.Parameter.VAR_KEYWORD
                    for parameter in parameters.values()
                )
            }
            kwargs["agent_id"] = agent_id
            context_parameter = parameters.get("context")
            if context_parameter is not None and context_parameter.kind is not (
                inspect.Parameter.POSITIONAL_ONLY
            ):
                # A context-aware resolver must receive the complete merged
                # object, even when it also exposes named fields.  Without
                # this, ``resolver(agent_id, context=None)`` would bind only
                # ``agent_id`` and silently discard the mapping.
                kwargs["context"] = dict(context)
            try:
                signature.bind(**kwargs)
            except TypeError:
                pass
            else:
                value = resolver(**kwargs)
                return await value if inspect.isawaitable(value) else value

    try:
        signature = inspect.signature(resolver)
    except (TypeError, ValueError):
        signature = None
    resolver_context = context
    if isinstance(context, Mapping):
        resolver_context = dict(context)
        resolver_context["agent_id"] = agent_id
    if context is not None and signature is not None:
        parameters = list(signature.parameters.values())
        positional = [
            parameter
            for parameter in parameters
            if parameter.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        accepts_varargs = any(
            parameter.kind is inspect.Parameter.VAR_POSITIONAL
            for parameter in parameters
        )
        if "context" in signature.parameters:
            value = resolver(agent_id, context=resolver_context)
        elif accepts_varargs or len(positional) >= 2:
            value = resolver(agent_id, resolver_context)
        else:
            value = resolver(agent_id)
    elif context is not None and isinstance(context, (tuple, list)):
        value = resolver(agent_id, *context)
    else:
        value = resolver(agent_id)
    return await value if inspect.isawaitable(value) else value
