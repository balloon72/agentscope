"""Adapters for registering :class:`SubagentTool` in an app toolkit.

The service layer already accepts a per-session ``extra_agent_tools``
factory. Keeping this small adapter in the tool package lets applications
provide an agent resolver without teaching :mod:`agentscope.app` about a
particular resolver, storage backend, or hosted-worker implementation.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ._base import ToolBase
from ._subagent_tool import SubagentTool

AgentResolver = Callable[..., Any]
"""Resolver accepted by :class:`~agentscope.tool.SubagentTool`.

The resolver is intentionally typed as a callable with an implementation
defined signature. ``SubagentTool`` supports the resolver forms documented
by its constructor (for example ``resolver(agent_id)`` or
``resolver(agent_id, context)``), while an application may close over its own
request/session context.
"""


AgentToolFactory = Callable[
    [str, str, str],
    Awaitable[list[ToolBase]],
]
"""The shape expected by ``create_app(extra_agent_tools=...)``."""


def create_subagent_tool_factory(
    agent_resolver: AgentResolver,
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
    worker; it is not called while the toolkit is assembled.

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
        **tool_kwargs: Additional ``SubagentTool`` constructor options.

    Returns:
        An async ``extra_agent_tools`` factory suitable for ``create_app`` or
        ``ChatService``.
    """
    if not callable(agent_resolver):
        raise TypeError("agent_resolver must be callable")

    async def factory(
        user_id: str,
        agent_id: str,
        session_id: str,
    ) -> list[ToolBase]:
        # Keep the public app callback's arguments even though this generic
        # adapter does not interpret them. Applications needing those values
        # can close over them when constructing ``agent_resolver``.
        del user_id, agent_id, session_id
        return [SubagentTool(agent_resolver=agent_resolver, **tool_kwargs)]

    return factory


__all__ = [
    "AgentResolver",
    "AgentToolFactory",
    "create_subagent_tool_factory",
]
