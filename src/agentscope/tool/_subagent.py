"""Utilities for composing stateless one-shot subagent tools."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from .._execution_context import (
    SubagentExecutionContext,
    call_factory_with_context,
)
from ..message import Msg, TextBlock
from ._adapters import FunctionTool


class OneShotSubagent:
    """Run a fresh worker agent for one task and discard its state.

    The factory is called for every invocation, so a worker never carries
    conversation state from one tool call into another. The returned mapping
    is intentionally small and JSON-serializable, which makes it suitable as
    a normal :class:`~agentscope.tool.FunctionTool` result.
    """

    def __init__(
        self,
        agent_factory: Callable[[], Any],
        *,
        timeout: float | None = None,
        max_result_chars: int = 8_000,
    ) -> None:
        if max_result_chars <= 0:
            raise ValueError("max_result_chars must be positive")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        self._agent_factory = agent_factory
        self._timeout = timeout
        self._max_result_chars = max_result_chars

    async def run(
        self,
        task: str,
        context: SubagentExecutionContext | None = None,
    ) -> dict[str, Any]:
        """Run one task on a newly created worker agent."""
        if not isinstance(task, str) or not task.strip():
            return {"status": "error", "error": "task must not be empty"}

        worker = None
        try:
            worker = call_factory_with_context(self._agent_factory, context)
            request = Msg(name="parent", role="user", content=[TextBlock(text=task)])
            reply = worker.reply(request)
            if self._timeout is None:
                result = await reply
            else:
                try:
                    result = await asyncio.wait_for(reply, timeout=self._timeout)
                except TimeoutError:
                    result = {"status": "timeout", "error": "subagent timed out"}
                    if context is not None:
                        result["context"] = context
                    return result
            result: dict[str, Any] = {
                "status": "completed",
                "result": self._extract_text(result)[: self._max_result_chars],
            }
            if context is not None:
                result["context"] = context
            return result
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001  # worker errors are returned to the parent
            result = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
            if context is not None:
                result["context"] = context
            return result
        finally:
            await self._close_worker(worker)

    def as_tool(
        self,
        *,
        name: str = "call_subagent",
        description: str = (
            "Run a fresh specialist agent for one task. The specialist does "
            "not retain conversation state after the call."
        ),
    ) -> FunctionTool:
        """Return this runner as a regular AgentScope function tool."""

        async def call_subagent(task: str) -> dict[str, Any]:
            return await self.run(task)

        return FunctionTool(
            call_subagent,
            name=name,
            description=description,
            is_concurrency_safe=True,
        )

    @staticmethod
    def _extract_text(message: Msg) -> str:
        """Extract text blocks while keeping non-text content out of context."""
        return "".join(
            block.text for block in message.content if isinstance(block, TextBlock)
        )

    @staticmethod
    async def _close_worker(worker: Any) -> None:
        """Best-effort cleanup for one-shot workers.

        Agent implementations may own transports or background tasks. A
        one-shot invocation must release those resources on success, errors,
        timeout and caller cancellation alike. Cleanup failures are ignored so
        they cannot mask the original result or exception.
        """
        if worker is None:
            return
        for name in ("aclose", "close"):
            closer = getattr(worker, name, None)
            if not callable(closer):
                continue
            try:
                result = closer()
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001, S110 - cleanup must not mask result
                pass
            else:
                break
