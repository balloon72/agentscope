"""Parent-agent bridge for stateful subagents.

Unlike :class:`OneShotSubagent`, ``SubagentTool`` keeps a worker reply parked
while it waits for a human or external result.  The worker's events are made
available to the parent application through :meth:`events`; the parent can
route a matching result back with :meth:`resume`.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..agent._subagent import SubagentSession
    from ..event import (
        AgentEvent,
        ExternalExecutionResultEvent,
        UserConfirmResultEvent,
    )
from .._logging import logger
from ..message import Msg, TextBlock, ToolResultState
from ..permission import PermissionBehavior, PermissionContext, PermissionDecision
from ._base import ToolBase
from ._response import ToolChunk
from ._toolkit import Toolkit


def _is_hitl(event: object) -> bool:
    """Check HITL without importing event types during package bootstrap."""
    from ..event import RequireExternalExecutionEvent, RequireUserConfirmEvent

    return isinstance(event, (RequireUserConfirmEvent, RequireExternalExecutionEvent))


def _is_resume(event: object) -> bool:
    """Check a resume event without creating a model/event import cycle."""
    from ..event import ExternalExecutionResultEvent, UserConfirmResultEvent

    return isinstance(event, (UserConfirmResultEvent, ExternalExecutionResultEvent))


_SENTINEL = object()


@dataclass(frozen=True)
class SubagentEvent:
    """An event emitted by a child run, tagged with its parent run ID."""

    run_id: str
    event: AgentEvent | Msg | None
    error: str | None = None

    @property
    def reply_id(self) -> str | None:
        """The child reply ID, when the wrapped value has one."""
        return getattr(self.event, "reply_id", None)

    @property
    def requires_input(self) -> bool:
        """Whether this event parks the child awaiting parent input."""
        return _is_hitl(self.event)


@dataclass
class _Run:
    """Mutable state for one invocation of a ``SubagentTool``."""

    run_id: str
    session: SubagentSession
    queue: asyncio.Queue[SubagentEvent | object] = field(default_factory=asyncio.Queue)
    pending: SubagentEvent | None = None
    waiter: asyncio.Future[Any] | None = None
    task: asyncio.Task[dict[str, Any]] | None = None
    result: dict[str, Any] | None = None
    text: list[str] = field(default_factory=list)


class SubagentTool(ToolBase):
    """Expose a resumable :class:`SubagentSession` as a parent-agent tool.

    Register an instance in the parent's ``Toolkit``.  The parent's normal
    reply can then call this tool.  While the child runs, consume
    ``tool.events()`` (or ``tool.events(run_id)``) concurrently with the
    parent's reply.  On ``RequireUserConfirmEvent`` or
    ``RequireExternalExecutionEvent``, call ``await tool.resume(run_id,
    matching_result)``.  Results are checked against both run ID and child
    ``reply_id`` so nested children cannot consume one another's approvals.

    Child tool permissions are deliberately unchanged: the bridge itself is
    allowed, while each worker's own tools still issue their normal HITL
    events.
    """

    is_external_tool = False
    is_concurrency_safe = True
    is_read_only = False

    def __init__(
        self,
        agent_factory: Callable[[], Any],
        *,
        name: str = "call_subagent",
        description: str = (
            "Delegate one task to a specialist subagent. The specialist may "
            "pause for user confirmation or external tool execution."
        ),
        timeout: float | None = None,
        max_result_chars: int = 8_000,
        session_factory: Callable[[Callable[[], Any]], SubagentSession] | None = None,
    ) -> None:
        super().__init__()
        if not name:
            raise ValueError("name must not be empty")
        if max_result_chars <= 0:
            raise ValueError("max_result_chars must be positive")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        self.name = name
        self.description = description
        self.input_schema = {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "The complete task for the specialist.",
                },
            },
            "required": ["task"],
            "additionalProperties": False,
        }
        self._agent_factory = agent_factory
        if session_factory is None:
            # Import lazily: agentscope.agent imports agentscope.tool during
            # package initialization, so an eager import would cycle.
            from ..agent._subagent import SubagentSession

            session_factory = SubagentSession
        self._session_factory = session_factory
        self._timeout = timeout
        self._max_result_chars = max_result_chars
        self._runs: dict[str, _Run] = {}
        self._events: asyncio.Queue[Any] = asyncio.Queue()
        self._global_finished: set[str] = set()

    async def check_permissions(
        self,
        tool_input: dict[str, Any],
        context: PermissionContext,
    ) -> PermissionDecision:
        """The bridge itself is safe; worker tools perform their own checks."""
        return PermissionDecision(
            behavior=PermissionBehavior.ALLOW,
            message="Subagent delegation is allowed; worker tools retain their permissions.",
        )

    async def call(self, task: str) -> ToolChunk:
        """Run a child task and return its JSON result to the parent model."""
        result = await self.run(task)
        return ToolChunk(
            content=[TextBlock(text=json.dumps(result, ensure_ascii=False))],
            state=(
                ToolResultState.SUCCESS
                if result.get("status") == "completed"
                else ToolResultState.ERROR
            ),
        )

    async def run(self, task: str) -> dict[str, Any]:
        """Run a child task, waiting through any number of HITL turns."""
        if not isinstance(task, str) or not task.strip():
            return {"status": "error", "error": "task must not be empty"}
        run_id = uuid.uuid4().hex
        try:
            session = self._session_factory(self._agent_factory)
        except Exception as exc:  # noqa: BLE001  # factory errors belong in parent result
            return {
                "status": "error",
                "run_id": run_id,
                "error": f"{type(exc).__name__}: {exc}",
            }
        run = _Run(run_id=run_id, session=session)
        self._runs[run.run_id] = run
        run.task = asyncio.create_task(self._execute(run, task))
        try:
            return await run.task
        except asyncio.CancelledError:
            # Parent cancellation must never retain a worker or a resolver.
            if run.task is not asyncio.current_task() and not run.task.done():
                run.task.cancel()
                await asyncio.gather(run.task, return_exceptions=True)
            raise

    async def _execute(self, run: _Run, task: str) -> dict[str, Any]:
        """Drive session streams and park at child HITL events."""
        try:
            if self._timeout is None:
                return await self._drive(run, task)
            return await asyncio.wait_for(self._drive(run, task), self._timeout)
        except TimeoutError:
            run.result = {
                "status": "timeout",
                "run_id": run.run_id,
                "error": "subagent timed out",
            }
            return run.result
        except asyncio.CancelledError:
            # _drive closes its current stream and the SubagentSession marks
            # cancellation terminal. Keep a terminal record even though the
            # parent awaiter still receives CancelledError.
            run.result = {"status": "cancelled", "run_id": run.run_id}
            raise
        except Exception as exc:  # noqa: BLE001  # worker errors belong in parent result
            run.result = {
                "status": "error",
                "run_id": run.run_id,
                "error": f"{type(exc).__name__}: {exc}",
            }
            await self._emit(run, None, run.result["error"])
            return run.result
        finally:
            if run.waiter is not None and not run.waiter.done():
                run.waiter.cancel()
            if run.pending is not None:
                run.pending = None
            # ``_drive`` closes the worker on terminal replies. On timeout or
            # cancellation, interrupt a parked worker and drain the events.
            if not run.session.finished:
                try:
                    async for evt in run.session.interrupt():
                        await self._emit(run, evt)
                except Exception as exc:  # noqa: BLE001  # cleanup cannot mask terminal state
                    # It may have been actively running and already closed by
                    # cancellation, resumed concurrently, or failed while
                    # unwinding. Cleanup must not mask the parent outcome.
                    logger.debug("Subagent cleanup failed", exc_info=exc)
            await run.queue.put(_SENTINEL)
            # Tag the marker so a concurrent run cannot cause the global
            # stream to end before its already-queued events are delivered.
            await self._events.put((_SENTINEL, run.run_id))

    async def _drive(self, run: _Run, task: str) -> dict[str, Any]:
        """Drive start/resume generators one at a time."""
        stream: AsyncGenerator[Any, None] = run.session.start(task)
        try:
            while True:
                try:
                    event = await stream.__anext__()
                except StopAsyncIteration:
                    break
                is_hitl = _is_hitl(event)
                if is_hitl:
                    # Park before publishing the event so a fast parent can
                    # resume immediately without racing assignment of the
                    # resolver future. Closing the outer generator releases
                    # its frame before resume starts.
                    await stream.aclose()
                    run.pending = SubagentEvent(run.run_id, event)
                    run.waiter = asyncio.get_running_loop().create_future()
                await self._emit(run, event)
                if is_hitl:
                    try:
                        result = await run.waiter
                    finally:
                        run.waiter = None
                        run.pending = None
                    stream = run.session.resume(result)
            result = {
                "status": (
                    "completed"
                    if run.session.finished and run.session.reply_end_seen
                    else "error"
                ),
                "run_id": run.run_id,
                "result": "".join(run.text)[: self._max_result_chars],
            }
            if result["status"] != "completed":
                result["error"] = "subagent stream ended before ReplyEndEvent"
            run.result = result
            return result
        finally:
            await stream.aclose()

    async def _emit(
        self,
        run: _Run,
        event: AgentEvent | Msg | None,
        error: str | None = None,
    ) -> None:
        """Publish one child event to both global and run-specific streams."""
        wrapped = SubagentEvent(run.run_id, event, error)
        await self._events.put(wrapped)
        await run.queue.put(wrapped)
        # Deltas are reliable across all AgentScope event versions. End events
        # can omit their text when streaming, so use them only as a fallback.
        if event is not None:
            event_type = type(event).__name__
            if event_type == "TextBlockDeltaEvent":
                run.text.append(getattr(event, "delta", ""))
            elif event_type == "TextBlockEndEvent" and not run.text:
                text = getattr(event, "text", None)
                if text:
                    run.text.append(text)

    async def events(
        self,
        run_id: str | None = None,
    ) -> AsyncGenerator[SubagentEvent, None]:
        """Yield child events globally or for one run until it terminates."""
        queue: asyncio.Queue[Any]
        if run_id is None:
            queue = self._events
        else:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(f"unknown subagent run: {run_id}")
            queue = run.queue
        while True:
            event = await queue.get()
            if event is _SENTINEL:
                if run_id is not None:
                    return
                continue
            if isinstance(event, tuple) and len(event) == 2 and event[0] is _SENTINEL:
                if run_id is not None:
                    return
                # A marker can remain queued after a caller discards a run;
                # do not let that stale marker terminate a later invocation.
                if event[1] not in self._runs:
                    continue
                self._global_finished.add(event[1])
                # A global stream remains useful for concurrent children.
                # Every run gets a tagged marker, and markers are ordered
                # after that run's events in the queue.
                if len(self._global_finished) >= len(self._runs):
                    return
                continue
            yield event

    async def resume(
        self,
        run_id: str,
        event: UserConfirmResultEvent | ExternalExecutionResultEvent | SubagentEvent,
    ) -> None:
        """Resume a parked child after validating run and reply IDs."""
        if isinstance(event, SubagentEvent):
            if event.event is None:
                raise ValueError("subagent event does not contain a resume event")
            event = event.event
        if not _is_resume(event):
            raise TypeError(
                "event must be UserConfirmResultEvent or ExternalExecutionResultEvent",
            )
        run = self._runs.get(run_id)
        if run is None:
            raise KeyError(f"unknown subagent run: {run_id}")
        if run.pending is None or run.waiter is None:
            raise RuntimeError("subagent is not waiting for HITL")
        if event.reply_id != run.pending.reply_id:
            raise ValueError(
                f"resume reply_id {event.reply_id!r} does not match "
                f"pending reply {run.pending.reply_id!r}",
            )
        # Validate the result family before waking the child.  A mismatched
        # result must be retryable; passing it into Agent.reply_stream would
        # otherwise turn a parked HITL run into a terminal error.
        from ..event import (
            ExternalExecutionResultEvent,
            RequireExternalExecutionEvent,
            RequireUserConfirmEvent,
            UserConfirmResultEvent,
        )

        pending_event = run.pending.event
        if isinstance(pending_event, RequireUserConfirmEvent) and not isinstance(
            event, UserConfirmResultEvent
        ):
            raise TypeError("pending HITL requires UserConfirmResultEvent")
        if isinstance(pending_event, RequireExternalExecutionEvent) and not isinstance(
            event, ExternalExecutionResultEvent
        ):
            raise TypeError("pending HITL requires ExternalExecutionResultEvent")
        if run.waiter.done():
            raise RuntimeError("subagent HITL request was already resumed")
        run.waiter.set_result(event)

    async def cancel(self, run_id: str) -> dict[str, Any]:
        """Cancel one child run and wait until its worker is closed."""
        run = self._runs.get(run_id)
        if run is None:
            raise KeyError(f"unknown subagent run: {run_id}")
        if run.task is None:
            raise RuntimeError("subagent has not started")
        if not run.task.done():
            run.task.cancel()
            await asyncio.gather(run.task, return_exceptions=True)
        if run.result is not None:
            return run.result
        result = {"status": "cancelled", "run_id": run_id}
        run.result = result
        return result

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return a completed result or ``None`` while a run is active."""
        run = self._runs.get(run_id)
        return None if run is None else run.result

    def discard(self, run_id: str) -> None:
        """Forget a completed run; active or unknown runs are rejected."""
        run = self._runs.get(run_id)
        if run is None:
            raise KeyError(f"unknown subagent run: {run_id}")
        if run.task is None or not run.task.done():
            raise RuntimeError("cannot discard an active subagent run")
        del self._runs[run_id]


def create_subagent_toolkit(
    agent_factory: Callable[[], Any],
    **kwargs: Any,
) -> Toolkit:
    """Create an independent toolkit containing one resumable subagent tool."""
    return Toolkit(tools=[SubagentTool(agent_factory, **kwargs)])
