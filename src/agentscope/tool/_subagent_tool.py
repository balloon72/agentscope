"""Parent-agent bridge for stateful subagents.

Unlike :class:`OneShotSubagent`, ``SubagentTool`` keeps a worker reply parked
while it waits for a human or external result.  The worker's events are made
available to the parent application through :meth:`events`; the parent can
route a matching result back with :meth:`resume`.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import time
import uuid
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .._execution_context import (
    SubagentExecutionContext,
    call_factory_with_context,
    context_for_run,
)

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

# A bridge can be installed in a child toolkit as well as a parent toolkit.
# Keep the nesting level in a context variable so it propagates through the
# asyncio tasks created by an Agent, while remaining isolated between sibling
# parent requests.  ``max_depth=1`` permits a parent to delegate once and
# rejects an accidental child -> child recursion by default.
_SUBAGENT_DEPTH: contextvars.ContextVar[int] = contextvars.ContextVar(
    "agentscope_subagent_depth", default=0
)


@dataclass(frozen=True)
class SubagentEvent:
    """An event emitted by a child run, tagged with its parent run ID."""

    run_id: str
    event: AgentEvent | Msg | None
    error: str | None = None
    agent_id: str | None = None
    context: SubagentExecutionContext | None = None

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
    # Guard a single parked HITL request against duplicate deliveries.  This
    # is reset only when a new HITL event is emitted for the same run.
    resume_claimed: bool = False
    task: asyncio.Task[dict[str, Any]] | None = None
    result: dict[str, Any] | None = None
    text: list[str] = field(default_factory=list)
    agent_id: str | None = None
    context: Any = None
    parent_key: str = "__default__"
    started_at: float = field(default_factory=time.monotonic)
    terminal_at: float | None = None


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
        agent_factory: Callable[[], Any] | None = None,
        *,
        agent_resolver: Callable[..., Any] | None = None,
        resolver: Callable[..., Any] | None = None,
        collaborator_resolver: Callable[..., Any] | None = None,
        allowed_agent_ids: Iterable[str] | None = None,
        allowlist: Iterable[str] | None = None,
        agent_allowlist: Iterable[str] | None = None,
        name: str = "call_subagent",
        description: str = (
            "Delegate one task to a specialist subagent. The specialist may "
            "pause for user confirmation or external tool execution."
        ),
        timeout: float | None = None,
        hitl_timeout: float | None = None,
        hitl_ttl: float | None = None,
        max_concurrent: int = 8,
        max_concurrent_runs: int | None = None,
        max_concurrency: int | None = None,
        max_depth: int = 1,
        recursion_policy: str = "reject",
        max_task_chars: int = 32_000,
        max_result_chars: int = 8_000,
        max_event_queue: int = 256,
        event_queue_limit: int | None = None,
        max_queue_size: int | None = None,
        cleanup_ttl: float | None = 60.0,
        terminal_ttl: float | None = None,
        session_factory: Callable[[Callable[[], Any]], SubagentSession] | None = None,
    ) -> None:
        super().__init__()
        if not name:
            raise ValueError("name must not be empty")
        if max_result_chars <= 0:
            raise ValueError("max_result_chars must be positive")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if hitl_timeout is not None and hitl_timeout <= 0:
            raise ValueError("hitl_timeout must be positive")
        if hitl_ttl is not None and hitl_ttl <= 0:
            raise ValueError("hitl_ttl must be positive")
        if max_concurrent_runs is not None:
            max_concurrent = max_concurrent_runs
        if max_concurrency is not None:
            max_concurrent = max_concurrency
        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")
        if max_depth < 0:
            raise ValueError("max_depth must be non-negative")
        if recursion_policy not in {"reject", "allow"}:
            raise ValueError("recursion_policy must be 'reject' or 'allow'")
        if max_task_chars <= 0:
            raise ValueError("max_task_chars must be positive")
        if event_queue_limit is not None:
            max_event_queue = event_queue_limit
        if max_queue_size is not None:
            max_event_queue = max_queue_size
        if max_event_queue <= 0:
            raise ValueError("max_event_queue must be positive")
        if terminal_ttl is not None:
            cleanup_ttl = terminal_ttl
        if cleanup_ttl is not None and cleanup_ttl < 0:
            raise ValueError("cleanup_ttl must be non-negative or None")
        resolvers = [
            value
            for value in (agent_resolver, resolver, collaborator_resolver)
            if value is not None
        ]
        if len(resolvers) > 1 and any(
            value is not resolvers[0] for value in resolvers[1:]
        ):
            raise ValueError(
                "agent_resolver, resolver, and collaborator_resolver are aliases; "
                "provide only one",
            )
        resolver = resolvers[0] if resolvers else None
        allowlists = [
            value
            for value in (allowed_agent_ids, allowlist, agent_allowlist)
            if value is not None
        ]
        if len(allowlists) > 1 and any(
            value is not allowlists[0] for value in allowlists[1:]
        ):
            raise ValueError(
                "allowed_agent_ids, allowlist, and agent_allowlist are aliases; "
                "provide only one",
            )
        if agent_factory is None and resolver is None:
            raise ValueError("agent_factory or agent_resolver must be provided")
        self.name = name
        self.description = description
        self.input_schema = {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "The complete task for the specialist.",
                },
                "agent_id": {
                    "type": "string",
                    "description": (
                        "Optional collaborator identifier. When omitted, use "
                        "the default specialist factory."
                    ),
                },
            },
            "required": ["task"],
            "additionalProperties": False,
        }
        self._agent_factory = agent_factory
        self._agent_resolver = resolver
        self._allowed_agent_ids = (
            frozenset(allowlists[0]) if allowlists else None
        )
        if session_factory is None:
            # Import lazily: agentscope.agent imports agentscope.tool during
            # package initialization, so an eager import would cycle.
            from ..agent._subagent import SubagentSession

            session_factory = SubagentSession
        self._session_factory = session_factory
        self._timeout = timeout
        self._hitl_timeout = hitl_timeout if hitl_timeout is not None else hitl_ttl
        self._max_concurrent = max_concurrent
        self._max_depth = max_depth
        self._recursion_policy = recursion_policy
        self._max_task_chars = max_task_chars
        self._max_result_chars = max_result_chars
        self._max_event_queue = max_event_queue
        self._cleanup_ttl = cleanup_ttl
        self._runs: dict[str, _Run] = {}
        self._events: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_event_queue)
        self._global_finished: set[str] = set()
        self._active_count = 0
        self._active_by_parent: dict[str, int] = {}
        self._active_lock = asyncio.Lock()
        self._cleanup_tasks: set[asyncio.Task[None]] = set()

    @staticmethod
    def _error_result(
        run_id: str | None,
        agent_id: str | None,
        code: str,
        message: str,
    ) -> dict[str, Any]:
        """Build a stable, non-sensitive result for caller-visible errors."""
        result: dict[str, Any] = {
            "status": "error",
            "agent_id": agent_id,
            "error_code": code,
            "error": message,
        }
        if run_id is not None:
            result["run_id"] = run_id
        return result

    def _validate_agent_id(
        self,
        agent_id: Any,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """Validate the requested collaborator against the server allowlist."""
        if agent_id is None:
            if self._agent_resolver is not None and self._agent_factory is None:
                return None, self._error_result(
                    None,
                    None,
                    "agent_id_required",
                    "agent_id is required when using a collaborator resolver",
                )
            return None, None
        if not isinstance(agent_id, str) or not agent_id.strip():
            return None, self._error_result(
                None,
                agent_id if isinstance(agent_id, str) else None,
                "invalid_agent_id",
                "agent_id must be a non-empty string",
            )
        if (
            self._allowed_agent_ids is not None
            and agent_id not in self._allowed_agent_ids
        ):
            return None, self._error_result(
                None,
                agent_id,
                "agent_id_not_allowed",
                "agent_id is not allowed",
            )
        if self._agent_resolver is None:
            return None, self._error_result(
                None,
                agent_id,
                "agent_resolver_unavailable",
                "agent_id cannot be used without a collaborator resolver",
            )
        return agent_id, None

    async def _invoke_resolver(self, agent_id: str, context: Any = None) -> Any:
        """Invoke sync/async resolver functions with optional request context.

        The normal resolver contract is ``resolver(agent_id)``.  A second
        context parameter is supported for per-request authorization and
        tenancy.  Mapping contexts may also be expanded into named resolver
        arguments, which keeps app integrations from needing an adapter.
        """
        resolver = self._agent_resolver
        assert resolver is not None
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
                # The tool argument is authoritative; request context cannot
                # smuggle a second collaborator ID past the allowlist.
                kwargs["agent_id"] = agent_id
                try:
                    signature.bind(**kwargs)
                except TypeError:
                    # Fall back to positional context below if the mapping
                    # does not describe this resolver's parameter names.
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
            # Keep the validated tool argument authoritative even when the
            # context object also happens to carry an ``agent_id`` field.
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

    async def _resolve_factory(
        self,
        agent_id: str | None,
        context: Any = None,
    ) -> tuple[Callable[[], Any] | None, dict[str, Any] | None]:
        """Resolve a worker factory for one run without exposing exceptions."""
        validated, error = self._validate_agent_id(agent_id)
        if error is not None:
            return None, error
        if validated is None:
            assert self._agent_factory is not None
            return self._agent_factory, None
        try:
            worker = await self._invoke_resolver(validated, context)
            if worker is None:
                return None, self._error_result(
                    None,
                    validated,
                    "agent_not_found",
                    "collaborator could not be resolved",
                )
            # Resolvers may return an already-created Agent or a factory.  A
            # callable with Agent's reply API is an agent; other callables are
            # treated as factories and invoked once for this ephemeral run.
            if inspect.isclass(worker) or (
                callable(worker)
                and not callable(getattr(worker, "reply_stream", None))
            ):
                worker = worker()
                if inspect.isawaitable(worker):
                    worker = await worker
            if worker is None:
                return None, self._error_result(
                    None,
                    validated,
                    "agent_not_found",
                    "collaborator could not be resolved",
                )
            if not callable(getattr(worker, "reply_stream", None)):
                return None, self._error_result(
                    None,
                    validated,
                    "invalid_agent",
                    "resolved collaborator is not executable",
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - details stay in logs only
            logger.debug("Collaborator resolver failed", exc_info=exc)
            return None, self._error_result(
                None,
                validated,
                "agent_resolver_error",
                "collaborator resolver failed",
            )
        # Keep a zero-argument signature so execution-context adaptation does
        # not accidentally pass the request context as the worker instance.
        def resolved_factory() -> Any:
            return worker

        return resolved_factory, None

    async def check_permissions(
        self,
        tool_input: dict[str, Any],
        context: PermissionContext,
    ) -> PermissionDecision:
        """Validate collaborator routing; worker tools retain permissions."""
        _, error = self._validate_agent_id(tool_input.get("agent_id"))
        if error is not None:
            return PermissionDecision(
                behavior=PermissionBehavior.DENY,
                message=error["error"],
            )
        return PermissionDecision(
            behavior=PermissionBehavior.ALLOW,
            message=(
                "Subagent delegation is allowed; worker tools retain their "
                "permissions."
            ),
        )

    async def call(
        self,
        task: str,
        agent_id: str | None = None,
        context: Any = None,
    ) -> ToolChunk:
        """Run a child task and return its JSON result to the parent model."""
        result = await self.run(task, agent_id=agent_id, context=context)
        return ToolChunk(
            content=[
                TextBlock(
                    text=json.dumps(
                        result,
                        ensure_ascii=False,
                        default=(
                            lambda value: value.to_dict()
                            if isinstance(value, SubagentExecutionContext)
                            else str(value)
                        ),
                    ),
                ),
            ],
            state=(
                ToolResultState.SUCCESS
                if result.get("status") == "completed"
                else ToolResultState.ERROR
            ),
        )

    @staticmethod
    def _parent_key(context: Any) -> str:
        """Return the concurrency bucket for a parent invocation."""
        if isinstance(context, SubagentExecutionContext):
            return context.parent_run_id or "__default__"
        return "__default__"

    async def _release_slot(self, parent_key: str) -> None:
        """Release one active run from its parent bucket."""
        async with self._active_lock:
            self._active_count = max(0, self._active_count - 1)
            count = self._active_by_parent.get(parent_key, 0) - 1
            if count > 0:
                self._active_by_parent[parent_key] = count
            else:
                self._active_by_parent.pop(parent_key, None)

    async def run(
        self,
        task: str,
        agent_id: str | None = None,
        context: Any = None,
    ) -> dict[str, Any]:
        """Run a child task, waiting through any number of HITL turns."""
        if not isinstance(task, str) or not task.strip():
            return self._error_result(
                None,
                agent_id,
                "invalid_task",
                "task must not be empty",
            )
        if len(task) > self._max_task_chars:
            return self._error_result(
                None,
                agent_id,
                "task_too_large",
                f"task exceeds max_task_chars ({self._max_task_chars})",
            )
        depth = _SUBAGENT_DEPTH.get()
        if self._recursion_policy == "reject" and depth >= self._max_depth:
            return self._error_result(
                None,
                agent_id,
                "recursion_limit",
                "subagent recursion depth exceeded "
                f"(max_depth={self._max_depth})",
            )
        _, validation_error = self._validate_agent_id(agent_id)
        if validation_error is not None:
            validation_error["agent_id"] = agent_id
            return validation_error
        parent_key = self._parent_key(context)
        async with self._active_lock:
            if self._active_by_parent.get(parent_key, 0) >= self._max_concurrent:
                return self._error_result(
                    None,
                    agent_id,
                    "concurrency_limit",
                    "maximum concurrent subagent runs reached "
                    f"({self._max_concurrent})",
                )
            self._active_count += 1
            self._active_by_parent[parent_key] = (
                self._active_by_parent.get(parent_key, 0) + 1
            )
        run_id = (
            context.subagent_run_id
            if isinstance(context, SubagentExecutionContext)
            and context.subagent_run_id
            else uuid.uuid4().hex
        )
        if isinstance(context, SubagentExecutionContext):
            try:
                context = context_for_run(context, run_id)
            except ValueError as exc:
                await self._release_slot(parent_key)
                return self._error_result(
                    run_id,
                    agent_id,
                    "invalid_context",
                    str(exc),
                )
        # A caller-provided child ID is a routing capability, not a reusable
        # correlation key.  Reject collisions (including retained terminal
        # runs) instead of replacing the first run in ``_runs`` and allowing
        # a later resume to drive the wrong worker.
        if run_id in self._runs:
            await self._release_slot(parent_key)
            return self._error_result(
                run_id,
                agent_id,
                "run_id_in_use",
                "subagent run ID is already in use",
            )
        try:
            factory, resolver_error = await self._resolve_factory(
                agent_id,
                context,
            )
            if resolver_error is not None:
                await self._release_slot(parent_key)
                resolver_error["run_id"] = run_id
                resolver_error["agent_id"] = agent_id
                return resolver_error
            assert factory is not None
            try:
                session = call_factory_with_context(
                    self._session_factory,
                    context,
                    factory,
                )
            except Exception:  # noqa: BLE001  # factory errors belong in parent result
                await self._release_slot(parent_key)
                return self._error_result(
                    run_id,
                    agent_id,
                    "session_factory_error",
                    "subagent session could not be created",
                )
        except Exception as exc:  # noqa: BLE001  # factory errors belong in parent result
            await self._release_slot(parent_key)
            logger.debug("Subagent resolver setup failed", exc_info=exc)
            return self._error_result(
                run_id,
                agent_id,
                "agent_resolver_error",
                "collaborator resolver failed",
            )
        run = _Run(
            run_id=run_id,
            session=session,
            queue=asyncio.Queue(maxsize=self._max_event_queue),
            agent_id=agent_id,
            context=context,
            parent_key=parent_key,
        )
        self._runs[run.run_id] = run
        run.task = asyncio.create_task(self._execute(run, task))
        try:
            return await run.task
        except asyncio.CancelledError:
            # Parent cancellation must never retain a worker or a resolver.
            if run.task is not asyncio.current_task() and not run.task.done():
                run.task.cancel()
                await asyncio.gather(run.task, return_exceptions=True)
            # Cancellation can arrive in the same turn as create_task(),
            # before the child coroutine has entered _execute (and therefore
            # before its finally block can release the active slot).  Finalize
            # that never-started task here so limits and run bookkeeping do
            # not leak across subsequent parent turns.
            if run.terminal_at is None:
                await self._finalize_cancelled_run(run)
            raise

    async def _finalize_cancelled_run(self, run: _Run) -> None:
        """Finalize a run cancelled before its worker coroutine starts."""
        if run.terminal_at is not None:
            return
        run.result = {
            "status": "cancelled",
            "run_id": run.run_id,
            "agent_id": run.agent_id,
        }
        if run.context is not None:
            run.result["context"] = run.context
        await self._put_bounded(run.queue, _SENTINEL)
        await self._put_bounded(self._events, (_SENTINEL, run.run_id))
        run.terminal_at = time.monotonic()
        await self._release_slot(run.parent_key)
        self._schedule_cleanup(run.run_id)

    async def _execute(self, run: _Run, task: str) -> dict[str, Any]:
        """Drive session streams and park at child HITL events."""
        depth_token = _SUBAGENT_DEPTH.set(_SUBAGENT_DEPTH.get() + 1)
        try:
            if self._timeout is None:
                return await self._drive(run, task)
            return await asyncio.wait_for(self._drive(run, task), self._timeout)
        except TimeoutError:
            run.result = {
                "status": "timeout",
                "run_id": run.run_id,
                "agent_id": run.agent_id,
                "error": "subagent timed out",
            }
            if run.context is not None:
                run.result["context"] = run.context
            return run.result
        except asyncio.CancelledError:
            # _drive closes its current stream and the SubagentSession marks
            # cancellation terminal. Keep a terminal record even though the
            # parent awaiter still receives CancelledError.
            run.result = {
                "status": "cancelled",
                "run_id": run.run_id,
                "agent_id": run.agent_id,
            }
            if run.context is not None:
                run.result["context"] = run.context
            raise
        except Exception as exc:  # noqa: BLE001  # worker errors belong in parent result
            run.result = {
                "status": "error",
                "run_id": run.run_id,
                "agent_id": run.agent_id,
                "error": f"{type(exc).__name__}: {exc}",
            }
            if run.context is not None:
                run.result["context"] = run.context
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
            await self._put_bounded(run.queue, _SENTINEL)
            # Tag the marker so a concurrent run cannot cause the global
            # stream to end before its already-queued events are delivered.
            await self._put_bounded(self._events, (_SENTINEL, run.run_id))
            run.terminal_at = time.monotonic()
            await self._release_slot(run.parent_key)
            _SUBAGENT_DEPTH.reset(depth_token)
            self._schedule_cleanup(run.run_id)

    async def _drive(self, run: _Run, task: str) -> dict[str, Any]:
        """Drive start/resume generators one at a time."""
        start = run.session.start
        try:
            start_signature = inspect.signature(start)
        except (TypeError, ValueError):
            start_signature = None
        supports_context = (
            start_signature is not None
            and "context" in start_signature.parameters
        )
        # Custom sessions written before context propagation may only accept
        # the task positional argument.
        stream: AsyncGenerator[Any, None] = (
            start(task, context=run.context) if supports_context else start(task)
        )
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
                    run.pending = SubagentEvent(
                        run.run_id,
                        event,
                        agent_id=run.agent_id,
                        context=run.context,
                    )
                    run.resume_claimed = False
                    run.waiter = asyncio.get_running_loop().create_future()
                await self._emit(run, event)
                if is_hitl:
                    try:
                        if self._hitl_timeout is None:
                            result = await run.waiter
                        else:
                            result = await asyncio.wait_for(
                                run.waiter,
                                timeout=self._hitl_timeout,
                            )
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
                "agent_id": run.agent_id,
                "result": "".join(run.text)[: self._max_result_chars],
            }
            if result["status"] != "completed":
                result["error"] = "subagent stream ended before ReplyEndEvent"
            if run.context is not None:
                result["context"] = run.context
            run.result = result
            return result
        finally:
            await stream.aclose()

    @staticmethod
    async def _put_bounded(queue: asyncio.Queue[Any], value: Any) -> None:
        """Put an event without allowing a slow consumer to retain memory.

        Event streams are intentionally lossy under pressure: the newest
        event (especially a HITL request or terminal marker) is more useful
        than an arbitrarily old delta.  ``events(run_id)`` remains ordered for
        all entries that fit in the configured window.
        """
        while queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            queue.put_nowait(value)
        except asyncio.QueueFull:
            # A race with another producer is harmless; terminal cleanup must
            # never be held hostage by an abandoned stream consumer.
            return

    def _schedule_cleanup(self, run_id: str) -> None:
        """Schedule removal of a terminal run after its retention TTL."""
        if self._cleanup_ttl is None:
            return

        async def cleanup() -> None:
            if self._cleanup_ttl:
                await asyncio.sleep(self._cleanup_ttl)
            run = self._runs.get(run_id)
            if run is not None and run.terminal_at is not None:
                self._runs.pop(run_id, None)
                self._global_finished.discard(run_id)

        task = asyncio.create_task(cleanup())
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_tasks.discard)

    async def _emit(
        self,
        run: _Run,
        event: AgentEvent | Msg | None,
        error: str | None = None,
    ) -> None:
        """Publish one child event to both global and run-specific streams."""
        wrapped = SubagentEvent(
            run.run_id,
            event,
            error,
            run.agent_id,
            run.context
            if isinstance(run.context, SubagentExecutionContext)
            else None,
        )
        await self._put_bounded(self._events, wrapped)
        await self._put_bounded(run.queue, wrapped)
        # Deltas are reliable across all AgentScope event versions. End events
        # can omit their text when streaming, so use them only as a fallback.
        if event is not None:
            event_type = type(event).__name__
            if event_type == "TextBlockDeltaEvent":
                # Bound accumulation as well as the final serialized result;
                # a child that streams indefinitely must not grow memory even
                # when the caller never asks for its result.
                delta = str(getattr(event, "delta", ""))
                used = sum(len(part) for part in run.text)
                if used < self._max_result_chars:
                    run.text.append(delta[: self._max_result_chars - used])
            elif event_type == "TextBlockEndEvent" and not run.text:
                text = getattr(event, "text", None)
                if text:
                    run.text.append(str(text)[: self._max_result_chars])

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
        context: SubagentExecutionContext | None = None,
    ) -> None:
        """Resume a parked child after validating run and reply IDs."""
        if isinstance(event, SubagentEvent):
            if event.run_id != run_id:
                raise ValueError("subagent event run_id does not match run")
            if context is None:
                context = event.context
            elif event.context is not None and event.context != context:
                raise ValueError("subagent execution context does not match event")
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
        if isinstance(run.context, SubagentExecutionContext):
            # Context-bound runs must carry the exact immutable binding back
            # on resume.  Omitting it would allow a caller that only knows a
            # run/reply pair to drive another parent's parked child.
            if context is None:
                raise ValueError("subagent execution context is required for run")
            try:
                context = context_for_run(context, run_id)
            except ValueError:
                raise ValueError("subagent execution context does not match run") from None
            if context.parent_run_id != run.context.parent_run_id:
                raise ValueError("subagent execution context does not match parent")
            if context != run.context:
                raise ValueError("subagent execution context does not match run")
        elif context is not None and run.context != context:
            raise ValueError("subagent execution context does not match run")
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
        if run.resume_claimed or run.waiter.done():
            raise RuntimeError("subagent HITL request was already resumed")
        run.resume_claimed = True
        run.waiter.set_result(event)

    async def cancel(
        self,
        run_id: str,
        context: SubagentExecutionContext | None = None,
    ) -> dict[str, Any]:
        """Cancel one child run and wait until its worker is closed."""
        run = self._runs.get(run_id)
        if run is None:
            raise KeyError(f"unknown subagent run: {run_id}")
        if run.task is None:
            raise RuntimeError("subagent has not started")
        if context is not None and run.context != context:
            raise ValueError("subagent execution context does not match run")
        if not run.task.done():
            run.task.cancel()
            await asyncio.gather(run.task, return_exceptions=True)
        if run.result is not None:
            return run.result
        result = {
            "status": "cancelled",
            "run_id": run_id,
            "agent_id": run.agent_id,
        }
        if run.context is not None:
            result["context"] = run.context
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
    agent_factory: Callable[[], Any] | None = None,
    **kwargs: Any,
) -> Toolkit:
    """Create an independent toolkit containing one resumable subagent tool."""
    return Toolkit(tools=[SubagentTool(agent_factory, **kwargs)])
