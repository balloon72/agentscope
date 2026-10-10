"""Composable subagent sessions with resumable HITL turns."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncGenerator, Callable
from typing import TYPE_CHECKING

from ..event import (
    AgentEvent,
    ExternalExecutionResultEvent,
    ReplyEndEvent,
    ReplyStartEvent,
    RequireExternalExecutionEvent,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
    UserInterruptEvent,
)
from ..message import Msg, TextBlock

if TYPE_CHECKING:
    from ._agent import Agent


_HITL_EVENT = (
    RequireUserConfirmEvent,
    RequireExternalExecutionEvent,
)
_RESUME_EVENT = (
    UserConfirmResultEvent,
    ExternalExecutionResultEvent,
)


class SubagentSession:
    """A parent-owned subagent run that can pause and resume on HITL.

    A worker is created once per ``start`` call and retained only while that
    run is active or waiting for a human/external result. It is discarded by
    the caller after a terminal ``ReplyEndEvent`` or failed/cancelled run.
    This gives a one-task subagent the complete AgentScope reply protocol
    without making the worker a permanent conversation member.
    """

    def __init__(self, agent_factory: Callable[[], Agent]) -> None:
        self._agent_factory = agent_factory
        self._worker: Agent | None = None
        self._started = False
        self._finished = False
        self._pending_reply_id: str | None = None
        self._pending_event_type: type[AgentEvent] | None = None
        # Keep the identity of the worker reply across a start/resume turn.
        # ``Require*`` events carry only ``reply_id`` while ``ReplyStart`` and
        # ``ReplyEnd`` carry both reply and session IDs.  Tracking both lets us
        # reject a malformed worker stream instead of routing a result to the
        # wrong continuation.
        self._reply_id: str | None = None
        self._session_id: str | None = None
        # A single worker reply may only be driven by one caller at a time.
        # This is intentionally a lightweight guard rather than an asyncio
        # lock: ``reply_stream`` is an async generator and can be parked at a
        # HITL event while its caller is still consuming the stream.
        self._driving = False

    @property
    def pending_reply_id(self) -> str | None:
        """The worker reply ID awaiting human/external input, if any."""
        return self._pending_reply_id

    @property
    def pending_event_type(self) -> type[AgentEvent] | None:
        """The HITL event class awaiting a matching result, if any."""
        return self._pending_event_type

    @property
    def finished(self) -> bool:
        """Whether the worker run reached a terminal state."""
        return self._finished

    @property
    def reply_id(self) -> str | None:
        """The worker reply ID for this run, when one has been emitted."""
        return self._reply_id

    @property
    def session_id(self) -> str | None:
        """The worker session ID for this run, when one has been emitted."""
        return self._session_id

    async def start(self, task: str) -> AsyncGenerator[AgentEvent | Msg, None]:
        """Start a fresh worker and stream its events."""
        if self._started:
            raise RuntimeError("subagent session has already started")
        if not task.strip():
            raise ValueError("task must not be empty")
        self._started = True
        try:
            self._worker = self._agent_factory()
        except Exception:
            self._finished = True
            self._pending_reply_id = None
            self._pending_event_type = None
            self._reply_id = None
            self._session_id = None
            self._worker = None
            raise
        request = Msg(name="parent", role="user", content=[TextBlock(text=task)])
        drive = self._drive(request)
        try:
            async for event in drive:
                yield event
        finally:
            await self._close_stream(drive)

    async def resume(
        self,
        event: UserConfirmResultEvent | ExternalExecutionResultEvent,
    ) -> AsyncGenerator[AgentEvent | Msg, None]:
        """Resume the parked worker after a HITL/external result."""
        if not isinstance(event, _RESUME_EVENT):
            raise TypeError(
                "event must be UserConfirmResultEvent or "
                "ExternalExecutionResultEvent",
            )
        if not self._started:
            raise RuntimeError("subagent session has not started")
        if self._finished:
            raise RuntimeError("subagent session has finished")
        if self._worker is None:
            raise RuntimeError("subagent worker is unavailable")
        if self._pending_reply_id != event.reply_id:
            raise ValueError(
                "resume reply_id does not match the pending subagent reply",
            )
        if self._pending_event_type is RequireUserConfirmEvent and not isinstance(
            event,
            UserConfirmResultEvent,
        ):
            raise TypeError(
                "pending subagent reply requires UserConfirmResultEvent",
            )
        if self._pending_event_type is RequireExternalExecutionEvent and not isinstance(
            event,
            ExternalExecutionResultEvent,
        ):
            raise TypeError(
                "pending subagent reply requires ExternalExecutionResultEvent",
            )
        drive = self._drive(event)
        try:
            async for output in drive:
                yield output
        finally:
            await self._close_stream(drive)

    async def interrupt(self) -> AsyncGenerator[AgentEvent | Msg, None]:
        """Interrupt a parked worker and close the run."""
        if self._worker is None or self._finished:
            return
        if self._pending_reply_id is None:
            raise RuntimeError("subagent is not waiting for HITL")
        drive = self._drive(
            UserInterruptEvent(reply_id=self._pending_reply_id),
        )
        try:
            async for event in drive:
                yield event
        finally:
            await self._close_stream(drive)

    async def _drive(
        self,
        inputs: Msg
        | UserConfirmResultEvent
        | ExternalExecutionResultEvent
        | UserInterruptEvent,
    ) -> AsyncGenerator[AgentEvent | Msg, None]:
        assert self._worker is not None
        if self._driving:
            raise RuntimeError("subagent session already has a reply in flight")
        self._driving = True
        stream: object | None = None
        try:
            stream = self._worker.reply_stream(inputs)
            async for event in stream:
                if isinstance(event, _HITL_EVENT):
                    self._record_reply_id(event.reply_id)
                    self._pending_reply_id = event.reply_id
                    self._pending_event_type = type(event)
                    # A HITL event parks the stream. Let the caller resume the
                    # worker even if it consumed only this first event via
                    # ``__anext__`` instead of exhausting the generator.
                    await self._close_stream(stream)
                    stream = None
                    self._driving = False
                    yield event
                    return
                if isinstance(event, ReplyStartEvent):
                    self._record_reply_start(event)
                elif isinstance(event, ReplyEndEvent):
                    self._record_reply_end(event)
                    self._pending_reply_id = None
                    self._pending_event_type = None
                yield event
        except ValueError:
            # AgentScope validates HITL payloads against the parked tool
            # calls.  A malformed confirmation/external result leaves the
            # worker parked and is retryable; do not discard the worker or
            # consume the pending continuation on this error.
            if isinstance(inputs, _RESUME_EVENT) and self._pending_reply_id:
                self._driving = False
                if stream is not None:
                    await self._close_stream(stream)
                raise
            self._pending_reply_id = None
            self._pending_event_type = None
            self._finished = True
            self._driving = False
            if stream is not None:
                await self._close_stream(stream)
            await self._close_worker()
            raise
        except (Exception, asyncio.CancelledError):
            # Errors and task cancellation are terminal for a one-shot run.
            # Marking it finished lets the manager discard the run and avoids
            # retaining a worker after a failed/cancelled stream.
            self._pending_reply_id = None
            self._pending_event_type = None
            self._pending_event_type = None
            self._finished = True
            self._driving = False
            if stream is not None:
                await self._close_stream(stream)
            await self._close_worker()
            raise
        else:
            # A malformed worker that ends without ReplyEndEvent must not leave
            # an undiscardable manager entry forever. Preserve a parked HITL
            # stream (pending_reply_id is set), otherwise treat exhaustion as
            # terminal and release the worker.  Do this *after* the stream has
            # been exhausted: AgentScope's reply generator may perform state
            # persistence or emit a final message after ReplyEndEvent.
            if self._pending_reply_id is None:
                # Mark completion only after the worker stream has naturally
                # exhausted.  In particular, AgentScope may persist reply
                # state or emit a final Msg after ReplyEndEvent; setting
                # ``finished`` at the marker would let a manager discard this
                # session before that tail is consumed.
                self._finished = True
                if stream is not None:
                    await self._close_stream(stream)
                await self._close_worker()
        finally:
            # ``GeneratorExit`` from a caller that stops after one event must
            # still release a terminal worker, while a parked HITL run keeps
            # its worker alive for ``resume``.
            self._driving = False
            if stream is not None:
                await self._close_stream(stream)
            if self._pending_reply_id is None and not self._finished:
                # GeneratorExit can arrive before the stream naturally
                # exhausts (for example, a caller only asks for the first
                # message).  There is no resumable continuation in that
                # case, so mark the one-shot run terminal as well.
                self._finished = True
            if self._pending_reply_id is None and self._finished:
                await self._close_worker()

    def _record_reply_id(self, reply_id: str) -> None:
        """Record/validate one worker reply ID."""
        if self._reply_id is None:
            self._reply_id = reply_id
        elif self._reply_id != reply_id:
            raise RuntimeError(
                "subagent emitted inconsistent reply_id values: "
                f"expected {self._reply_id!r}, got {reply_id!r}",
            )

    def _record_reply_start(self, event: ReplyStartEvent) -> None:
        """Record and validate a worker ``ReplyStartEvent``."""
        self._record_reply_id(event.reply_id)
        if self._session_id is None:
            self._session_id = event.session_id
        elif self._session_id != event.session_id:
            raise RuntimeError(
                "subagent emitted inconsistent session_id values: "
                f"expected {self._session_id!r}, got {event.session_id!r}",
            )

    def _record_reply_end(self, event: ReplyEndEvent) -> None:
        """Record and validate a worker ``ReplyEndEvent``."""
        self._record_reply_id(event.reply_id)
        if self._session_id is None:
            self._session_id = event.session_id
        elif self._session_id != event.session_id:
            raise RuntimeError(
                "subagent emitted inconsistent session_id values: "
                f"expected {self._session_id!r}, got {event.session_id!r}",
            )

    @staticmethod
    async def _close_stream(stream: object) -> None:
        """Close a parked async generator without masking the result."""
        closer = getattr(stream, "aclose", None)
        if not callable(closer):
            return
        try:
            result = closer()
            if inspect.isawaitable(result):
                await result
        except Exception:  # noqa: BLE001 - cleanup must not mask the result
            return

    async def _close_worker(self) -> None:
        """Best-effort cleanup for workers that expose a close/aclose hook."""
        worker = self._worker
        self._worker = None
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
                # Cleanup must not mask the worker's terminal result/error.
                pass
            else:
                break


class SubagentManager:
    """Manage multiple isolated subagent runs and route HITL by run ID."""

    def __init__(self, agent_factory: Callable[[], Agent]) -> None:
        self._agent_factory = agent_factory
        self._runs: dict[str, SubagentSession] = {}

    def create(self, run_id: str) -> SubagentSession:
        """Create a run, rejecting accidental run ID reuse."""
        if run_id in self._runs:
            raise ValueError(f"subagent run already exists: {run_id}")
        session = SubagentSession(self._agent_factory)
        self._runs[run_id] = session
        return session

    def get(self, run_id: str) -> SubagentSession:
        """Get an active run by its parent-generated ID."""
        try:
            return self._runs[run_id]
        except KeyError as exc:
            raise KeyError(f"unknown subagent run: {run_id}") from exc

    def discard(self, run_id: str) -> None:
        """Discard a finished run and its worker state."""
        session = self.get(run_id)
        if not session.finished:
            raise RuntimeError("cannot discard an unfinished subagent run")
        del self._runs[run_id]
