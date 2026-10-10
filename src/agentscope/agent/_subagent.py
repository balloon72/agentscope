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
    def finished(self) -> bool:
        """Whether the worker run reached a terminal state."""
        return self._finished

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
                    self._pending_reply_id = event.reply_id
                    # A HITL event parks the stream. Let the caller resume the
                    # worker even if it consumed only this first event via
                    # ``__anext__`` instead of exhausting the generator.
                    await self._close_stream(stream)
                    self._driving = False
                elif isinstance(event, ReplyEndEvent):
                    self._pending_reply_id = None
                    self._finished = True
                    self._driving = False
                    await self._close_stream(stream)
                    await self._close_worker()
                yield event
        except (Exception, asyncio.CancelledError):
            # Errors and task cancellation are terminal for a one-shot run.
            # Marking it finished lets the manager discard the run and avoids
            # retaining a worker after a failed/cancelled stream.
            self._pending_reply_id = None
            self._finished = True
            self._driving = False
            if stream is not None:
                await self._close_stream(stream)
            await self._close_worker()
            raise
        except GeneratorExit:
            # A caller may consume just the HITL event with ``__anext__`` and
            # drop the stream object. A parked run remains resumable, while a
            # non-parked stream is terminal and can be cleaned up.
            self._driving = False
            if self._pending_reply_id is None:
                self._finished = True
                if stream is not None:
                    await self._close_stream(stream)
                await self._close_worker()
            raise
        else:
            self._driving = False
            # A malformed worker that ends without ReplyEndEvent must not leave
            # an undiscardable manager entry forever. Preserve a parked HITL
            # stream (pending_reply_id is set), otherwise treat exhaustion as
            # terminal and release the worker.
            if not self._finished and self._pending_reply_id is None:
                self._finished = True
                if stream is not None:
                    await self._close_stream(stream)
                await self._close_worker()

    @staticmethod
    async def _close_stream(stream: object) -> None:
        """Close a parked async generator without masking the result."""
        closer = getattr(stream, "aclose", None)
        if closer is None:
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
            if closer is None:
                continue
            try:
                result = closer()
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001 - cleanup must not mask the result
                # Cleanup must not mask the worker's terminal result/error.
                return
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
