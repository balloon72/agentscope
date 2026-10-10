import asyncio

import pytest

from agentscope.agent import SubagentManager, SubagentSession
from agentscope.event import (
    ExternalExecutionResultEvent,
    ReplyEndEvent,
    ReplyStartEvent,
    RequireExternalExecutionEvent,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
    UserInterruptEvent,
)
from agentscope.message import Msg, TextBlock, ToolCallBlock
from agentscope.types import ReplyFinishedReason


class FakeWorker:
    created = 0

    def __init__(self) -> None:
        type(self).created += 1
        self.calls = []

    async def reply_stream(self, inputs):
        self.calls.append(inputs)
        if len(self.calls) == 1:
            yield RequireUserConfirmEvent(
                reply_id="worker-reply-1",
                tool_calls=[
                    ToolCallBlock(
                        id="call-1",
                        name="dangerous_action",
                        input="{}",
                    ),
                ],
            )
            return
        yield Msg(
            name="worker",
            role="assistant",
            content=[TextBlock(text="finished")],
        )
        yield ReplyEndEvent(
            session_id="worker-session",
            reply_id="worker-reply-1",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )


@pytest.mark.asyncio
async def test_hilt_pauses_and_resumes_by_reply_id() -> None:
    worker = FakeWorker()
    session = SubagentSession(lambda: worker)

    pending = [event async for event in session.start("do it")]
    assert isinstance(pending[0], RequireUserConfirmEvent)
    assert session.pending_reply_id == "worker-reply-1"
    assert not session.finished

    resumed = [
        event
        async for event in session.resume(
            UserConfirmResultEvent(reply_id="worker-reply-1", confirm_results=[]),
        )
    ]
    assert any(isinstance(event, Msg) for event in resumed)
    assert session.pending_reply_id is None
    assert session.finished


@pytest.mark.asyncio
async def test_manager_isolates_runs_and_rejects_wrong_reply_id() -> None:
    manager = SubagentManager(FakeWorker)
    first = manager.create("run-1")
    second = manager.create("run-2")
    await first.start("one").__anext__()
    await second.start("two").__anext__()

    with pytest.raises(ValueError, match="reply_id"):
        [
            event
            async for event in first.resume(
                UserConfirmResultEvent(reply_id="wrong", confirm_results=[]),
            )
        ]
    assert first.pending_reply_id == "worker-reply-1"
    assert second.pending_reply_id == "worker-reply-1"


class ExternalWorker:
    async def reply_stream(self, inputs):
        if isinstance(inputs, Msg):
            yield RequireExternalExecutionEvent(
                reply_id="external-reply",
                tool_calls=[],
            )
            return
        assert isinstance(inputs, ExternalExecutionResultEvent)
        yield ReplyEndEvent(
            session_id="worker-session",
            reply_id="external-reply",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )


@pytest.mark.asyncio
async def test_external_execution_can_resume_and_finish() -> None:
    session = SubagentSession(ExternalWorker)

    pending = [event async for event in session.start("run external")]
    assert isinstance(pending[0], RequireExternalExecutionEvent)
    assert session.pending_reply_id == "external-reply"

    resumed = [
        event
        async for event in session.resume(
            ExternalExecutionResultEvent(
                reply_id="external-reply",
                execution_results=[],
            ),
        )
    ]
    assert isinstance(resumed[-1], ReplyEndEvent)
    assert session.finished
    assert session.pending_reply_id is None


class InterruptWorker:
    async def reply_stream(self, inputs):
        if isinstance(inputs, Msg):
            yield RequireUserConfirmEvent(reply_id="interrupt-reply", tool_calls=[])
            return
        assert isinstance(inputs, UserInterruptEvent)
        yield ReplyEndEvent(
            session_id="worker-session",
            reply_id="interrupt-reply",
            finished_reason=ReplyFinishedReason.INTERRUPTED,
        )


@pytest.mark.asyncio
async def test_interrupt_closes_a_parked_run_and_rejects_duplicate_resume() -> None:
    session = SubagentSession(InterruptWorker)
    [event async for event in session.start("interrupt me")]

    interrupted = [event async for event in session.interrupt()]
    assert interrupted[-1].finished_reason == ReplyFinishedReason.INTERRUPTED
    assert session.finished
    assert session.pending_reply_id is None
    with pytest.raises(RuntimeError, match="finished"):
        [
            event
            async for event in session.resume(
                UserConfirmResultEvent(reply_id="interrupt-reply", confirm_results=[]),
            )
        ]


class ConcurrentResumeWorker:
    def __init__(self) -> None:
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def reply_stream(self, inputs):
        self.calls += 1
        if self.calls == 1:
            yield RequireUserConfirmEvent(reply_id="race-reply", tool_calls=[])
            return
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.entered.set()
        try:
            await self.release.wait()
            yield ReplyEndEvent(
                session_id="worker-session",
                reply_id="race-reply",
                finished_reason=ReplyFinishedReason.COMPLETED,
            )
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_concurrent_resume_is_rejected_and_worker_stays_isolated() -> None:
    worker = ConcurrentResumeWorker()
    session = SubagentSession(lambda: worker)
    [event async for event in session.start("race")]
    result = UserConfirmResultEvent(reply_id="race-reply", confirm_results=[])

    async def consume():
        return [event async for event in session.resume(result)]

    first = asyncio.create_task(consume())
    await worker.entered.wait()
    second = asyncio.create_task(consume())
    with pytest.raises(RuntimeError, match="in flight"):
        await second
    worker.release.set()
    await first
    assert worker.max_active == 1
    assert session.finished


@pytest.mark.asyncio
async def test_factory_failure_marks_run_finished_for_manager_cleanup() -> None:
    def factory():
        raise RuntimeError("factory failed")

    manager = SubagentManager(factory)
    manager.create("factory-run")
    session = manager.get("factory-run")
    with pytest.raises(RuntimeError, match="factory failed"):
        [event async for event in session.start("fail")]
    assert session.finished
    manager.discard("factory-run")
    with pytest.raises(KeyError):
        manager.get("factory-run")


class SyncErrorWorker:
    def reply_stream(self, inputs):
        raise RuntimeError("stream setup failed")


@pytest.mark.asyncio
async def test_reply_stream_setup_failure_marks_run_finished_for_cleanup() -> None:
    manager = SubagentManager(SyncErrorWorker)
    session = manager.create("stream-run")
    with pytest.raises(RuntimeError, match="stream setup failed"):
        [event async for event in session.start("fail")]
    assert session.finished
    manager.discard("stream-run")


class ClosableWorker:
    def __init__(self) -> None:
        self.closed = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def close(self) -> None:
        self.closed = True

    async def reply_stream(self, inputs):
        self.started.set()
        yield Msg(name="worker", role="assistant", content=[TextBlock(text="started")])
        await self.release.wait()


@pytest.mark.asyncio
async def test_closing_active_stream_cleans_worker_and_allows_discard() -> None:
    worker = ClosableWorker()
    session = SubagentSession(lambda: worker)
    stream = session.start("close me")
    first = await stream.__anext__()
    assert isinstance(first, Msg)
    await stream.aclose()
    assert worker.closed
    assert session.finished


@pytest.mark.asyncio
async def test_cancelling_active_stream_cleans_worker() -> None:
    worker = ClosableWorker()
    session = SubagentSession(lambda: worker)

    async def consume() -> list[object]:
        return [event async for event in session.start("cancel me")]

    task = asyncio.create_task(consume())
    await worker.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert worker.closed
    assert session.finished


@pytest.mark.asyncio
async def test_reply_end_does_not_drop_trailing_events_or_state_persistence() -> None:
    """ReplyEnd is a protocol marker, not permission to truncate the stream.

    AgentScope's reply generator may persist state or emit a final message as
    it unwinds after yielding ReplyEndEvent.  The session must consume that
    tail before closing the worker.
    """

    class TrailingWorker:
        def __init__(self) -> None:
            self.persisted = False
            self.closed = False

        async def reply_stream(self, inputs):
            try:
                yield ReplyEndEvent(
                    session_id="worker-session",
                    reply_id="reply-1",
                    finished_reason=ReplyFinishedReason.COMPLETED,
                )
                # This models a final message/state checkpoint emitted while
                # the AgentScope reply generator unwinds after ReplyEnd.
                yield Msg(
                    name="worker",
                    role="assistant",
                    content=[TextBlock(text="tail")],
                )
            finally:
                self.persisted = True

        async def close(self) -> None:
            self.closed = True

    worker = TrailingWorker()
    session = SubagentSession(lambda: worker)
    events = [event async for event in session.start("finish")]

    assert isinstance(events[0], ReplyEndEvent)
    assert any(
        isinstance(event, Msg) and event.content[0].text == "tail"
        for event in events
    )
    assert worker.persisted
    assert worker.closed
    assert session.finished


@pytest.mark.asyncio
async def test_reply_and_session_ids_are_consistent_across_stream() -> None:
    class MismatchedWorker:
        async def reply_stream(self, inputs):
            yield ReplyStartEvent(
                session_id="session-1",
                reply_id="reply-1",
                name="worker",
            )
            yield ReplyEndEvent(
                session_id="session-2",
                reply_id="reply-1",
                finished_reason=ReplyFinishedReason.COMPLETED,
            )

    session = SubagentSession(MismatchedWorker)
    with pytest.raises(RuntimeError, match="session_id"):
        [event async for event in session.start("bad ids")]
    assert session.finished
