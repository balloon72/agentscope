import asyncio

import pytest

from agentscope.message import Msg, TextBlock
from agentscope.tool import OneShotSubagent


class FakeWorker:
    created = 0

    def __init__(self, reply_text: str = "done", delay: float = 0) -> None:
        type(self).created += 1
        self.reply_text = reply_text
        self.delay = delay
        self.calls = 0

    async def reply(self, message: Msg) -> Msg:
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return Msg(
            name="worker",
            role="assistant",
            content=[TextBlock(text=f"{self.reply_text}:{message.content[0].text}")],
        )


@pytest.mark.asyncio
async def test_one_shot_creates_a_fresh_worker_per_call() -> None:
    workers: list[FakeWorker] = []

    def factory() -> FakeWorker:
        worker = FakeWorker()
        workers.append(worker)
        return worker

    runner = OneShotSubagent(factory)
    first = await runner.run("first")
    second = await runner.run("second")

    assert first == {"status": "completed", "result": "done:first"}
    assert second == {"status": "completed", "result": "done:second"}
    assert len(workers) == 2
    assert workers[0] is not workers[1]
    assert [worker.calls for worker in workers] == [1, 1]


@pytest.mark.asyncio
async def test_one_shot_returns_timeout_without_leaking_worker() -> None:
    worker = FakeWorker(delay=0.05)
    runner = OneShotSubagent(lambda: worker, timeout=0.001)

    result = await runner.run("slow")

    assert result == {
        "status": "timeout",
        "error_code": "one_shot_timeout",
        "error": "subagent timed out",
        "retryable": True,
    }


@pytest.mark.asyncio
async def test_timeout_cancels_reply_and_closes_worker() -> None:
    class CancellableWorker:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled = False
            self.closed = False

        async def reply(self, message: Msg) -> Msg:
            self.started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled = True
            raise AssertionError("unreachable")

        async def close(self) -> None:
            self.closed = True

    worker = CancellableWorker()
    runner = OneShotSubagent(lambda: worker, timeout=60)
    task = asyncio.create_task(runner.run("cancel by caller"))
    await worker.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert worker.cancelled
    assert worker.closed


@pytest.mark.asyncio
async def test_factory_error_is_returned_and_worker_is_not_leaked() -> None:
    secret = "provider=acme /srv/private/api-key=secret-value"

    def factory() -> FakeWorker:
        raise RuntimeError(secret)

    result = await OneShotSubagent(factory).run("factory error")

    assert result == {
        "status": "error",
        "error_code": "one_shot_subagent_error",
        "error": "one-shot subagent execution failed",
        "retryable": True,
    }
    assert secret not in str(result)


@pytest.mark.asyncio
async def test_parallel_calls_use_isolated_workers() -> None:
    workers: list[FakeWorker] = []

    def factory() -> FakeWorker:
        worker = FakeWorker(reply_text=f"worker-{len(workers)}", delay=0.01)
        workers.append(worker)
        return worker

    runner = OneShotSubagent(factory)
    first, second = await asyncio.gather(runner.run("a"), runner.run("b"))

    assert first["status"] == second["status"] == "completed"
    assert first["result"] == "worker-0:a"
    assert second["result"] == "worker-1:b"
    assert len(workers) == 2
    assert all(worker.calls == 1 for worker in workers)


@pytest.mark.asyncio
async def test_empty_task_is_rejected_before_worker_creation() -> None:
    created = False

    def factory() -> FakeWorker:
        nonlocal created
        created = True
        return FakeWorker()

    result = await OneShotSubagent(factory).run("  ")

    assert result["status"] == "error"
    assert result["error"] == "task must not be empty"
    assert created is False


@pytest.mark.asyncio
async def test_non_string_task_is_rejected_before_worker_creation() -> None:
    created = False

    def factory() -> FakeWorker:
        nonlocal created
        created = True
        return FakeWorker()

    result = await OneShotSubagent(factory).run(None)  # type: ignore[arg-type]

    assert result == {"status": "error", "error": "task must not be empty"}
    assert created is False


@pytest.mark.asyncio
async def test_factory_timeout_error_is_reported_as_worker_error() -> None:
    secret = "/tmp/provider-response-secret"

    def factory() -> FakeWorker:
        raise TimeoutError(secret)

    result = await OneShotSubagent(factory).run("factory error")

    assert result == {
        "status": "error",
        "error_code": "one_shot_subagent_error",
        "error": "one-shot subagent execution failed",
        "retryable": True,
    }
    assert secret not in str(result)


def test_timeout_must_be_positive() -> None:
    with pytest.raises(ValueError, match="timeout must be positive"):
        OneShotSubagent(FakeWorker, timeout=0)
