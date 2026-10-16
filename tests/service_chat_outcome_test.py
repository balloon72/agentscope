import asyncio
from unittest import IsolatedAsyncioTestCase

from agentscope.app._service import ChatService


class TestChatServiceRunOutcome(IsolatedAsyncioTestCase):
    async def test_overlapping_runs_keep_distinct_terminal_outcomes(self) -> None:
        service = object.__new__(ChatService)
        service._run_outcomes = {}
        started = asyncio.Event()
        both_started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def fake_run_impl(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
            if calls == 2:
                both_started.set()
            await release.wait()
            return True

        service._run_impl = fake_run_impl
        first = asyncio.create_task(service.run("u", "s", "a"))
        await started.wait()
        second = asyncio.create_task(service.run("u", "s", "a"))
        # The second invocation is still waiting in its body; no in-flight
        # ``not_started`` marker may be consumed as a terminal result.
        await both_started.wait()
        self.assertIsNone(service.consume_run_outcome("s"))
        release.set()
        self.assertTrue(await first)
        self.assertTrue(await second)
        self.assertEqual(service.consume_run_outcome("s"), "completed")
        self.assertEqual(service.consume_run_outcome("s"), "completed")
        self.assertIsNone(service.consume_run_outcome("s"))

    async def test_failed_preflight_is_retryable_and_consumed_once(self) -> None:
        service = object.__new__(ChatService)
        service._run_outcomes = {}

        async def fake_run_impl(*args, **kwargs):
            return False

        service._run_impl = fake_run_impl
        self.assertFalse(await service.run("u", "s", "a"))
        self.assertEqual(service.consume_run_outcome("s"), "retryable")
        self.assertIsNone(service.consume_run_outcome("s"))

