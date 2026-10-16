# -*- coding: utf-8 -*-
"""Focused durability/idempotency tests for wakeup trigger recovery."""
import asyncio
from unittest import IsolatedAsyncioTestCase

from agentscope.app._bus_ops import enqueue_run_trigger
from agentscope.app._manager import ChatRunRegistry, WakeupDispatcher
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.event import UserConfirmResultEvent


class _Storage:
    async def get_session(self, *_args: object, **_kwargs: object) -> object:
        return object()


class _Chat:
    def __init__(self) -> None:
        self.calls: list[object] = []
        self.called = asyncio.Event()

    async def run(self, **kwargs: object) -> None:
        self.calls.append(kwargs)
        self.called.set()


class TestWakeupRecovery(IsolatedAsyncioTestCase):
    async def test_duplicate_resume_is_deduped_in_ledger(self) -> None:
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-1",
            confirm_results=[],
        )
        self.assertTrue(
            await enqueue_run_trigger(
                bus,
                "u",
                "s",
                "a",
                kind=MessageBusKeys.WAKEUP_KIND_RESUME,
                inputs=event,
            ),
        )
        self.assertFalse(
            await enqueue_run_trigger(
                bus,
                "u",
                "s",
                "a",
                kind=MessageBusKeys.WAKEUP_KIND_RESUME,
                inputs=event,
            ),
        )
        queued = await bus.queue_drain(MessageBusKeys.wakeup_queue())
        self.assertEqual(len(queued), 1)
        ledger = await bus.registry_getall(MessageBusKeys.wakeup_ledger())
        self.assertEqual(len(ledger), 1)

    async def test_startup_recovers_entry_drained_before_dispatch(self) -> None:
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-crashed",
            confirm_results=[],
        )
        await enqueue_run_trigger(
            bus,
            "u",
            "s",
            "a",
            kind=MessageBusKeys.WAKEUP_KIND_RESUME,
            inputs=event,
        )
        # Simulate a worker dying immediately after XRANGE/XDEL.
        await bus.queue_drain(MessageBusKeys.wakeup_queue())

        chat = _Chat()
        async with WakeupDispatcher(
            bus,
            _Storage(),
            chat,
            ChatRunRegistry(),
        ):
            await asyncio.wait_for(chat.called.wait(), timeout=2)

        self.assertEqual(len(chat.calls), 1)
        records = await bus.registry_getall(MessageBusKeys.wakeup_ledger())
        self.assertIn('"state":"completed"', next(iter(records.values())))
