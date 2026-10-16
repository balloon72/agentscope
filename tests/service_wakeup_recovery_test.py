# -*- coding: utf-8 -*-
"""Focused durability/idempotency tests for wakeup trigger recovery."""
import asyncio
import json
import time
from unittest import IsolatedAsyncioTestCase

from agentscope.app._bus_ops import enqueue_run_trigger
from agentscope.app._manager import ChatRunRegistry, WakeupDispatcher
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.event import UserConfirmResultEvent
from agentscope.app._manager import _wakeup_dispatcher as wakeup_dispatcher_module


class _FlakySubscribeBus(InMemoryMessageBus):
    """Fail the first SUBSCRIBE attempt, then expose a live stream."""

    def __init__(self) -> None:
        super().__init__()
        self.subscribe_calls = 0

    async def subscribe(self, key: str, *, on_ready=None):
        self.subscribe_calls += 1
        if self.subscribe_calls == 1:
            raise ConnectionError("initial redis connection failed")
        async for item in super().subscribe(key, on_ready=on_ready):
            yield item


class _FlakyRecoveryBus(InMemoryMessageBus):
    """Raise once during a recovery tick, then recover normally."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_registry_snapshot = False

    async def registry_getall(self, namespace: str):
        if self.fail_registry_snapshot:
            self.fail_registry_snapshot = False
            raise ConnectionError("transient redis outage")
        return await super().registry_getall(namespace)


class _Storage:
    async def get_session(self, *_args: object, **_kwargs: object) -> object:
        return object()


class _Chat:
    def __init__(self) -> None:
        self.calls: list[object] = []
        self.called = asyncio.Event()

    async def run(self, **kwargs: object) -> bool:
        self.calls.append(kwargs)
        self.called.set()
        return True


class _FailingChat(_Chat):
    async def run(self, **kwargs: object) -> bool:
        self.calls.append(kwargs)
        self.called.set()
        raise RuntimeError("transient run failure")


class _FalseChat(_Chat):
    async def run(self, **kwargs: object) -> bool:
        self.calls.append(kwargs)
        self.called.set()
        return False


class _VanishingLedgerBus(InMemoryMessageBus):
    """Snapshot sees a record, but the locked read observes expiry."""

    async def registry_get(
        self,
        namespace: str,
        field: str,
    ) -> str | None:
        if namespace == MessageBusKeys.wakeup_ledger():
            return None
        return await super().registry_get(namespace, field)


class _CountingLedgerBus(InMemoryMessageBus):
    def __init__(self) -> None:
        super().__init__()
        self.registry_getall_calls = 0

    async def registry_getall(self, namespace: str) -> dict[str, str]:
        self.registry_getall_calls += 1
        return await super().registry_getall(namespace)


class TestWakeupRecovery(IsolatedAsyncioTestCase):
    async def test_first_subscribe_failure_reconnects_and_drains_durable_queue(self) -> None:
        """A connection error before SUBSCRIBE readiness must not lose work."""
        bus = _FlakySubscribeBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-reconnect",
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
        chat = _Chat()
        async with WakeupDispatcher(bus, _Storage(), chat, ChatRunRegistry()):
            await asyncio.wait_for(chat.called.wait(), timeout=3)
        self.assertGreaterEqual(bus.subscribe_calls, 2)
        self.assertEqual(len(chat.calls), 1)

    async def test_periodic_recovery_continues_after_transient_bus_error(self) -> None:
        """One failed recovery pass must not terminate the recovery task."""
        bus = _FlakyRecoveryBus()
        chat = _Chat()
        old_interval = wakeup_dispatcher_module._CLAIM_RECOVERY_INTERVAL_SECS
        wakeup_dispatcher_module._CLAIM_RECOVERY_INTERVAL_SECS = 0.01
        try:
            dispatcher = WakeupDispatcher(bus, _Storage(), chat, ChatRunRegistry())
            async with dispatcher:
                event = UserConfirmResultEvent.model_construct(
                    reply_id="reply-periodic",
                    confirm_results=[],
                )
                await enqueue_run_trigger(
                    bus,
                    "u",
                    "periodic",
                    "a",
                    kind=MessageBusKeys.WAKEUP_KIND_RESUME,
                    inputs=event,
                )
                field, raw = next(
                    iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()),
                )
                record = json.loads(raw)
                record["recovery_policy"] = "replay_safe"
                record["payload"]["recovery_policy"] = "replay_safe"
                record.update(
                    {
                        "state": "claimed",
                        "claim_token": "dead-worker",
                        "claim_expires_at": time.time() - 1,
                    },
                )
                await bus.registry_set(
                    MessageBusKeys.wakeup_ledger(),
                    field,
                    json.dumps(record, separators=(",", ":")),
                )
                await bus.queue_drain(MessageBusKeys.wakeup_queue())
                bus.fail_registry_snapshot = True
                await asyncio.wait_for(chat.called.wait(), timeout=2)
            self.assertEqual(len(chat.calls), 1)
        finally:
            wakeup_dispatcher_module._CLAIM_RECOVERY_INTERVAL_SECS = old_interval

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

    async def test_failed_run_is_left_claimed_for_recovery(self) -> None:
        """A run exception must not be recorded as successful completion."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-failure",
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
        chat = _FailingChat()
        registry = ChatRunRegistry()
        async with WakeupDispatcher(
            bus,
            _Storage(),
            chat,
            registry,
        ):
            await asyncio.wait_for(chat.called.wait(), timeout=2)
            # Let the spawned task propagate its exception and run its
            # completion callback before inspecting the durable record.
            task = registry.get("s")
            self.assertIsNotNone(task)
            with self.assertRaises(RuntimeError):
                await task

        records = await bus.registry_getall(MessageBusKeys.wakeup_ledger())
        self.assertEqual(len(records), 1)
        self.assertEqual(
            json.loads(next(iter(records.values())))["state"],
            "claimed",
        )

    async def test_explicit_failed_run_is_terminal(self) -> None:
        """Unknown/legacy False results are never automatically replayed."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-explicit-failure",
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
        chat = _FalseChat()
        registry = ChatRunRegistry()
        async with WakeupDispatcher(bus, _Storage(), chat, registry):
            await asyncio.wait_for(chat.called.wait(), timeout=2)
            task = registry.get("s")
            self.assertIsNotNone(task)
            await task
        records = await bus.registry_getall(MessageBusKeys.wakeup_ledger())
        self.assertEqual(
            json.loads(next(iter(records.values())))["state"],
            "failed_unknown",
        )

    async def test_retry_cannot_overwrite_terminal_record(self) -> None:
        """A stale busy-session retry must not requeue a completed trigger."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-terminal",
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
        # Consume the original queue copy; this test checks only that the
        # retry timer does not enqueue a second copy after completion.
        await bus.queue_drain(MessageBusKeys.wakeup_queue())
        field, raw = next(
            iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()),
        )
        record = json.loads(raw)
        record["state"] = "completed"
        await bus.registry_set(
            MessageBusKeys.wakeup_ledger(),
            field,
            json.dumps(record, separators=(",", ":")),
        )

        accepted = await enqueue_run_trigger(
            bus,
            "u",
            "s",
            "a",
            kind=MessageBusKeys.WAKEUP_KIND_RESUME,
            inputs=event,
            trigger_id=field,
            _replace_existing=True,
        )
        self.assertFalse(accepted)
        self.assertEqual(
            json.loads(
                await bus.registry_get(MessageBusKeys.wakeup_ledger(), field),
            )["state"],
            "completed",
        )
        self.assertEqual(
            await bus.queue_drain(MessageBusKeys.wakeup_queue()),
            [],
        )

    async def test_explicit_trigger_id_avoids_full_ledger_scan(self) -> None:
        """Resume/message lookup uses the ledger field directly."""
        bus = _CountingLedgerBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-direct-lookup",
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
        field, raw = next(
            iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()),
        )
        bus.registry_getall_calls = 0
        dispatcher = WakeupDispatcher(
            bus,
            _Storage(),
            _Chat(),
            ChatRunRegistry(),
        )
        found = await dispatcher._find_trigger(
            trigger_id=field,
            entry_id=None,
            payload=json.loads(raw)["payload"],
        )
        self.assertIsNotNone(found)
        self.assertEqual(bus.registry_getall_calls, 0)

    async def test_claim_does_not_resurrect_expired_ledger_record(self) -> None:
        """A disappearing ledger entry is skipped rather than recreated."""
        bus = _VanishingLedgerBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-vanished",
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
        field, raw = next(
            iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()),
        )
        dispatcher = WakeupDispatcher(
            bus,
            _Storage(),
            _Chat(),
            ChatRunRegistry(),
        )
        payload = json.loads(raw)["payload"]
        self.assertEqual(
            await dispatcher._claim_trigger(
                trigger_id=field,
                entry_id=None,
                payload=payload,
            ),
            "",
        )
        self.assertIsNone(
            await InMemoryMessageBus.registry_get(
                bus,
                MessageBusKeys.wakeup_ledger(),
                field,
            ),
        )

    async def test_expired_claim_can_be_reclaimed(self) -> None:
        """A duplicate queue copy can take over an expired claim."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-expired",
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
        field, raw = next(
            iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()),
        )
        record = json.loads(raw)
        record["recovery_policy"] = "replay_safe"
        record["payload"]["recovery_policy"] = "replay_safe"
        record["state"] = "claimed"
        record["claim_expires_at"] = time.time() - 1
        # Explicitly opt into replay for this synthetic idempotent trigger.
        record["recovery_policy"] = "replay_safe"
        await bus.registry_set(
            MessageBusKeys.wakeup_ledger(),
            field,
            json.dumps(record, separators=(",", ":")),
        )
        dispatcher = WakeupDispatcher(
            bus,
            _Storage(),
            _Chat(),
            ChatRunRegistry(),
        )
        payload = record["payload"]
        self.assertEqual(
            await dispatcher._claim_trigger(
                trigger_id=field,
                entry_id=None,
                payload=payload,
            ),
            field,
        )
        claimed = json.loads(
            await bus.registry_get(MessageBusKeys.wakeup_ledger(), field),
        )
        self.assertGreater(claimed["claim_expires_at"], time.time())

    async def test_finish_requires_current_claim_token(self) -> None:
        """A stale worker cannot complete a claim recovered by another one."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-token",
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
        field, raw = next(
            iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()),
        )
        dispatcher = WakeupDispatcher(
            bus,
            _Storage(),
            _Chat(),
            ChatRunRegistry(),
        )
        payload = json.loads(raw)["payload"]
        self.assertEqual(
            await dispatcher._claim_trigger(
                trigger_id=field,
                entry_id=None,
                payload=payload,
            ),
            field,
        )
        token = dispatcher._claim_tokens[field]
        await dispatcher._finish_trigger(
            field,
            state="completed",
            claim_token="stale-token",
        )
        current = json.loads(
            await bus.registry_get(MessageBusKeys.wakeup_ledger(), field),
        )
        self.assertEqual(current["state"], "claimed")
        await dispatcher._finish_trigger(
            field,
            state="completed",
            claim_token=token,
        )
        current = json.loads(
            await bus.registry_get(MessageBusKeys.wakeup_ledger(), field),
        )
        self.assertEqual(current["state"], "completed")

    async def test_recovery_skips_fresh_claim_until_expiry(self) -> None:
        """Startup recovery must not duplicate a live worker's claim."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-live-claim",
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
        field, raw = next(
            iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()),
        )
        record = json.loads(raw)
        record["recovery_policy"] = "replay_safe"
        record["payload"]["recovery_policy"] = "replay_safe"
        record.update(
            {
                "state": "claimed",
                "claim_token": "live-token",
                "claim_expires_at": time.time() + 60,
            },
        )
        await bus.registry_set(
            MessageBusKeys.wakeup_ledger(),
            field,
            json.dumps(record, separators=(",", ":")),
        )
        await bus.queue_drain(MessageBusKeys.wakeup_queue())
        dispatcher = WakeupDispatcher(
            bus,
            _Storage(),
            _Chat(),
            ChatRunRegistry(),
        )
        await dispatcher._recover_trigger_ledger()
        self.assertEqual(
            await bus.queue_drain(MessageBusKeys.wakeup_queue()),
            [],
        )

        record["claim_expires_at"] = time.time() - 1
        await bus.registry_set(
            MessageBusKeys.wakeup_ledger(),
            field,
            json.dumps(record, separators=(",", ":")),
        )
        await dispatcher._recover_trigger_ledger()
        self.assertEqual(
            len(await bus.queue_drain(MessageBusKeys.wakeup_queue())),
            1,
        )

    async def test_expired_claim_with_unknown_outcome_requires_manual_review(self) -> None:
        """Cross-process recovery must not replay a potentially side-effecting run."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-unknown-outcome", confirm_results=[]
        )
        await enqueue_run_trigger(
            bus, "u", "s", "a",
            kind=MessageBusKeys.WAKEUP_KIND_RESUME,
            inputs=event,
        )
        field, raw = next(iter((await bus.registry_getall(MessageBusKeys.wakeup_ledger())).items()))
        record = json.loads(raw)
        record.update({
            "state": "claimed",
            "claim_token": "crashed-worker",
            "claim_expires_at": time.time() - 1,
        })
        await bus.registry_set(
            MessageBusKeys.wakeup_ledger(), field,
            json.dumps(record, separators=(",", ":")),
        )
        await bus.queue_drain(MessageBusKeys.wakeup_queue())
        dispatcher = WakeupDispatcher(bus, _Storage(), _Chat(), ChatRunRegistry())
        await dispatcher._recover_trigger_ledger()
        current = json.loads(await bus.registry_get(MessageBusKeys.wakeup_ledger(), field))
        self.assertEqual(current["state"], "manual_review")
        self.assertEqual(await bus.queue_drain(MessageBusKeys.wakeup_queue()), [])

class _BlockingChat(_Chat):
    """Run that would keep writing until the dispatcher fences it."""

    def __init__(self) -> None:
        super().__init__()
        self.cancelled = asyncio.Event()

    async def run(self, **kwargs: object) -> bool:
        self.calls.append(kwargs)
        self.called.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class TestWakeupClaimFencing(IsolatedAsyncioTestCase):
    async def test_lost_claim_cancels_old_chat_run(self) -> None:
        """A reclaimed token must stop the old run before side effects continue."""
        bus = InMemoryMessageBus()
        event = UserConfirmResultEvent.model_construct(
            reply_id="reply-claim-lost",
            confirm_results=[],
        )
        await enqueue_run_trigger(
            bus,
            "u",
            "claim-lost",
            "a",
            kind=MessageBusKeys.WAKEUP_KIND_RESUME,
            inputs=event,
        )
        chat = _BlockingChat()
        registry = ChatRunRegistry()
        dispatcher = WakeupDispatcher(bus, _Storage(), chat, registry)

        async def _lost_claim(_field: str | None, _token: str | None) -> bool:
            return False

        dispatcher._renew_trigger_claim = _lost_claim  # type: ignore[method-assign]
        old_ttl = MessageBusKeys.WAKEUP_CLAIM_TTL_SECS
        MessageBusKeys.WAKEUP_CLAIM_TTL_SECS = 3
        try:
            async with dispatcher:
                await asyncio.wait_for(chat.called.wait(), timeout=2)
                await asyncio.wait_for(chat.cancelled.wait(), timeout=3)
                task = registry.get("claim-lost")
                self.assertIsNotNone(task)
                with self.assertRaises(asyncio.CancelledError):
                    await task
        finally:
            MessageBusKeys.WAKEUP_CLAIM_TTL_SECS = old_ttl

        records = await bus.registry_getall(MessageBusKeys.wakeup_ledger())
        self.assertEqual(
            json.loads(next(iter(records.values())))['state'],
            'claimed',
        )
