# -*- coding: utf-8 -*-
"""Single per-process dispatcher for all cross-session run triggers.

One asyncio task per process. Subscribes to the shared trigger signal
channel and drains the durable trigger queue on each signal. It is the
**sole** site that spawns :meth:`ChatService.run` into the shared
:class:`ChatRunRegistry`, which is what makes concurrent-spawn races
(two writers contending for one session's run slot → a spurious "already
has an active chat run" 409) structurally impossible: every run trigger
funnels through this one serial consumer.

Each queue entry carries a ``kind`` that selects how a busy session is
handled:

- ``wake`` (idle-session wake-up, ``input_msg=None``): produced only
  when no run is registered as the session's inbox consumer, so it
  always has something to deliver.
- ``resume`` (a parked HITL run being fed its result): carries the
  event the parked run is waiting for.

No kind is ever dropped. A trigger whose session still holds its run
lock is re-queued after a short backoff until the lock frees, then
spawned.

All bus keys live on the :class:`MessageBus` base class (see
``enqueue_wakeup`` / ``enqueue_input``, ``dequeue_wakeups``,
``subscribe_wakeup_signal``, ``session_is_running``), so this file has
no hard-coded key strings.
"""
import asyncio
import json
import os
import time
import uuid
from typing import TYPE_CHECKING, Self

from pydantic import TypeAdapter

from ..._logging import logger
from ...event import (
    ExternalExecutionResultEvent,
    ReplyEndEvent,
    ReplyStartEvent,
    UserConfirmResultEvent,
    UserInterruptEvent,
)
from ...message import Msg
from ...types import ErrorInfo, ErrorType, ReplyFinishedReason
from ..message_bus import MessageBusKeys
from .._bus_ops import enqueue_run_trigger, publish_session_event

if TYPE_CHECKING:
    from ..message_bus import MessageBus
    from ..storage import StorageBase
    from .._service import ChatService
    from ._chat_run_registry import ChatRunRegistry

# Parses a queued ``resume`` input dict back into its concrete event,
# discriminated by the ``type`` field shared by these result events.
_RESUME_INPUT_ADAPTER: TypeAdapter = TypeAdapter(
    UserConfirmResultEvent | ExternalExecutionResultEvent | UserInterruptEvent,
)

# Delay before re-queuing a trigger whose target session still holds
# its run lock. Short enough to feel instant to the user, long enough
# to avoid a hot re-enqueue loop while the lock is held.
_RESUME_RETRY_BACKOFF_SECS = 0.1
# Transient preflight failures are retried a bounded number of times.  Keep
# this small: a bad workspace/provider configuration must not grow an error
# transcript forever or spin a durable trigger indefinitely.
_MAX_PREFLIGHT_RETRIES = 3
_PREFLIGHT_RETRY_BASE_SECS = 0.25
_PREFLIGHT_RETRY_EXHAUSTED_MESSAGE = (
    "Temporary setup failure exhausted the retry budget; manual review is "
    "required."
)
# Bound startup on a bus that cannot establish a subscription.
_SUBSCRIBE_READY_TIMEOUT_SECS = 10.0
# Avoid a tight reconnect loop while Redis (or another bus) is unavailable.
_SUBSCRIBE_RETRY_BACKOFF_SECS = 0.5
# Keep cleanup work bounded when a long-lived process accumulates terminal
# dedupe records. The ledger hash itself remains durable for the configured
# retention period; this only removes records older than that period.
_TERMINAL_CLEANUP_BATCH = 64
# Bound the amount of trigger work handed to downstream session checks at once.
_DRAIN_BATCH_SIZE = 64
_DISPATCH_CONCURRENCY = 16
_CLAIM_RECOVERY_INTERVAL_SECS = min(
    30.0, max(1.0, MessageBusKeys.WAKEUP_CLAIM_TTL_SECS / 2)
)
# Periodic claim recovery is useful for strict crash-recovery deployments, but
# it adds a background scan to every API process. Keep the moderate-SLA
# profile lightweight by enabling it explicitly; startup recovery and normal
# queue draining remain enabled in all profiles.
_ENABLE_PERIODIC_RECOVERY = os.getenv(
    "AGENTSCOPE_WAKEUP_PERIODIC_RECOVERY",
    "0",
).strip().lower() in {"1", "true", "yes", "on"}


class WakeupDispatcher:
    """One asyncio task per process, draining the shared trigger queue.

    Args:
        message_bus (`MessageBus`):
            Application message bus. Used for signal subscription,
            queue drain, ``session_is_running`` checks, and re-queuing
            deferred ``resume`` triggers.
        storage (`StorageBase`):
            Persistent storage backend. Consulted before spawning a
            run so triggers whose target session has been deleted are
            dropped instead of crashing :class:`ChatService.run`.
        chat_service (`ChatService`):
            Drives the actual chat run when a trigger fires.
        chat_run_registry (`ChatRunRegistry`):
            Per-process registry that holds the spawned task handle so
            it can be located by :class:`CancelDispatcher`.
    """

    def __init__(
        self,
        message_bus: "MessageBus",
        storage: "StorageBase",
        chat_service: "ChatService",
        chat_run_registry: "ChatRunRegistry",
        enable_periodic_recovery: bool | None = None,
    ) -> None:
        """Bind dependencies.

        Args:
            message_bus (`MessageBus`):
                Application message bus.
            storage (`StorageBase`):
                Persistent storage backend.
            chat_service (`ChatService`):
                Drives session runs via :meth:`ChatService.run`.
            chat_run_registry (`ChatRunRegistry`):
                Shared chat-run registry to spawn into.
            enable_periodic_recovery (`bool | None`):
                Whether to scan and recover expired claims periodically while
                the process is running. ``None`` uses the
                ``AGENTSCOPE_WAKEUP_PERIODIC_RECOVERY`` environment setting.
                Startup recovery is always performed.
        """
        self._bus = message_bus
        self._storage = storage
        self._chat_service = chat_service
        self._registry = chat_run_registry
        self._enable_periodic_recovery = (
            _ENABLE_PERIODIC_RECOVERY
            if enable_periodic_recovery is None
            else enable_periodic_recovery
        )
        self._task: asyncio.Task | None = None
        self._recovery_task: asyncio.Task | None = None
        # Serialize drain passes and cap dispatches across all signal/recovery
        # callers in this process, rather than per invocation.
        self._drain_lock = asyncio.Lock()
        self._dispatch_semaphore = asyncio.Semaphore(_DISPATCH_CONCURRENCY)
        # Detached backoff timers for deferred ``resume`` re-enqueues.
        # Held so they are not garbage-collected mid-sleep and can be
        # cancelled on shutdown.
        self._retry_tasks: set[asyncio.Task] = set()
        # Claim tokens are copied into the spawned coroutine's closure so a
        # stale worker cannot finish a claim that another dispatcher has
        # already recovered and re-claimed.
        self._claim_tokens: dict[str, str] = {}
        self._retry_attempts: dict[str, int] = {}

    async def __aenter__(self) -> Self:
        """Start the dispatcher loop and wait until its bus
        subscription is live.

        Also performs an initial drain right after subscription so
        triggers produced while this process was down (durable in
        the queue) are picked up immediately on startup.

        Returns:
            `Self`: This dispatcher instance.
        """
        ready = asyncio.Event()
        self._task = asyncio.create_task(
            self._loop(ready),
            name="wakeup-dispatcher",
        )
        try:
            await asyncio.wait_for(
                ready.wait(),
                timeout=_SUBSCRIBE_READY_TIMEOUT_SECS,
            )
        except asyncio.TimeoutError as exc:
            # Do not leave a reconnecting task behind when startup cannot
            # establish a subscription within a bounded time.
            if self._task is not None:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                self._task = None
            raise RuntimeError(
                "WakeupDispatcher subscription did not become ready"
            ) from exc
        # Startup is the one point where a prior process may have drained a
        # queue entry and died before claiming it. Force a replay of queued
        # records here; steady-state recovery avoids adding another copy when
        # the ledger still records a live queue entry.
        await self._recover_trigger_ledger()
        await self._drain_and_dispatch()
        if self._enable_periodic_recovery:
            self._recovery_task = asyncio.create_task(
                self._periodic_recovery(), name="wakeup-recovery"
            )
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Cancel the dispatcher loop and any pending retries."""
        retries = list(self._retry_tasks)
        for retry in retries:
            retry.cancel()
        for retry in retries:
            try:
                await retry
            except asyncio.CancelledError:
                pass
        self._retry_tasks.clear()
        if self._recovery_task is not None:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
            self._recovery_task = None
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _periodic_recovery(self) -> None:
        """Recover durable triggers without replaying unknown side effects."""
        try:
            while True:
                await asyncio.sleep(_CLAIM_RECOVERY_INTERVAL_SECS)
                try:
                    await self._recover_trigger_ledger(periodic=True)
                    await self._drain_and_dispatch()
                except Exception:  # pragma: no cover - transient bus error
                    logger.exception("WakeupDispatcher periodic recovery failed.")
        except asyncio.CancelledError:
            raise

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _loop(self, ready: asyncio.Event) -> None:
        """Long-lived loop: subscribe to the signal channel and drain
        the queue on every received signal.

        Args:
            ready (`asyncio.Event`):
                Signalled after the underlying SUBSCRIBE completes.
                :meth:`start` blocks on this so callers can publish a
                trigger immediately after start without racing.
        """
        while True:
            subscription_ready = asyncio.Event()

            def _on_ready() -> None:
                subscription_ready.set()
                ready.set()

            try:
                async for _signal in self._bus.subscribe(
                    MessageBusKeys.wakeup_signal(),
                    on_ready=_on_ready,
                ):
                    await self._drain_and_dispatch()
                # A cleanly ending subscription is still a disconnect. Drain
                # the durable queue before reconnecting in case a signal was
                # published while the subscriber was down.
                if subscription_ready.is_set():
                    await self._drain_and_dispatch()
                raise RuntimeError("wakeup subscription ended")
            except asyncio.CancelledError:
                raise
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "WakeupDispatcher subscription failed; reconnecting.",
                )
                if subscription_ready.is_set():
                    try:
                        await self._drain_and_dispatch()
                    except Exception:  # pylint: disable=broad-except
                        logger.exception(
                            "WakeupDispatcher recovery drain failed.",
                        )
                await asyncio.sleep(_SUBSCRIBE_RETRY_BACKOFF_SECS)

    async def _ledger_snapshot(self) -> dict[str, dict]:
        """Read the trigger ledger, or return an empty map for old buses."""
        if getattr(self, "_ledger_supported", None) is False:
            return {}
        try:
            raw = await self._bus.registry_getall(
                MessageBusKeys.wakeup_ledger(),
            )
        except (AttributeError, NotImplementedError, TypeError):
            # Compatibility fakes / pre-ledger bus implementations. Other
            # exceptions (for example a Redis outage) propagate so the
            # dispatcher cannot silently lose durable trigger recovery.
            self._ledger_supported = False
            return {}
        if not isinstance(raw, dict):
            self._ledger_supported = False
            return {}
        self._ledger_supported = True
        records: dict[str, dict] = {}
        for field, value in raw.items():
            if isinstance(field, bytes):
                field = field.decode("utf-8", errors="replace")
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            try:
                record = json.loads(value) if isinstance(value, str) else value
            except (TypeError, ValueError):
                continue
            if isinstance(record, dict):
                records[str(field)] = record
        return records

    @staticmethod
    def _has_ledger_metadata(payload: dict) -> bool:
        """Whether a trigger payload was produced by a ledger-aware sender."""
        return isinstance(payload, dict) and (
            "trigger_id" in payload or "recovery_policy" in payload
        )

    @staticmethod
    def _same_payload(left: dict, right: dict) -> bool:
        """Compare JSON payloads without relying on dict insertion order."""
        try:
            return json.dumps(
                left,
                sort_keys=True,
                separators=(",", ":"),
            ) == json.dumps(
                right,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _claim_expired(record: dict) -> bool:
        """Return whether an in-flight trigger claim has timed out."""
        expires_at = record.get("claim_expires_at")
        if not isinstance(expires_at, (int, float)):
            # Records written before claim leases were introduced have no
            # expiry metadata. Treat them as stale so an upgrade cannot leave
            # an old claimed trigger suppressed forever.
            return True
        return expires_at <= time.time()

    @staticmethod
    def _recovery_safe(record: dict) -> bool:
        """Whether an expired claim is explicitly safe to replay.

        The default is fail-closed: a crash can leave an external write
        completed even though the worker never persisted its result. Only a
        producer that has deliberately classified the trigger as read-only or
        idempotent may opt into automatic cross-process recovery.
        """
        policy = record.get("recovery_policy")
        if policy is None:
            # Ledger records written before recovery_policy was promoted to a
            # top-level field kept it in the queued payload. Continue to
            # recover those records with the policy their producer supplied.
            payload = record.get("payload")
            if isinstance(payload, dict):
                policy = payload.get("recovery_policy")
        return policy == "replay_safe"

    async def _find_trigger(
        self,
        *,
        trigger_id: str | None,
        entry_id: str | None,
        payload: dict,
    ) -> tuple[str, dict] | None:
        # Resume/message entries carry the ledger field explicitly. Resolve
        # those in O(1); scanning the whole hash for every trigger turns a
        # busy inbox into an O(number-of-ledger-records) hot path.
        if trigger_id is not None and getattr(
            self,
            "_ledger_supported",
            None,
        ) is not False:
            try:
                raw = await self._bus.registry_get(
                    MessageBusKeys.wakeup_ledger(),
                    trigger_id,
                )
            except (AttributeError, NotImplementedError, TypeError):
                self._ledger_supported = False
            else:
                self._ledger_supported = True
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                try:
                    record = (
                        json.loads(raw) if isinstance(raw, str) else raw
                    )
                except (TypeError, ValueError):
                    record = None
                if isinstance(record, dict):
                    return trigger_id, record

        records = await self._ledger_snapshot()
        if trigger_id is not None and trigger_id in records:
            return trigger_id, records[trigger_id]
        if entry_id is not None:
            for field, record in records.items():
                if record.get("queue_id") == entry_id:
                    return field, record
        # Wake payloads intentionally remain wire-compatible with older
        # producers and therefore do not carry trigger_id. A recovery push
        # receives a new queue entry id, so match the oldest queued record by
        # payload as the final fallback.
        for field, record in records.items():
            if (
                record.get("state") == "queued"
                and isinstance(record.get("payload"), dict)
                and self._same_payload(record["payload"], payload)
            ):
                return field, record
        return None

    async def _set_record(self, field: str, record: dict) -> None:
        if getattr(self, "_ledger_supported", None) is not True:
            return
        await self._bus.registry_set(
            MessageBusKeys.wakeup_ledger(),
            field,
            json.dumps(record, ensure_ascii=False, separators=(",", ":")),
            ttl_secs=MessageBusKeys.WAKEUP_LEDGER_TTL_SECS,
        )

    async def _claim_trigger(
        self,
        *,
        trigger_id: str | None,
        entry_id: str | None,
        payload: dict,
    ) -> str | None:
        """Claim one ledger record; duplicate queue entries are skipped."""
        found = await self._find_trigger(
            trigger_id=trigger_id,
            entry_id=entry_id,
            payload=payload,
        )
        if found is None:
            if trigger_id is not None:
                # A trigger_id is emitted only by ledger-backed producers.
                # If its record has expired, do not treat the payload as an
                # old ledger-less trigger and run it outside dedupe/fencing.
                return ""
            # A ledger-aware payload with no matching record is a stale
            # modern copy. Do not fall through to the legacy path, which
            # would bypass dedupe and can execute the wake twice.
            if (
                getattr(self, "_ledger_supported", None) is True
                and self._has_ledger_metadata(payload)
            ):
                return ""
            # No ledger entry means this is an old producer payload (or a
            # compatibility test bus); dispatch it through the legacy path.
            return None
        field, record = found
        async with self._bus.acquire_lock(
            MessageBusKeys.wakeup_ledger_lock(),
            ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
        ):
            # Re-read under the lock so two dispatchers cannot both spawn.
            current = await self._bus.registry_get(
                MessageBusKeys.wakeup_ledger(),
                field,
            )
            # The record may have expired or been removed between the
            # snapshot used by ``_find_trigger`` and this locked read.  Do
            # not resurrect that stale snapshot: claiming it would create a
            # ledger entry for a queue item whose dedupe/retention record is
            # already gone.  Likewise, malformed current data is unsafe to
            # overwrite with the stale value.
            if not current:
                await self._bus.registry_del(
                    MessageBusKeys.wakeup_ledger(),
                    field,
                )
                return ""
            try:
                current_record = json.loads(current)
            except (TypeError, ValueError):
                await self._bus.registry_del(
                    MessageBusKeys.wakeup_ledger(),
                    field,
                )
                return ""
            if not isinstance(current_record, dict):
                await self._bus.registry_del(
                    MessageBusKeys.wakeup_ledger(),
                    field,
                )
                return ""
            state = current_record.get("state")
            if state in {"completed", "failed", "failed_unknown", "manual_review", "dropped"}:
                return ""
            if state == "claimed":
                if not self._claim_expired(current_record):
                    return ""
                if not self._recovery_safe(current_record):
                    current_record["state"] = "manual_review"
                    current_record["review_reason"] = "claim_expired_unknown_outcome"
                    current_record["finished_at"] = time.time()
                    await self._set_record(field, current_record)
                    logger.error(
                        "Wakeup trigger %s requires manual review after claim "
                        "expiry (session=%s)",
                        field,
                        current_record.get("payload", {}).get("session_id"),
                    )
                    return ""
            claim_token = uuid.uuid4().hex
            current_record["state"] = "claimed"
            current_record[
                "claim_expires_at"
            ] = time.time() + MessageBusKeys.WAKEUP_CLAIM_TTL_SECS
            current_record["claim_token"] = claim_token
            await self._set_record(field, current_record)
            self._claim_tokens[field] = claim_token
        return field

    async def _finish_trigger(
        self,
        field: str | None,
        *,
        state: str,
        claim_token: str | None = None,
    ) -> None:
        if not field:
            return
        async with self._bus.acquire_lock(
            MessageBusKeys.wakeup_ledger_lock(),
            ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
        ):
            record = await self._bus.registry_get(
                MessageBusKeys.wakeup_ledger(),
                field,
            )
            if not record:
                return
            try:
                parsed = json.loads(record)
            except (TypeError, ValueError):
                return
            if not isinstance(parsed, dict):
                return
            if claim_token is not None:
                # A recovered worker may have replaced this claim while the
                # old task was still unwinding. Only the current owner may
                # transition a claimed record to a terminal state.
                if (
                    parsed.get("state") != "claimed"
                    or parsed.get("claim_token") != claim_token
                ):
                    return
            elif parsed.get("state") != "queued":
                # Pre-claim drops (malformed input/orphaned session) may only
                # transition the still-queued record. An expired claim must
                # first be re-claimed so its token can fence this write.
                return
            parsed["state"] = state
            parsed["finished_at"] = time.time()
            await self._set_record(field, parsed)
            self._claim_tokens.pop(field, None)

    async def _renew_trigger_claim(
        self, field: str | None, claim_token: str | None
    ) -> bool:
        """Extend a claim only while its fencing token is still current."""
        if not field or claim_token is None:
            return False
        async with self._bus.acquire_lock(
            MessageBusKeys.wakeup_ledger_lock(),
            ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
        ):
            raw = await self._bus.registry_get(
                MessageBusKeys.wakeup_ledger(), field
            )
            if not raw:
                return False
            try:
                record = json.loads(raw)
            except (TypeError, ValueError):
                return False
            if not isinstance(record, dict) or (
                record.get("state") != "claimed"
                or record.get("claim_token") != claim_token
            ):
                return False
            record["claim_expires_at"] = (
                time.time() + MessageBusKeys.WAKEUP_CLAIM_TTL_SECS
            )
            await self._set_record(field, record)
            return True

    async def _release_trigger_claim(
        self,
        field: str | None,
        claim_token: str | None,
    ) -> None:
        """Return a claim to ``queued`` after a local spawn race.

        This is the one non-terminal transition after claim. It is fenced by
        the token so a stale dispatcher cannot release a newer owner's claim.
        """
        if not field or claim_token is None:
            return
        async with self._bus.acquire_lock(
            MessageBusKeys.wakeup_ledger_lock(),
            ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
        ):
            raw = await self._bus.registry_get(
                MessageBusKeys.wakeup_ledger(),
                field,
            )
            if not raw:
                return
            try:
                record = json.loads(raw)
            except (TypeError, ValueError):
                return
            if not isinstance(record, dict):
                return
            if (
                record.get("state") != "claimed"
                or record.get("claim_token") != claim_token
            ):
                return
            record["state"] = "queued"
            record.pop("claim_token", None)
            record.pop("claim_expires_at", None)
            await self._set_record(field, record)
            self._claim_tokens.pop(field, None)

    async def _prepare_retry(self, field: str | None) -> int | None:
        """Increment and persist a trigger's retry counter atomically."""
        if not field:
            return 0
        if getattr(self, "_ledger_supported", None) is not True:
            count = self._retry_attempts.get(field, 0) + 1
            self._retry_attempts[field] = count
            return count
        async with self._bus.acquire_lock(
            MessageBusKeys.wakeup_ledger_lock(),
            ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
        ):
            raw = await self._bus.registry_get(MessageBusKeys.wakeup_ledger(), field)
            if not raw:
                return None
            try:
                record = json.loads(raw)
            except (TypeError, ValueError):
                return None
            if not isinstance(record, dict) or record.get("state") != "queued":
                return None
            count = int(record.get("retry_count", 0)) + 1
            record["retry_count"] = count
            record["last_retry_at"] = time.time()
            await self._set_record(field, record)
            return count

    async def _recover_trigger_ledger(self, *, periodic: bool = False) -> None:
        """Requeue queued records; quarantine expired unsafe claims.

        Queue drain is intentionally destructive, so a worker can disappear
        after XRANGE/XDEL but before spawning the chat run. Startup recovery
        re-pushes every queued record to cover that crash window. Periodic
        recovery only re-pushes queued records whose prior queue copy has no
        recorded id (or whose queue marker is old enough to indicate a lost
        drain), preventing an otherwise unbounded pile of duplicate queue
        entries while a healthy worker is merely idle. Claimed records are
        replayed only after their lease expires when the producer explicitly
        marked the work ``replay_safe``. All other expired claims become
        ``manual_review`` because their external outcome is unknowable.
        """
        records = await self._ledger_snapshot()
        if not records:
            return
        async with self._bus.acquire_lock(
            MessageBusKeys.wakeup_ledger_lock(),
            ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
        ):
            # Re-read while holding the lock to avoid racing an enqueue.
            records = await self._ledger_snapshot()
            now = time.time()
            cleaned = 0
            for field, record in records.items():
                state = record.get("state")
                if (
                    cleaned < _TERMINAL_CLEANUP_BATCH
                    and state in {"completed", "failed", "failed_unknown", "manual_review", "dropped"}
                    and isinstance(record.get("finished_at"), (int, float))
                    and now - record["finished_at"]
                    >= MessageBusKeys.WAKEUP_LEDGER_TTL_SECS
                ):
                    await self._bus.registry_del(
                        MessageBusKeys.wakeup_ledger(),
                        field,
                    )
                    cleaned += 1
                    continue
                if state == "claimed" and not self._claim_expired(record):
                    # Another live worker still owns this claim. Replaying it
                    # before its lease expires could run a side-effecting chat
                    # turn twice. A later recovery pass can take it over.
                    continue
                if state == "claimed" and not self._recovery_safe(record):
                    record["state"] = "manual_review"
                    record["review_reason"] = "claim_expired_unknown_outcome"
                    record["finished_at"] = now
                    await self._set_record(field, record)
                    logger.error(
                        "Wakeup trigger %s requires manual review after claim "
                        "expiry (session=%s)",
                        field,
                        record.get("payload", {}).get("session_id"),
                    )
                    continue
                if state not in {"queued", "claimed"}:
                    continue
                if (
                    state == "queued"
                    and periodic
                    and record.get("queue_id") is not None
                    and isinstance(record.get("queued_at"), (int, float))
                    and now - record["queued_at"]
                    < _CLAIM_RECOVERY_INTERVAL_SECS * 2
                ):
                    # The producer/recovery pass already left a copy on the
                    # durable queue. Re-pushing it every periodic tick would
                    # grow duplicate copies without improving crash safety;
                    # startup handles the drain-before-claim case.
                    continue
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    record["state"] = "dropped"
                    await self._set_record(field, record)
                    continue
                record["state"] = "queued"
                record.pop("claim_token", None)
                record.pop("claim_expires_at", None)
                try:
                    queue_id = await self._bus.queue_push(
                        MessageBusKeys.wakeup_queue(),
                        payload,
                    )
                except Exception:  # pragma: no cover - transient Redis error
                    logger.exception(
                        "WakeupDispatcher: failed recovering trigger %s",
                        field,
                    )
                    await self._set_record(field, record)
                    continue
                record["queue_id"] = str(queue_id)
                record["queued_at"] = now
                await self._set_record(field, record)
        await self._bus.publish(MessageBusKeys.wakeup_signal(), {})

    async def _drain_and_dispatch(self) -> None:
        """Serialize queue drains across signals and recovery ticks."""
        async with self._drain_lock:
            await self._drain_and_dispatch_locked()

    async def _drain_and_dispatch_locked(self) -> None:
        """Drain and dispatch all queued triggers in bounded batches.

        Queue drains are intentionally capped so a large backlog cannot be
        handed to the dispatcher (and downstream storage/session checks) in a
        single unbounded operation.  We continue draining until a batch is
        empty, while limiting concurrent dispatches within each batch.
        """
        async def dispatch_entry(entry_id: str, payload: dict) -> None:
            async with self._dispatch_semaphore:
                try:
                    user_id = payload["user_id"]
                    session_id = payload["session_id"]
                    agent_id = payload["agent_id"]
                except (KeyError, TypeError):
                    logger.warning(
                        "WakeupDispatcher: skipping malformed trigger entry %r",
                        payload,
                    )
                    # A malformed entry can still be ledger-backed (for
                    # example, if a producer wrote an incomplete payload).
                    # Mark that record terminally dropped; otherwise the
                    # periodic recovery pass will requeue it forever and
                    # repeatedly emit the same warning.
                    try:
                        found = await self._find_trigger(
                            trigger_id=(
                                payload.get("trigger_id")
                                if isinstance(payload, dict)
                                else None
                            ),
                            entry_id=str(entry_id),
                            payload=payload,
                        )
                        if found is not None:
                            await self._finish_trigger(
                                found[0], state="dropped"
                            )
                    except Exception:  # pragma: no cover - bus outage
                        logger.exception(
                            "WakeupDispatcher: failed to drop malformed "
                            "entry %s",
                            entry_id,
                        )
                    return
                # Entries from older producers omit ``kind`` — treat as wake.
                kind = payload.get("kind", MessageBusKeys.WAKEUP_KIND_WAKE)
                await self._dispatch_one(
                    user_id=user_id,
                    session_id=session_id,
                    agent_id=agent_id,
                    kind=kind,
                    raw_input=payload.get("input"),
                    entry_id=str(entry_id),
                    trigger_id=payload.get("trigger_id"),
                )

        while True:
            try:
                raw_entries = await self._bus.queue_drain(
                    MessageBusKeys.wakeup_queue(),
                    max_count=_DRAIN_BATCH_SIZE,
                )
            except Exception:  # pylint: disable=broad-except
                logger.exception("WakeupDispatcher: dequeue_wakeups failed.")
                return
            if not raw_entries:
                return

            results = await asyncio.gather(
                *(
                    dispatch_entry(entry_id, payload)
                    for entry_id, payload in raw_entries
                ),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.exception(
                        "WakeupDispatcher: dispatch entry failed.",
                        exc_info=(type(result), result, result.__traceback__),
                    )

    async def _report_retry_exhausted(
        self,
        user_id: str,
        session_id: str,
        agent_id: str,
    ) -> None:
        """Publish and persist a terminal preflight failure.

        ChatService owns the canonical failure-reply path, including
        persistence of the synthetic assistant message. Keep a small
        compatibility fallback for custom test/services that predate the
        public ``report_failure`` hook; those still receive a valid event
        pair, while the production ChatService path is durable.
        """
        error = RuntimeError(_PREFLIGHT_RETRY_EXHAUSTED_MESSAGE)
        reporter = getattr(self._chat_service, "report_failure", None)
        if reporter is not None:
            await reporter(user_id, session_id, agent_id, error)
            return

        # Legacy ChatService implementations may expose only the original
        # private helper. It persists the same pair, although callers are
        # responsible for session serialization in that compatibility path.
        reporter = getattr(self._chat_service, "_report_failure", None)
        if reporter is not None:
            await reporter(user_id, session_id, agent_id, error)
            return

        # Last-resort compatibility for minimal stand-ins used by callers;
        # never leave the frontend with a REPLY_END that has no matching id.
        reply_id = uuid.uuid4().hex
        await publish_session_event(
            self._bus,
            session_id,
            ReplyStartEvent(
                session_id=session_id,
                reply_id=reply_id,
                name=agent_id,
            ).model_dump(mode="json"),
        )
        await publish_session_event(
            self._bus,
            session_id,
            ReplyEndEvent(
                session_id=session_id,
                reply_id=reply_id,
                finished_reason=ReplyFinishedReason.ERROR,
                error=ErrorInfo(
                    type=ErrorType.UPSTREAM,
                    message=_PREFLIGHT_RETRY_EXHAUSTED_MESSAGE,
                ),
            ).model_dump(mode="json"),
        )

    async def _dispatch_one(
        self,
        user_id: str,
        session_id: str,
        agent_id: str,
        kind: str,
        raw_input: dict | None,
        entry_id: str | None = None,
        trigger_id: str | None = None,
    ) -> None:
        """Dispatch a single trigger entry by its ``kind``.

        Args:
            user_id (`str`):
                The owning user id.
            session_id (`str`):
                The session to trigger.
            agent_id (`str`):
                The agent that owns the session.
            kind (`str`):
                Trigger kind (``wake`` / ``resume``); see module docstring.
            raw_input (`dict | None`):
                Serialised input event for ``resume`` triggers, else
                ``None``.
        """
        is_resume = kind == MessageBusKeys.WAKEUP_KIND_RESUME
        is_message = kind == MessageBusKeys.WAKEUP_KIND_MESSAGE
        # ``resume`` and ``message`` both carry input that must be
        # delivered — never dropped while the session is busy.
        carries_input = is_resume or is_message

        # Resolve the durable trigger record before any early-return path.
        # Older producers have no ledger entry and continue through the
        # compatibility path unchanged.
        ledger_field: str | None = None
        ledger_record: dict | None = None
        found = await self._find_trigger(
            trigger_id=trigger_id,
            entry_id=entry_id,
            payload={
                "user_id": user_id,
                "session_id": session_id,
                "agent_id": agent_id,
                "kind": kind,
                "input": raw_input,
                **({"trigger_id": trigger_id} if trigger_id else {}),
            },
        )
        if found is not None:
            ledger_field, ledger_record = found
            state = ledger_record.get("state")
            if state in {"completed", "failed", "failed_unknown", "manual_review", "dropped"} or (
                state == "claimed"
                and not self._claim_expired(ledger_record)
            ):
                # A duplicate queue copy is expected during recovery. The
                # original owner (or a later recovery pass) owns this record.
                return
        recovery_policy = "manual_review"
        if ledger_record is not None:
            recovery_policy = ledger_record.get("recovery_policy")
            if recovery_policy is None:
                payload = ledger_record.get("payload")
                if isinstance(payload, dict):
                    recovery_policy = payload.get("recovery_policy")
            if not isinstance(recovery_policy, str):
                recovery_policy = "manual_review"

        # Parse the carried input early so every downstream path
        # (lock-retry, spawn-retry) receives a typed object rather than a
        # raw dict.
        input_msg: (
            UserConfirmResultEvent
            | ExternalExecutionResultEvent
            | UserInterruptEvent
            | Msg
            | None
        ) = None
        if carries_input:
            if raw_input is None:
                logger.warning(
                    "WakeupDispatcher: dropping %s trigger for session "
                    "%s — no input carried.",
                    kind,
                    session_id,
                )
                await self._finish_trigger(ledger_field, state="dropped")
                return
            try:
                input_msg = (
                    Msg.model_validate(raw_input)
                    if is_message
                    else _RESUME_INPUT_ADAPTER.validate_python(raw_input)
                )
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "WakeupDispatcher: dropping %s trigger for session "
                    "%s — input failed to parse: %r",
                    kind,
                    session_id,
                    raw_input,
                )
                await self._finish_trigger(ledger_field, state="dropped")
                return

        if await self._bus.is_locked(
            MessageBusKeys.session_lock(session_id),
        ):
            # The session is busy, so nothing can be spawned right now:
            # re-queue after a short backoff rather than dropping.
            #
            # ``wake`` needs this as much as the input-carrying kinds. A
            # producer only enqueues one after finding no registered
            # inbox consumer, which a finished run gives up *before* it
            # releases the session lock — so a held lock is no evidence
            # that anyone is still going to drain the inbox.
            self._schedule_retry(
                user_id,
                session_id,
                agent_id,
                kind,
                input_msg,
                trigger_id=ledger_field or trigger_id,
                recovery_policy=recovery_policy,
            )
            return

        # Orphan guard: the queue is unaware of session lifecycle. A
        # trigger enqueued before the session was deleted (e.g. by a
        # BG-task completion callback or a schedule trigger) will still
        # arrive here. Drop it rather than letting ChatService.run crash
        # on a missing storage record.
        if (
            await self._storage.get_session(user_id, agent_id, session_id)
            is None
        ):
            logger.warning(
                "WakeupDispatcher: dropping %s trigger for session %s "
                "(agent %s, user %s) — session no longer exists in "
                "storage; it was likely enqueued before the session was "
                "deleted.",
                kind,
                session_id,
                agent_id,
                user_id,
            )
            # Surface an error on the event stream so collectors (e.g. the
            # channel gateway) fail fast instead of waiting for a timeout.
            await publish_session_event(
                self._bus,
                session_id,
                ReplyEndEvent(
                    session_id=session_id,
                    reply_id="",
                    finished_reason=ReplyFinishedReason.ERROR,
                    error=ErrorInfo(
                        type=ErrorType.INTERNAL,
                        message="Session no longer exists.",
                    ),
                ).model_dump(mode="json"),
            )
            await self._finish_trigger(ledger_field, state="dropped")
            return

        # Claim only after the session is known to exist and it is idle.
        claimed = await self._claim_trigger(
            trigger_id=ledger_field or trigger_id,
            entry_id=entry_id,
            payload={
                "user_id": user_id,
                "session_id": session_id,
                "agent_id": agent_id,
                "kind": kind,
                "input": raw_input,
                **({"trigger_id": trigger_id} if trigger_id else {}),
            },
        )
        if claimed == "":
            return
        ledger_field = claimed or ledger_field
        claim_token = (
            self._claim_tokens.get(ledger_field)
            if ledger_field is not None
            else None
        )
        outcome_token: str | None = None
        begin_outcome = getattr(
            self._chat_service, "begin_run_outcome", None
        )
        if begin_outcome is not None:
            outcome_token = begin_outcome(session_id)

        async def _run() -> None:
            heartbeat: asyncio.Task | None = None
            # The renewal watcher is a child task. Keep the spawned run task
            # handle so a lost fencing token can stop the actual ChatService
            # coroutine before it performs any further external writes.
            run_task = asyncio.current_task()

            async def _heartbeat() -> None:
                try:
                    while True:
                        await asyncio.sleep(
                            max(
                                1.0,
                                MessageBusKeys.WAKEUP_CLAIM_TTL_SECS / 3,
                            )
                        )
                        try:
                            renewed = await self._renew_trigger_claim(
                                ledger_field, claim_token
                            )
                        except Exception:  # pragma: no cover - transient bus error
                            logger.exception(
                                "WakeupDispatcher: claim renewal failed "
                                "for session %s; cancelling stale run.",
                                session_id,
                            )
                            renewed = False
                        if not renewed:
                            logger.warning(
                                "WakeupDispatcher: claim lost for session %s; "
                                "cancelling run to fence external writes.",
                                session_id,
                            )
                            if run_task is not None and not run_task.done():
                                run_task.cancel()
                            return
                except asyncio.CancelledError:
                    raise

            if ledger_field is not None and claim_token is not None:
                heartbeat = asyncio.create_task(
                    _heartbeat(), name=f"wakeup-claim-renew:{session_id}"
                )
            try:
                run_kwargs = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "agent_id": agent_id,
                    "input_msg": input_msg,
                }
                if outcome_token is not None:
                    run_kwargs["_outcome_token"] = outcome_token
                succeeded = await self._chat_service.run(**run_kwargs)
            except asyncio.CancelledError:
                # Leave the record claimed. A subsequent process will
                # recover it, preserving the crash-after-drain guarantee.
                raise
            except Exception:
                # The run did not complete.  Leave the claim in place so a
                # subsequent dispatcher startup can safely recover it.  A
                # failed run must never be recorded as completed: doing so
                # permanently drops the trigger after a transient worker or
                # model failure.
                raise
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    try:
                        await heartbeat
                    except asyncio.CancelledError:
                        pass
            if succeeded is not True:
                # A preflight/infrastructure failure happened before the
                # agent entered business execution: release and retry. Once
                # execution started, the outcome may be unknown (a tool could
                # have produced an external side effect), so terminally drop
                # the trigger instead of replaying it after claim expiry.
                outcome_reader = getattr(
                    self._chat_service, "consume_run_outcome", None
                )
                outcome = "unknown"
                if outcome_reader is not None:
                    outcome = (
                        outcome_reader(session_id, outcome_token)
                        if outcome_token is not None
                        else outcome_reader(session_id)
                    )
                if outcome in {"not_started", "retryable"}:
                    await self._release_trigger_claim(
                        ledger_field, claim_token
                    )
                    retry_key = ledger_field or trigger_id or (
                        f"legacy:{session_id}:{kind}"
                    )
                    retry_count = await self._prepare_retry(retry_key)
                    if retry_count is None or retry_count > _MAX_PREFLIGHT_RETRIES:
                        self._retry_attempts.pop(retry_key, None)
                        await self._report_retry_exhausted(
                            user_id,
                            session_id,
                            agent_id,
                        )
                        await self._finish_trigger(ledger_field, state="failed")
                    else:
                        self._schedule_retry(
                            user_id,
                            session_id,
                            agent_id,
                            kind,
                            input_msg,
                            trigger_id=ledger_field or trigger_id,
                            retry_count=retry_count,
                            recovery_policy=recovery_policy,
                        )
                else:
                    await self._finish_trigger(
                        ledger_field,
                        # The run reached business execution and its outcome
                        # is no longer safe to replay automatically. Keep a
                        # distinct terminal state for manual inspection.
                        state="failed_unknown",
                        claim_token=claim_token,
                    )
                    self._retry_attempts.pop(
                        f"legacy:{session_id}:{kind}",
                        None,
                    )
                return
            # Consume the success outcome so stale status cannot leak into a
            # later run for the same session.
            outcome_reader = getattr(
                self._chat_service, "consume_run_outcome", None
            )
            if outcome_reader is not None:
                if outcome_token is not None:
                    outcome_reader(session_id, outcome_token)
                else:
                    outcome_reader(session_id)
            await self._finish_trigger(
                ledger_field,
                state="completed",
                claim_token=claim_token,
            )
            self._retry_attempts.pop(
                f"legacy:{session_id}:{kind}",
                None,
            )

        try:
            self._registry.spawn(
                _run(),
                session_id=session_id,
                name=f"{kind}-run:{session_id}",
            )
        except RuntimeError:
            # A local run was registered between the running-check and
            # the spawn. Re-queue so neither the carried input nor a
            # queued inbox payload is stranded — that run may already be
            # past its last drain.
            await self._release_trigger_claim(ledger_field, claim_token)
            self._schedule_retry(
                user_id,
                session_id,
                agent_id,
                kind,
                input_msg,
                trigger_id=ledger_field or trigger_id,
                recovery_policy=recovery_policy,
            )

    def _schedule_retry(
        self,
        user_id: str,
        session_id: str,
        agent_id: str,
        kind: str,
        input_msg: UserConfirmResultEvent
        | ExternalExecutionResultEvent
        | UserInterruptEvent
        | Msg
        | None,
        *,
        trigger_id: str | None = None,
        retry_count: int | None = None,
        recovery_policy: str = "manual_review",
    ) -> None:
        """Re-enqueue an input-carrying (``resume``/``message``) trigger
        after a short backoff.

        Spawns a detached timer that sleeps, then re-enqueues the trigger
        (which re-fires the signal, re-driving the drain). This keeps the
        input alive across the window where the running turn still holds
        the session lock, without a hot re-enqueue loop.

        Args:
            user_id (`str`):
                The owning user id.
            session_id (`str`):
                The session to trigger.
            agent_id (`str`):
                The agent that owns the session.
            kind (`str`):
                The trigger kind to re-enqueue (``resume`` / ``message``).
            input_msg:
                The parsed input to redeliver.
        """

        async def _retry() -> None:
            try:
                retry_attempt = retry_count or 0
                delay = min(
                    _PREFLIGHT_RETRY_BASE_SECS
                    * (2 ** max(0, retry_attempt - 1)),
                    5.0,
                )
                await asyncio.sleep(delay)
                await enqueue_run_trigger(
                    self._bus,
                    user_id=user_id,
                    session_id=session_id,
                    agent_id=agent_id,
                    kind=kind,  # type: ignore[arg-type]  # resume | message
                    inputs=input_msg,
                    trigger_id=trigger_id,
                    _replace_existing=(
                        trigger_id is not None
                        and getattr(self, "_ledger_supported", None)
                        is not False
                    ),
                    _retry_count=retry_count,
                    recovery_policy=recovery_policy,  # explicit replay-safe extension
                    # Compatibility buses without the durable ledger must
                    # not receive a generated trigger id: the consumer
                    # correctly treats an explicit id as ledger-backed and
                    # would drop it when no ledger can be queried.
                    _include_trigger_id=(
                        getattr(self, "_ledger_supported", None) is not False
                    ),
                )
            except asyncio.CancelledError:
                pass
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "WakeupDispatcher: failed to re-enqueue %s trigger "
                    "for session %s.",
                    kind,
                    session_id,
                )

        task = asyncio.create_task(
            _retry(),
            name=f"{kind}-retry:{session_id}",
        )
        self._retry_tasks.add(task)
        task.add_done_callback(self._retry_tasks.discard)
