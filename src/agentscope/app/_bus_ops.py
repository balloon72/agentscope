# -*- coding: utf-8 -*-
"""Business-level operations built on top of MessageBus primitives.

These helpers compose generic bus primitives (``log_append``, ``publish``,
``queue_push``) with domain-specific key layouts from ``MessageBusKeys``.
They live here — between the transport layer (``message_bus``) and the
service layer (``_service``) — so that neither layer needs to know about the
other's internals.

.. list-table::
   :widths: 30 70

   * - :func:`publish_session_event`
     - Append an event to the session replay log and fan it out live.
   * - :func:`enqueue_run_trigger`
     - Enqueue a typed run trigger and signal dispatchers.
   * - :func:`enqueue_index_task`
     - Enqueue a knowledge-document indexing task and signal consumers.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import TYPE_CHECKING, Literal

from .message_bus._keys import MessageBusKeys

if TYPE_CHECKING:
    from .message_bus._base import MessageBus

    from agentscope.event import (
        ExternalExecutionResultEvent,
        UserConfirmResultEvent,
        UserInterruptEvent,
    )
    from agentscope.message import Msg


# ── publish_session_event ──────────────────────────────────────────────


async def publish_session_event(
    bus: "MessageBus",
    session_id: str,
    event: dict,
) -> str:
    """Append event to replay log + fan out live.

    Args:
        bus (`MessageBus`):
            The application message bus.
        session_id (`str`):
            The session this event belongs to.
        event (`dict`):
            JSON-serializable event payload.

    Returns:
        `str`:
            The replay-log entry id assigned by the backend.
    """
    key = MessageBusKeys.session_events(session_id)
    entry_id = await bus.log_append(
        key,
        event,
        max_len=MessageBusKeys.SESSION_REPLAY_MAX_LEN,
    )
    await bus.publish(key, {**event, "_entry_id": entry_id})
    return entry_id


# ── enqueue_run_trigger ────────────────────────────────────────────────


async def enqueue_run_trigger(
    bus: "MessageBus",
    user_id: str,
    session_id: str,
    agent_id: str,
    *,
    kind: Literal[
        "wake",
        "resume",
        "message",
    ] = MessageBusKeys.WAKEUP_KIND_WAKE,
    inputs: UserConfirmResultEvent
    | ExternalExecutionResultEvent
    | UserInterruptEvent
    | Msg
    | dict
    | None = None,
    trigger_id: str | None = None,
    _replace_existing: bool = False,
    _include_trigger_id: bool = True,
    _retry_count: int | None = None,
    recovery_policy: Literal["manual_review", "replay_safe"] = "manual_review",
) -> bool:
    """Enqueue a typed run trigger and signal dispatchers.

    A trigger is recorded in a durable ledger before it is placed on the
    destructive Redis queue.  The dispatcher marks the record claimed and
    completed around the spawned run, so a process that dies after draining
    an entry can recover it on the next startup.  ``resume`` triggers get a
    deterministic identity derived from their parked reply/tool call; a
    repeated card click therefore returns ``False`` instead of adding a
    second resume.

    ``recovery_policy`` defaults to ``manual_review``. A claimed trigger has
    an unknown outcome after a process crash and is therefore never replayed
    automatically. Callers may opt into ``replay_safe`` only for explicitly
    read-only or idempotent work. This is an extension point, not an
    exactly-once guarantee.

    ``trigger_id`` and ``_replace_existing`` are internal knobs used when a
    dispatcher requeues an entry after a busy-session retry.  Existing
    callers can ignore the boolean return value.
    """
    if inputs is None:
        input_payload = None
    elif hasattr(inputs, "model_dump"):
        input_payload = inputs.model_dump(mode="json")
    else:
        input_payload = inputs
    payload = {
        "user_id": user_id,
        "session_id": session_id,
        "agent_id": agent_id,
        "kind": kind,
        "input": input_payload,
        "recovery_policy": recovery_policy,
    }

    if trigger_id is None:
        if kind == MessageBusKeys.WAKEUP_KIND_WAKE:
            # A wake ID identifies one logical inbox payload. Callers that
            # have the payload (deliver_to_inbox) provide a deterministic
            # digest; direct wake callers get an independent trigger.
            trigger_id = f"wake:{uuid.uuid4().hex}"
        elif kind == MessageBusKeys.WAKEUP_KIND_RESUME:
            # The reply id and tool-call ids identify the parked decision;
            # deliberately ignore the approval bit so two clicks on either
            # card action cannot race two resumes through the queue.
            identity = {
                "user_id": user_id,
                "session_id": session_id,
                "agent_id": agent_id,
                "kind": kind,
                "reply_id": (input_payload or {}).get("reply_id"),
                "tool_call_ids": sorted(
                    {
                        *(
                            str(item.get("tool_call", {}).get("id"))
                            for item in (input_payload or {}).get(
                                "confirm_results", []
                            )
                            if isinstance(item, dict)
                            and isinstance(item.get("tool_call"), dict)
                            and item.get("tool_call", {}).get("id")
                        ),
                        *(
                            str(item.get("id"))
                            for item in (input_payload or {}).get(
                                "execution_results", []
                            )
                            if isinstance(item, dict) and item.get("id")
                        ),
                    }
                ),
                "input_type": (input_payload or {}).get("type"),
            }
            digest = hashlib.sha256(
                json.dumps(
                    identity,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8"),
            ).hexdigest()
            trigger_id = f"resume:{digest}"
        else:
            trigger_id = f"trigger:{uuid.uuid4().hex}"

    # Modern producers include the id on every payload so the dispatcher
    # can resolve the ledger record in O(1). The ledger-less compatibility
    # path removes it below before enqueueing.
    if _include_trigger_id:
        payload["trigger_id"] = trigger_id

    ledger_available = await _wakeup_ledger_available(bus)
    if not ledger_available:
        # Ledger-less buses use the legacy compatibility path. Do not attach
        # a trigger id that the dispatcher cannot resolve, or resume/message
        # entries would be dropped as "unknown trigger".
        payload.pop("trigger_id", None)
        await bus.queue_push(MessageBusKeys.wakeup_queue(), payload)
        await bus.publish(MessageBusKeys.wakeup_signal(), {})
        return True

    namespace = MessageBusKeys.wakeup_ledger()
    async with bus.acquire_lock(
        MessageBusKeys.wakeup_ledger_lock(),
        ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
    ):
        existing = await bus.registry_get(namespace, trigger_id)
        if existing:
            if not _replace_existing:
                try:
                    existing_record = (
                        json.loads(existing)
                        if isinstance(existing, (str, bytes))
                        else existing
                    )
                except (TypeError, ValueError):
                    existing_record = None
                if (
                    kind == MessageBusKeys.WAKEUP_KIND_WAKE
                    and isinstance(existing_record, dict)
                    and existing_record.get("state")
                    in {"completed", "failed", "failed_unknown", "manual_review", "dropped"}
                ):
                    trigger_id = f"wake:{user_id}:{session_id}:{uuid.uuid4().hex}"
                    payload["trigger_id"] = trigger_id
                    existing = None
                else:
                    return False
            if existing is not None:
                # A deferred dispatch may have a retry timer still pending
                # when another worker claims (or completes) the same trigger.
                # Never let that stale timer reset a terminal/in-flight
                # record back to queued and run the input a second time.
                # Replacement is only valid while the current record is
                # still queued.
                try:
                    existing_record = (
                        json.loads(existing)
                        if isinstance(existing, (str, bytes))
                        else existing
                    )
                except (TypeError, ValueError):
                    return False
                if not isinstance(existing_record, dict):
                    return False
                if existing_record.get("state") != "queued":
                    return False
        # Preserve dispatcher retry metadata when replacing a queued record.
        # This prevents a transient preflight failure from resetting its
        # attempt budget on every re-enqueue.
        preserved = {}
        if existing and _replace_existing:
            for key in ("retry_count", "last_retry_at"):
                if key in existing_record:
                    preserved[key] = existing_record[key]
        if _retry_count is not None:
            # Retry timers can complete out of order.  A late enqueue must
            # never move a durable retry budget backwards, otherwise an
            # already exhausted trigger could receive extra attempts.
            previous_retry_count = preserved.get("retry_count")
            if isinstance(previous_retry_count, int):
                preserved["retry_count"] = max(
                    previous_retry_count,
                    _retry_count,
                )
            else:
                preserved["retry_count"] = _retry_count
        record = {
            "trigger_id": trigger_id,
            "state": "queued",
            # Keep the recovery policy alongside the ledger state. The
            # dispatcher reads this record when deciding whether an expired
            # claim may be replayed; storing it only in the queue payload
            # makes replay-safe producers look unsafe during recovery.
            "recovery_policy": recovery_policy,
            "payload": payload,
            "queue_id": None,
            "queued_at": __import__("time").time(),
            **preserved,
        }
        await bus.registry_set(
            namespace,
            trigger_id,
            json.dumps(record, ensure_ascii=False, separators=(",", ":")),
            ttl_secs=MessageBusKeys.WAKEUP_LEDGER_TTL_SECS,
        )
        try:
            queue_id = await bus.queue_push(
                MessageBusKeys.wakeup_queue(),
                payload,
            )
        except Exception:
            # Keep the queued ledger record. Startup recovery will retry the
            # push after a transient Redis failure.
            raise
        record["queue_id"] = str(queue_id)
        await bus.registry_set(
            namespace,
            trigger_id,
            json.dumps(record, ensure_ascii=False, separators=(",", ":")),
            ttl_secs=MessageBusKeys.WAKEUP_LEDGER_TTL_SECS,
        )

    await bus.publish(MessageBusKeys.wakeup_signal(), {})
    return True


async def _wakeup_ledger_available(bus: "MessageBus") -> bool:
    """Probe the optional ledger API, caching the result per bus instance.

    A few narrow unit-test fakes implement only queue/pubsub primitives.  Do
    not make those tests (or old third-party bus implementations) fail merely
    because they predate the ledger; Redis and InMemoryMessageBus both return
    a real ``dict`` here and take the durable path.
    """
    cached = getattr(bus, "_wakeup_ledger_supported", None)
    if cached is not None:
        return bool(cached)
    try:
        snapshot = await bus.registry_getall(MessageBusKeys.wakeup_ledger())
    except (AttributeError, NotImplementedError, TypeError):
        # Compatibility fakes / pre-ledger bus implementations. Redis
        # connection failures deliberately propagate so callers do not
        # silently downgrade a durable trigger to at-most-once delivery.
        setattr(bus, "_wakeup_ledger_supported", False)
        return False
    supported = isinstance(snapshot, dict)
    setattr(bus, "_wakeup_ledger_supported", supported)
    return supported


# ── session inbox hand-off ─────────────────────────────────────────────
#
# Three helpers implementing one protocol, whose only job is to make
# sure a payload pushed to a session inbox is always consumed by *some*
# run rather than sitting there until the next user turn.
#
# The naive version — "push, then wake the session unless it looks
# busy" — loses entries: a run that already performed its last drain is
# still busy (it is streaming, persisting, releasing its lock), so the
# producer skips the wake-up while the run will never look again.
#
# The fix is to make two tiny critical sections mutually exclusive
# under :meth:`MessageBusKeys.inbox_lock`:
#
#   producer   push entry            → read consumer flag
#   consumer   drain remaining entries → clear consumer flag
#
# Whichever runs first, the entry is covered. Producer first → the
# consumer's drain sees the entry and keeps going. Consumer first →
# the flag is already clear, so the producer enqueues a wake-up.
# Because the wake-up is only produced when no consumer is registered,
# it never spawns a run that has nothing to do.


async def deliver_to_inbox(
    bus: "MessageBus",
    *,
    user_id: str,
    session_id: str,
    agent_id: str,
    payload: dict,
) -> None:
    """Push a payload to a session inbox and wake the session if no run
    is currently consuming it.

    Args:
        bus (`MessageBus`):
            The application message bus.
        user_id (`str`):
            The owning user id.
        session_id (`str`):
            The session whose inbox receives the payload.
        agent_id (`str`):
            The agent that owns the session.
        payload (`dict`):
            JSON-serialisable payload, normally a serialised
            :class:`~agentscope.message.HintBlock`.
    """
    async with bus.acquire_lock(
        MessageBusKeys.inbox_lock(session_id),
        ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
    ):
        await bus.queue_push(MessageBusKeys.inbox(session_id), payload)
        consumer = await bus.registry_get(
            MessageBusKeys.inbox_consumer(session_id),
            MessageBusKeys.INBOX_CONSUMER_FIELD,
        )

    if consumer is None:
        wake_digest = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            .encode("utf-8")
        ).hexdigest()
        await enqueue_run_trigger(
            bus,
            user_id=user_id,
            session_id=session_id,
            agent_id=agent_id,
            trigger_id=f"wake:{wake_digest}",
        )


async def register_inbox_consumer(bus: "MessageBus", session_id: str) -> None:
    """Mark this run as the consumer of ``session_id``'s inbox.

    Call once per run, as early as possible — any producer that pushes
    after this point relies on this run draining the entry rather than
    enqueueing its own wake-up.

    The flag carries the same lease as a chat run, so a process that
    dies mid-run stops suppressing wake-ups once the lease expires.

    Args:
        bus (`MessageBus`):
            The application message bus.
        session_id (`str`):
            The session being consumed.
    """
    await bus.registry_set(
        MessageBusKeys.inbox_consumer(session_id),
        MessageBusKeys.INBOX_CONSUMER_FIELD,
        "1",
        ttl_secs=MessageBusKeys.SESSION_RUN_TTL_SECS,
    )


async def has_pending_inbox_or_release(
    bus: "MessageBus",
    session_id: str,
) -> bool:
    """Report whether the inbox still holds anything, releasing the
    consumer registration when it does not.

    Call at the very end of a run, before it releases the session lock.
    ``True`` means the run must go around once more — it stays
    registered as the consumer, so producers keep deferring to it
    instead of enqueuing their own wake-up. ``False`` means the run may
    finish: the registration is gone, so the next producer wakes the
    session itself.

    The check reads by draining and putting everything straight back,
    because the bus deliberately exposes no non-destructive peek. Both
    halves happen under :meth:`MessageBusKeys.inbox_lock`, which every
    producer also holds while pushing, so nothing can slip in between
    and arrival order is preserved.

    Args:
        bus (`MessageBus`):
            The application message bus.
        session_id (`str`):
            The session being consumed.

    Returns:
        `bool`:
            ``True`` when payloads remain to be consumed.
    """
    inbox = MessageBusKeys.inbox(session_id)
    async with bus.acquire_lock(
        MessageBusKeys.inbox_lock(session_id),
        ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
    ):
        payloads: list[dict] = []
        while True:
            batch = await bus.queue_drain(inbox, max_count=100)
            if not batch:
                break
            payloads.extend(payload for _entry_id, payload in batch)

        if not payloads:
            await bus.registry_del(
                MessageBusKeys.inbox_consumer(session_id),
                MessageBusKeys.INBOX_CONSUMER_FIELD,
            )
            return False

        for payload in payloads:
            await bus.queue_push(inbox, payload)
        return True


async def abandon_inbox_consumer(
    bus: "MessageBus",
    *,
    user_id: str,
    session_id: str,
    agent_id: str,
) -> None:
    """Give up the consumer registration without having drained the
    inbox, waking the session when payloads are still queued.

    Used when a run ends abnormally — an interrupt, a cancelled task, a
    failed turn. Producers deferred to this run while it was
    registered, so it cannot simply drop the registration and leave
    their payloads unattended.

    Args:
        bus (`MessageBus`):
            The application message bus.
        user_id (`str`):
            The owning user id.
        session_id (`str`):
            The session being abandoned.
        agent_id (`str`):
            The agent that owns the session.
    """
    pending = await has_pending_inbox_or_release(bus, session_id)
    if not pending:
        return

    await bus.registry_del(
        MessageBusKeys.inbox_consumer(session_id),
        MessageBusKeys.INBOX_CONSUMER_FIELD,
    )
    await enqueue_run_trigger(
        bus,
        user_id=user_id,
        session_id=session_id,
        agent_id=agent_id,
    )


# ── enqueue_index_task ─────────────────────────────────────────────────


async def enqueue_index_task(
    bus: "MessageBus",
    user_id: str,
    knowledge_base_id: str,
    document_id: str,
) -> None:
    """Enqueue a knowledge-document indexing task and signal consumers.

    Pushes a structured payload onto the durable index-task queue and
    publishes a signal so any subscribed
    :class:`~agentscope.app._service.IndexTaskConsumer` drains it within
    one ``subscribe`` round-trip.

    The push happens *before* the publish so a worker woken by the
    signal is guaranteed to find the entry on its drain.  Re-enqueuing
    the same document is safe — the worker's lease CAS rejects
    duplicates — so the queue may legitimately hold multiple entries
    for the same document (one from upload, one from sweeper).

    Args:
        bus (`MessageBus`):
            The application message bus.
        user_id (`str`):
            The owning user id.
        knowledge_base_id (`str`):
            The parent knowledge base id.
        document_id (`str`):
            The document id to index.
    """
    await bus.queue_push(
        MessageBusKeys.index_tasks_queue(),
        {
            "user_id": user_id,
            "knowledge_base_id": knowledge_base_id,
            "document_id": document_id,
        },
    )
    await bus.publish(MessageBusKeys.index_tasks_signal(), {})
