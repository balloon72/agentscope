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
from typing import TYPE_CHECKING, Self

from pydantic import TypeAdapter

from ..._logging import logger
from ...event import (
    ExternalExecutionResultEvent,
    ReplyEndEvent,
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
        """
        self._bus = message_bus
        self._storage = storage
        self._chat_service = chat_service
        self._registry = chat_run_registry
        self._task: asyncio.Task | None = None
        # Detached backoff timers for deferred ``resume`` re-enqueues.
        # Held so they are not garbage-collected mid-sleep and can be
        # cancelled on shutdown.
        self._retry_tasks: set[asyncio.Task] = set()

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
        await ready.wait()
        await self._recover_trigger_ledger()
        await self._drain_and_dispatch()
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
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

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
        try:
            async for _signal in self._bus.subscribe(
                MessageBusKeys.wakeup_signal(),
                on_ready=ready.set,
            ):
                await self._drain_and_dispatch()
        except Exception:  # pylint: disable=broad-except
            logger.exception(
                "WakeupDispatcher loop crashed; subscription ended.",
            )

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

    async def _find_trigger(
        self,
        *,
        trigger_id: str | None,
        entry_id: str | None,
        payload: dict,
    ) -> tuple[str, dict] | None:
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
            try:
                current_record = json.loads(current) if current else record
            except (TypeError, ValueError):
                current_record = record
            state = current_record.get("state")
            if state in {"claimed", "completed", "dropped"}:
                return ""
            current_record["state"] = "claimed"
            await self._set_record(field, current_record)
        return field

    async def _finish_trigger(self, field: str | None, *, state: str) -> None:
        if not field:
            return
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
        parsed["state"] = state
        await self._set_record(field, parsed)

    async def _recover_trigger_ledger(self) -> None:
        """Requeue records left queued/claimed by a dead dispatcher.

        Queue drain is intentionally destructive, so a worker can disappear
        after XRANGE/XDEL but before spawning the chat run. Re-pushing every
        non-terminal ledger record is safe: the dispatcher claims records
        under the same ledger lock and skips duplicate queue entries.
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
            for field, record in records.items():
                if record.get("state") not in {"queued", "claimed"}:
                    continue
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    record["state"] = "dropped"
                    await self._set_record(field, record)
                    continue
                record["state"] = "queued"
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
                await self._set_record(field, record)
        await self._bus.publish(MessageBusKeys.wakeup_signal(), {})

    async def _drain_and_dispatch(self) -> None:
        """Read up to a batch of trigger entries and dispatch each."""
        try:
            raw_entries = await self._bus.queue_drain(
                MessageBusKeys.wakeup_queue(),
                max_count=64,
            )
            entries = raw_entries
        except Exception:  # pylint: disable=broad-except
            logger.exception("WakeupDispatcher: dequeue_wakeups failed.")
            return

        for entry_id, payload in entries:
            try:
                user_id = payload["user_id"]
                session_id = payload["session_id"]
                agent_id = payload["agent_id"]
            except (KeyError, TypeError):
                logger.warning(
                    "WakeupDispatcher: skipping malformed trigger entry %r",
                    payload,
                )
                continue
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
            if ledger_record.get("state") in {"claimed", "completed", "dropped"}:
                # A duplicate queue copy is expected during recovery. The
                # original owner (or a later recovery pass) owns this record.
                return

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

        async def _run() -> None:
            try:
                await self._chat_service.run(
                    user_id=user_id,
                    session_id=session_id,
                    agent_id=agent_id,
                    input_msg=input_msg,
                )
            except asyncio.CancelledError:
                # Leave the record claimed. A subsequent process will
                # recover it, preserving the crash-after-drain guarantee.
                raise
            except Exception:
                await self._finish_trigger(ledger_field, state="completed")
                raise
            else:
                await self._finish_trigger(ledger_field, state="completed")

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
            self._schedule_retry(
                user_id,
                session_id,
                agent_id,
                kind,
                input_msg,
                trigger_id=ledger_field or trigger_id,
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
                await asyncio.sleep(_RESUME_RETRY_BACKOFF_SECS)
                await enqueue_run_trigger(
                    self._bus,
                    user_id=user_id,
                    session_id=session_id,
                    agent_id=agent_id,
                    kind=kind,  # type: ignore[arg-type]  # resume | message
                    inputs=input_msg,
                    trigger_id=trigger_id,
                    _replace_existing=trigger_id is not None,
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
