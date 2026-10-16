# -*- coding: utf-8 -*-
"""Lifecycle owner for one browser WebRTC realtime session."""

import asyncio
import inspect
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta

from aiortc import RTCPeerConnection

from ..._logging import logger
from ...agent import RealtimeAgent
from ...event import (
    DataBlockDeltaEvent,
    DataBlockEndEvent,
    DataBlockStartEvent,
    ReplyEndEvent,
    ReplyStartEvent,
)
from ...state import AgentState
from .._bus_ops import publish_session_event
from ..message_bus import LockLostError, MessageBus, MessageBusKeys
from ..storage import StorageBase
from ._webrtc_audio_transport import WebRTCAudioTransport

_CHECKPOINT_LOCK_TTL_SECS = (
    MessageBusKeys.SESSION_EVENT_CHECKPOINT_LOCK_TTL_SECS
)


class WebRTCSession:
    """Run and persist one realtime agent over a peer connection."""

    def __init__(
        self,
        *,
        peer_connection: RTCPeerConnection,
        transport: WebRTCAudioTransport,
        agent_factory: Callable[[], Awaitable[RealtimeAgent]],
        storage: StorageBase,
        message_bus: MessageBus,
        user_id: str,
        agent_id: str,
        session_id: str,
        on_closed: Callable[["WebRTCSession"], None],
    ) -> None:
        self.peer_connection = peer_connection
        self.transport = transport
        self._agent_factory = agent_factory
        self.agent: RealtimeAgent | None = None
        self.storage = storage
        self.message_bus = message_bus
        self.user_id = user_id
        self.agent_id = agent_id
        self.session_id = session_id
        self._on_closed = on_closed
        self._task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._close_lock = asyncio.Lock()
        self._closing = False
        self._lock_acquired = asyncio.Event()
        self._persisted_messages: dict[str, str] = {}
        self._open_user_replies: set[str] = set()
        self._pcm_block_ids: set[str] = set()
        self._last_entry_id: str | None = None
        self._checkpoint_entry_id: str | None = None
        # The message-bus lock prevents two live runners from publishing at
        # once, while this durable lease fences a runner that survives a
        # process/Redis failover long enough for another worker to take over.
        self._lease_owner = f"webrtc:{uuid.uuid4().hex}"
        self._durable_lease: object | None = None
        self._durable_lease_lost = False
        self._lease_heartbeat: asyncio.Task[None] | None = None

    @staticmethod
    def _check_lock_lease(lease: object | None) -> None:
        """Fail closed when a distributed lock heartbeat reports a loss.

        Older/custom message buses used by callers and tests may not yield a
        lease handle.  Redis and the in-memory bus yield ``LockLease`` with a
        ``check`` method; invoke it when available before every side effect.
        """
        check = getattr(lease, "check", None)
        if callable(check):
            check()

    def _check_durable_lease(self) -> None:
        """Fail closed after a durable lease renewal or claim is lost."""
        if self._durable_lease_lost:
            raise LockLostError(
                f"Durable session lease lost for {self.session_id!r}",
            )

    async def _claim_durable_lease(self) -> None:
        """Claim the storage-backed session lease when the backend supports it."""
        claim = getattr(self.storage, "claim_session_lease", None)
        if claim is None:
            return
        try:
            lease = await claim(
                self.user_id,
                self.agent_id,
                self.session_id,
                self._lease_owner,
                timedelta(seconds=MessageBusKeys.SESSION_RUN_TTL_SECS),
            )
        except NotImplementedError:
            # Custom/legacy stores can still rely on the message-bus lock.
            return
        if lease is None:
            raise RuntimeError(
                f"Session {self.session_id!r} durable lease is already held.",
            )
        self._durable_lease = lease
        self._durable_lease_lost = False
        parent_task = asyncio.current_task()

        async def _renew() -> None:
            while True:
                await asyncio.sleep(
                    max(1.0, MessageBusKeys.SESSION_RUN_TTL_SECS / 3),
                )
                renew = getattr(self.storage, "renew_session_lease", None)
                if renew is None:
                    self._durable_lease_lost = True
                    logger.error(
                        "Session lease renewer is unavailable for %s; "
                        "cancelling the WebRTC run",
                        self.session_id,
                    )
                    if parent_task is not None:
                        parent_task.cancel()
                    return
                try:
                    renewed = await renew(
                        self.user_id,
                        self.agent_id,
                        self.session_id,
                        self._lease_owner,
                        lease.lease_epoch,
                        timedelta(seconds=MessageBusKeys.SESSION_RUN_TTL_SECS),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pylint: disable=broad-except
                    self._durable_lease_lost = True
                    logger.exception(
                        "Session lease heartbeat failed for %s; "
                        "cancelling the WebRTC run",
                        self.session_id,
                    )
                    if parent_task is not None:
                        parent_task.cancel()
                    return
                if not renewed:
                    self._durable_lease_lost = True
                    logger.error(
                        "Session lease renewal lost for %s; "
                        "cancelling the WebRTC run",
                        self.session_id,
                    )
                    if parent_task is not None:
                        parent_task.cancel()
                    return

        self._lease_heartbeat = asyncio.create_task(
            _renew(),
            name=f"webrtc-lease-renew:{self.session_id}",
        )

    async def _release_durable_lease(self) -> None:
        """Stop the lease heartbeat and conditionally release our fence."""
        heartbeat = self._lease_heartbeat
        self._lease_heartbeat = None
        if heartbeat is not None:
            heartbeat.cancel()
            try:
                await asyncio.shield(heartbeat)
            except asyncio.CancelledError:
                try:
                    await heartbeat
                except asyncio.CancelledError:
                    pass
                except Exception:  # pragma: no cover - defensive logging
                    logger.exception(
                        "Session lease heartbeat failed during cleanup",
                    )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Session lease heartbeat failed during cleanup")

        lease = self._durable_lease
        self._durable_lease = None
        if lease is None:
            return
        release = getattr(self.storage, "release_session_lease", None)
        if release is None:
            return
        release_task = asyncio.create_task(
            release(
                self.user_id,
                self.agent_id,
                self.session_id,
                self._lease_owner,
                lease.lease_epoch,
            ),
            name=f"webrtc-lease-release:{self.session_id}",
        )
        try:
            await asyncio.shield(release_task)
        except asyncio.CancelledError:
            try:
                await release_task
            except asyncio.CancelledError:
                pass
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to release session lease for %s",
                    self.session_id,
                )
        except Exception:  # pragma: no cover - defensive logging
            logger.exception(
                "Failed to release session lease for %s",
                self.session_id,
            )

    def start(self) -> None:
        """Start the agent pump after WebRTC negotiation succeeds."""
        if self._task is not None:
            raise RuntimeError("The WebRTC session is already running.")
        self._task = asyncio.create_task(
            self._run(),
            name=f"webrtc-session-{self.session_id}",
        )

    def request_close(self) -> None:
        """Schedule one owned close task from a synchronous callback."""
        if self._closing or self._close_task is not None:
            return
        self._close_task = asyncio.create_task(
            self.close(),
            name=f"webrtc-session-close-{self.session_id}",
        )
        self._close_task.add_done_callback(self._on_close_done)

    def _on_close_done(self, task: asyncio.Task[None]) -> None:
        """Retrieve an asynchronous close error instead of losing it."""
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Failed to close WebRTC session %r: %s",
                self.session_id,
                error,
            )

    async def close(self) -> None:
        """End the transport and wait for persistence to finish."""
        async with self._close_lock:
            if not self._closing:
                self._closing = True
                await self.transport.close()

        task = self._task
        if task is not None and task is not asyncio.current_task():
            if not self._lock_acquired.is_set():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        elif task is None:
            await self.peer_connection.close()
            self._on_closed(self)

    async def wait_until_lock_acquired(self, timeout: float) -> bool:
        """Wait until this runner owns the distributed session lock."""
        try:
            await asyncio.wait_for(
                self._lock_acquired.wait(),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            return False
        return True

    async def _run(self) -> None:
        """Hold the session lock while forwarding events to the SSE log."""
        try:
            async with self.message_bus.acquire_lock(
                MessageBusKeys.session_lock(self.session_id),
                ttl_secs=MessageBusKeys.SESSION_RUN_TTL_SECS,
            ) as session_lease:
                await self._claim_durable_lease()
                self._lock_acquired.set()
                try:
                    self.agent = await self._agent_factory()
                    self._persisted_messages = {
                        message.id: message.model_dump_json()
                        for message in self.agent.state.context
                    }
                    async with self.transport:
                        async with self.agent:
                            async for event in self.agent.reply_stream(
                                self.transport,
                            ):
                                if isinstance(event, DataBlockStartEvent) and (
                                    event.media_type.startswith("audio/pcm")
                                ):
                                    self._pcm_block_ids.add(event.block_id)
                                    continue
                                if isinstance(
                                    event,
                                    DataBlockDeltaEvent,
                                ) and event.media_type.startswith("audio/pcm"):
                                    self._pcm_block_ids.add(event.block_id)
                                    continue
                                if (
                                    isinstance(event, DataBlockEndEvent)
                                    and event.block_id in self._pcm_block_ids
                                ):
                                    self._pcm_block_ids.discard(event.block_id)
                                    continue
                                self._check_lock_lease(session_lease)
                                self._check_durable_lease()
                                checkpoint = self.agent.checkpoint_snapshot(
                                    event,
                                )
                                payload = event.model_dump(mode="json")
                                # A lock heartbeat can mark this runner stale
                                # while the realtime stream is yielding.  Do
                                # not publish a stale event after takeover.
                                self._check_lock_lease(session_lease)
                                entry_id = await publish_session_event(
                                    self.message_bus,
                                    self.session_id,
                                    payload,
                                )
                                self._last_entry_id = entry_id
                                if (
                                    isinstance(event, ReplyStartEvent)
                                    and event.role == "user"
                                ):
                                    self._open_user_replies.add(
                                        event.reply_id,
                                    )
                                elif isinstance(event, ReplyEndEvent):
                                    self._open_user_replies.discard(
                                        event.reply_id,
                                    )
                                if (
                                    checkpoint is not None
                                    and not self._open_user_replies
                                ):
                                    try:
                                        await self._checkpoint_state(
                                            entry_id,
                                            checkpoint,
                                            session_lease,
                                        )
                                    except LockLostError:
                                        raise
                                    except Exception as exc:
                                        logger.warning(
                                            "Failed to checkpoint WebRTC "
                                            "session %r: %s",
                                            self.session_id,
                                            exc,
                                        )
                finally:
                    try:
                        async with self.message_bus.acquire_lock(
                            MessageBusKeys.session_event_checkpoint_lock(
                                self.session_id,
                            ),
                            ttl_secs=_CHECKPOINT_LOCK_TTL_SECS,
                        ) as checkpoint_lease:
                            self._check_lock_lease(session_lease)
                            self._check_lock_lease(checkpoint_lease)
                            self._check_durable_lease()
                            await self._persist_state(
                                session_lease=session_lease,
                                checkpoint_lease=checkpoint_lease,
                            )
                            if (
                                self._last_entry_id is not None
                                and self._last_entry_id
                                != self._checkpoint_entry_id
                            ):
                                await self._set_checkpoint_cursor(
                                    self._last_entry_id,
                                    session_lease=session_lease,
                                    checkpoint_lease=checkpoint_lease,
                                )
                    except Exception as exc:
                        logger.exception(
                            "Failed to persist WebRTC session %r: %s",
                            self.session_id,
                            exc,
                        )
        except Exception as exc:
            logger.exception(
                "WebRTC session %r failed: %s",
                self.session_id,
                exc,
            )
            self.transport.send_error(str(exc))
        finally:
            await self._release_durable_lease()
            async with self._close_lock:
                self._closing = True
            await self.transport.close()
            await self.peer_connection.close()
            self._on_closed(self)

    async def _checkpoint_state(
        self,
        entry_id: str | None,
        state: AgentState,
        session_lease: object | None = None,
    ) -> None:
        """Persist state and bind it to the replay entry it includes."""
        async with self.message_bus.acquire_lock(
            MessageBusKeys.session_event_checkpoint_lock(self.session_id),
            ttl_secs=_CHECKPOINT_LOCK_TTL_SECS,
        ) as checkpoint_lease:
            self._check_lock_lease(session_lease)
            self._check_lock_lease(checkpoint_lease)
            self._check_durable_lease()
            await self._persist_state(
                state,
                session_lease=session_lease,
                checkpoint_lease=checkpoint_lease,
            )
            if entry_id is not None:
                await self._set_checkpoint_cursor(
                    entry_id,
                    session_lease=session_lease,
                    checkpoint_lease=checkpoint_lease,
                )

    async def _set_checkpoint_cursor(
        self,
        entry_id: str,
        *,
        session_lease: object | None = None,
        checkpoint_lease: object | None = None,
    ) -> None:
        """Record the last event included in the persisted state."""
        self._check_lock_lease(session_lease)
        self._check_lock_lease(checkpoint_lease)
        self._check_durable_lease()
        await self.message_bus.registry_set(
            MessageBusKeys.session_event_checkpoint(self.session_id),
            MessageBusKeys.SESSION_EVENT_CURSOR_FIELD,
            entry_id,
        )
        self._checkpoint_entry_id = entry_id
        self._check_lock_lease(session_lease)
        self._check_lock_lease(checkpoint_lease)
        self._check_durable_lease()
        await self.message_bus.log_trim(
            MessageBusKeys.session_events(self.session_id),
            before_id=entry_id,
        )

    async def _persist_state(
        self,
        state: AgentState | None = None,
        *,
        session_lease: object | None = None,
        checkpoint_lease: object | None = None,
    ) -> None:
        """Persist the latest complete messages and agent state."""
        if self.agent is None:
            return
        state = state or self.agent.state
        current_messages = {
            message.id: message.model_dump_json() for message in state.context
        }

        # Durable backends expose one fenced transaction for the message
        # snapshot and session state.  Prefer it whenever available so a
        # worker that lost its storage lease cannot append a late message or
        # overwrite a successor's state between two independent writes.
        checkpoint = getattr(
            self.storage,
            "persist_session_checkpoint",
            None,
        )
        if checkpoint is not None:
            self._check_lock_lease(session_lease)
            self._check_lock_lease(checkpoint_lease)
            self._check_durable_lease()
            checkpoint_args: dict[str, object] = {
                "user_id": self.user_id,
                "agent_id": self.agent_id,
                "session_id": self.session_id,
                "messages": list(state.context),
                "state": state,
            }
            if self._durable_lease is not None:
                checkpoint_args.update(
                    lease_owner=self._lease_owner,
                    lease_epoch=self._durable_lease.lease_epoch,
                )
            try:
                parameters = inspect.signature(checkpoint).parameters
            except (TypeError, ValueError):
                parameters = {}
            if parameters and not any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            ):
                checkpoint_args = {
                    key: value
                    for key, value in checkpoint_args.items()
                    if key in parameters
                }
            try:
                checkpointed = await checkpoint(**checkpoint_args)
            except NotImplementedError:
                checkpoint = None
            else:
                if checkpointed is False:
                    raise LockLostError(
                        f"Session lease fence rejected checkpoint for "
                        f"{self.session_id!r}",
                    )
                previous_ids = set(self._persisted_messages)
                self._persisted_messages = current_messages.copy()
                # The atomic API intentionally merges/upserts messages.  A
                # removed message therefore still needs an explicitly fenced
                # delete while the same bus/checkpoint locks are held.
                for message_id in sorted(previous_ids - current_messages.keys()):
                    self._check_lock_lease(session_lease)
                    self._check_lock_lease(checkpoint_lease)
                    self._check_durable_lease()
                    await self.storage.delete_message(
                        self.user_id,
                        self.session_id,
                        message_id,
                    )
                return

        for message in state.context:
            serialized = current_messages[message.id]
            if self._persisted_messages.get(message.id) == serialized:
                continue
            self._check_lock_lease(session_lease)
            self._check_lock_lease(checkpoint_lease)
            self._check_durable_lease()
            await self.storage.upsert_message(
                self.user_id,
                self.session_id,
                message,
            )
            self._persisted_messages[message.id] = serialized
        self._check_lock_lease(session_lease)
        self._check_lock_lease(checkpoint_lease)
        self._check_durable_lease()
        update_args: dict[str, object] = {
            "user_id": self.user_id,
            "agent_id": self.agent_id,
            "session_id": self.session_id,
            "state": state,
        }
        if self._durable_lease is not None:
            update_args.update(
                lease_owner=self._lease_owner,
                lease_epoch=self._durable_lease.lease_epoch,
            )
        update = self.storage.update_session_state
        try:
            parameters = inspect.signature(update).parameters
        except (TypeError, ValueError):
            parameters = {}
        if parameters and not any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        ):
            update_args = {
                key: value for key, value in update_args.items() if key in parameters
            }
        await update(**update_args)
        removed_ids = self._persisted_messages.keys() - current_messages.keys()
        for message_id in sorted(removed_ids):
            self._check_lock_lease(session_lease)
            self._check_lock_lease(checkpoint_lease)
            self._check_durable_lease()
            await self.storage.delete_message(
                self.user_id,
                self.session_id,
                message_id,
            )
            self._persisted_messages.pop(message_id, None)
