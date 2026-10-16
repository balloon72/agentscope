# -*- coding: utf-8 -*-
"""Focused durable session lease/fencing tests for both storage backends."""
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase, skipUnless

try:
    import fakeredis.aioredis as _fakeredis
except ImportError:  # pragma: no cover - optional storage extra
    _fakeredis = None

try:
    import aiosqlite as _aiosqlite
    import sqlalchemy as _sqlalchemy
except ImportError:  # pragma: no cover - optional storage extra
    _aiosqlite = None
    _sqlalchemy = None

from agentscope.app.storage import (
    AsyncSQLAlchemyStorage,
    ChatModelConfig,
    RedisStorage,
    SessionConfig,
)
from agentscope.message import UserMsg
from agentscope.state import AgentState


def _config() -> SessionConfig:
    return SessionConfig(
        workspace_id="ws",
        chat_model_config=ChatModelConfig(
            type="openai",
            credential_id="cred",
            model="gpt-4o",
            parameters={},
        ),
    )


class _SessionFencingAssertions:
    async def assert_fencing(self, storage) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        session = await storage.upsert_session(
            "u", "a", _config(), session_id="s-fence",
        )
        self.assertEqual(session.lease_epoch, 0)
        first = await storage.claim_session_lease(
            "u", "a", session.id, "worker-a", timedelta(seconds=10), now,
        )
        self.assertIsNotNone(first)
        self.assertEqual(first.lease_owner, "worker-a")
        self.assertEqual(first.lease_epoch, 1)
        self.assertIsNone(
            await storage.claim_session_lease(
                "u", "a", session.id, "worker-b", timedelta(seconds=10), now,
            ),
        )
        self.assertTrue(
            await storage.update_session_state_fenced(
                "u", "a", session.id, AgentState(), "worker-a", 1, now=now,
            ),
        )
        current = await storage.get_session("u", "a", session.id)
        self.assertEqual(current.state_version, 1)
        self.assertFalse(
            await storage.update_session_state_fenced(
                "u", "a", session.id, AgentState(), "worker-b", 1, now=now,
            ),
        )
        self.assertTrue(
            await storage.renew_session_lease(
                "u", "a", session.id, "worker-a", 1,
                timedelta(seconds=10), now,
            ),
        )
        self.assertTrue(
            await storage.release_session_lease(
                "u", "a", session.id, "worker-a", 1, now=now,
            ),
        )
        second = await storage.claim_session_lease(
            "u", "a", session.id, "worker-b", timedelta(seconds=10), now,
        )
        self.assertIsNotNone(second)
        self.assertEqual(second.lease_epoch, 2)
        self.assertFalse(
            await storage.renew_session_lease(
                "u", "a", session.id, "worker-a", 1,
                timedelta(seconds=10), now,
            ),
        )

    async def assert_config_update_preserves_state_and_lease(
        self,
        storage,
    ) -> None:
        """A config write must not restore an older state or lease."""
        session = await storage.upsert_session(
            "u", "a", _config(), session_id="s-config",
        )
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        claimed = await storage.claim_session_lease(
            "u", "a", session.id, "worker-a", timedelta(seconds=10), now,
        )
        self.assertIsNotNone(claimed)
        state = AgentState()
        state.summary = "checkpointed"
        self.assertTrue(
            await storage.update_session_state_fenced(
                "u", "a", session.id, state,
                "worker-a", claimed.lease_epoch, now=now,
            ),
        )

        updated = await storage.update_session_config(
            "u", "a", session.id,
            _config().model_copy(update={"name": "renamed"}),
        )
        self.assertEqual(updated.config.name, "renamed")
        self.assertEqual(updated.state.summary, "checkpointed")
        self.assertEqual(updated.lease_owner, "worker-a")
        self.assertEqual(updated.lease_epoch, claimed.lease_epoch)
        self.assertEqual(updated.state_version, 1)

        # The public upsert entry point uses the same atomic path for
        # config-only writes, preserving the lease/state contract for
        # existing callers that have not moved to the explicit method.
        updated = await storage.upsert_session(
            "u", "a",
            _config().model_copy(update={"name": "renamed-again"}),
            session_id=session.id,
        )
        self.assertEqual(updated.config.name, "renamed-again")
        self.assertEqual(updated.state.summary, "checkpointed")
        self.assertEqual(updated.lease_owner, "worker-a")
        self.assertEqual(updated.lease_epoch, claimed.lease_epoch)

        with self.assertRaises(KeyError):
            await storage.update_session_config(
                "u", "other-agent", session.id, _config(),
            )
        with self.assertRaises(KeyError):
            await storage.upsert_session(
                "u", "other-agent", _config(), session_id=session.id,
            )

    async def assert_checkpoint_persists_message_and_state(
        self,
        storage,
    ) -> None:
        """Reply messages and state advance together under the lease."""
        session = await storage.upsert_session(
            "u", "a", _config(), session_id="s-checkpoint",
        )
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        claimed = await storage.claim_session_lease(
            "u", "a", session.id, "worker-a", timedelta(seconds=10), now,
        )
        self.assertIsNotNone(claimed)
        state = AgentState()
        state.summary = "checkpointed"
        message = UserMsg(name="assistant", content="reply")

        self.assertTrue(
            await storage.persist_session_checkpoint(
                "u", "a", session.id, [message], state,
                "worker-a", claimed.lease_epoch, now,
            ),
        )
        current = await storage.get_session("u", "a", session.id)
        self.assertEqual(current.state.summary, "checkpointed")
        self.assertEqual(current.state_version, 1)
        stored = await storage.get_message("u", session.id, message.id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.get_text_content(), "reply")

    async def assert_checkpoint_rolls_back_on_message_failure(
        self,
        storage,
    ) -> None:
        """SQL transaction failures leave neither side of the checkpoint."""
        session = await storage.upsert_session(
            "u", "a", _config(), session_id="s-checkpoint-failure",
        )
        initial = await storage.get_session("u", "a", session.id)
        state = AgentState()
        state.summary = "should-not-land"
        message = UserMsg(name="assistant", content="reply")

        original = storage._upsert_stmt

        def fail_message_upsert(row_cls, *args, **kwargs):
            if row_cls.__name__ == "MessageRow":
                raise RuntimeError("injected message failure")
            return original(row_cls, *args, **kwargs)

        storage._upsert_stmt = fail_message_upsert
        with self.assertRaises(RuntimeError):
            await storage.persist_session_checkpoint(
                "u", "a", session.id, [message], state,
            )
        current = await storage.get_session("u", "a", session.id)
        self.assertEqual(current.state, initial.state)
        self.assertIsNone(await storage.get_message("u", session.id, message.id))


@skipUnless(_fakeredis is not None, "Redis storage dependencies unavailable")
class TestRedisSessionFencing(_SessionFencingAssertions, IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.storage = RedisStorage.__new__(RedisStorage)
        self.storage._client = _fakeredis.FakeRedis(decode_responses=True)
        self.storage.key_ttl = None
        self.storage.key_config = RedisStorage.KeyConfig()

    async def test_claim_renew_release_and_fence(self) -> None:
        await self.assert_fencing(self.storage)

    async def test_config_update_preserves_state_and_lease(self) -> None:
        await self.assert_config_update_preserves_state_and_lease(self.storage)

    async def test_checkpoint_persists_message_and_state(self) -> None:
        await self.assert_checkpoint_persists_message_and_state(self.storage)


@skipUnless(
    _aiosqlite is not None and _sqlalchemy is not None,
    "SQL storage dependencies unavailable",
)
class TestSQLSessionFencing(_SessionFencingAssertions, IsolatedAsyncioTestCase):
    async def test_claim_renew_release_and_fence(self) -> None:
        storage = AsyncSQLAlchemyStorage("sqlite+aiosqlite:///:memory:")
        async with storage:
            await self.assert_fencing(storage)

    async def test_config_update_preserves_state_and_lease(self) -> None:
        storage = AsyncSQLAlchemyStorage("sqlite+aiosqlite:///:memory:")
        async with storage:
            await self.assert_config_update_preserves_state_and_lease(storage)

    async def test_checkpoint_persists_message_and_state(self) -> None:
        storage = AsyncSQLAlchemyStorage("sqlite+aiosqlite:///:memory:")
        async with storage:
            await self.assert_checkpoint_persists_message_and_state(storage)

    async def test_checkpoint_rolls_back_on_message_failure(self) -> None:
        storage = AsyncSQLAlchemyStorage("sqlite+aiosqlite:///:memory:")
        async with storage:
            await self.assert_checkpoint_rolls_back_on_message_failure(storage)
