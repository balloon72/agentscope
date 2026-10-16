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


@skipUnless(_fakeredis is not None, "Redis storage dependencies unavailable")
class TestRedisSessionFencing(_SessionFencingAssertions, IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.storage = RedisStorage.__new__(RedisStorage)
        self.storage._client = _fakeredis.FakeRedis(decode_responses=True)
        self.storage.key_ttl = None
        self.storage.key_config = RedisStorage.KeyConfig()

    async def test_claim_renew_release_and_fence(self) -> None:
        await self.assert_fencing(self.storage)


@skipUnless(
    _aiosqlite is not None and _sqlalchemy is not None,
    "SQL storage dependencies unavailable",
)
class TestSQLSessionFencing(_SessionFencingAssertions, IsolatedAsyncioTestCase):
    async def test_claim_renew_release_and_fence(self) -> None:
        storage = AsyncSQLAlchemyStorage("sqlite+aiosqlite:///:memory:")
        async with storage:
            await self.assert_fencing(storage)
