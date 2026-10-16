# -*- coding: utf-8 -*-
"""Session status ownership and distributed liveness tests."""
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

from agentscope.app._service import SessionService, SessionStatus
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys


class _Storage:
    """Minimal storage double with one owner-scoped session."""

    def __init__(self) -> None:
        self.session = SimpleNamespace(
            state=SimpleNamespace(context=[]),
        )

    async def get_session(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
    ) -> SimpleNamespace | None:
        if (user_id, agent_id, session_id) != ("owner", "agent", "sid"):
            return None
        return self.session


class TestSessionStatusOwnership(IsolatedAsyncioTestCase):
    """A session run lock must not bypass storage ownership checks."""

    async def test_running_status_rejects_wrong_user_and_agent(self) -> None:
        storage = _Storage()
        async with InMemoryMessageBus() as bus:
            service = SessionService(storage=storage, message_bus=bus)
            async with bus.acquire_lock(
                MessageBusKeys.session_lock("sid"),
                ttl_secs=5,
            ):
                self.assertIsNone(
                    await service.get_session_status(
                        "intruder",
                        "agent",
                        "sid",
                    ),
                )
                self.assertIsNone(
                    await service.get_session_status(
                        "owner",
                        "other-agent",
                        "sid",
                    ),
                )
                self.assertEqual(
                    await service.get_session_status(
                        "owner",
                        "agent",
                        "sid",
                    ),
                    SessionStatus.RUNNING,
                )

