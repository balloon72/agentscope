# -*- coding: utf-8 -*-
# pylint: disable=protected-access, using-constant-test
"""Regression coverage for the ChatService durable lease heartbeat."""

import asyncio
from types import SimpleNamespace
from typing import AsyncGenerator
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from agentscope.agent import ContextConfig, ReActConfig
from agentscope.app._service import ChatService
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.app.storage import (
    AgentData,
    AgentRecord,
    ChatModelConfig,
    SessionConfig,
    SessionRecord,
)


class _Storage:
    """Provide a lease that fails once while the agent is still running."""

    def __init__(self, session: SessionRecord) -> None:
        self.session = session
        self.renew_calls = 0
        self.renew_started = asyncio.Event()
        self.release_calls = 0

    async def claim_session_lease(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        lease_owner: str,
        lease_ttl: object,
    ) -> SessionRecord:
        """Return a claimed session with a fencing epoch."""
        del user_id, agent_id, session_id, lease_owner, lease_ttl
        claimed = self.session.model_copy(deep=True)
        claimed.lease_epoch = 1
        return claimed

    async def renew_session_lease(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        lease_owner: str,
        lease_epoch: int,
        lease_ttl: object,
    ) -> bool:
        """Raise on the first renewal, then accept cleanup's fence check."""
        del user_id, agent_id, session_id, lease_owner, lease_epoch, lease_ttl
        self.renew_calls += 1
        self.renew_started.set()
        if self.renew_calls == 1:
            raise ConnectionError("database is unavailable")
        return True

    async def release_session_lease(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        lease_owner: str,
        lease_epoch: int,
    ) -> bool:
        """Record that teardown releases the durable lease."""
        del user_id, agent_id, session_id, lease_owner, lease_epoch
        self.release_calls += 1
        return True

    async def update_session_state(self, *args: object, **kwargs: object) -> None:
        """Accept the shielded cancellation checkpoint."""
        del args, kwargs

    async def upsert_message(self, *args: object, **kwargs: object) -> None:
        """Accept any synthesized reply from the run."""
        del args, kwargs


class _Access:
    """Resolve the test agent."""

    def __init__(self, agent: AgentRecord) -> None:
        self.agent = agent

    async def resolve_agent(
        self,
        user_id: str,
        agent_id: str,
    ) -> AgentRecord:
        """Return the visible agent record."""
        assert user_id == self.agent.user_id
        assert agent_id == self.agent.id
        return self.agent.model_copy(deep=True)


class _WorkspaceManager:
    """Return a minimal workspace handle."""

    async def get_workspace(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        workspace_id: str,
    ) -> object:
        """Return the configured workspace."""
        del user_id, agent_id, session_id, workspace_id
        return SimpleNamespace(workdir="/tmp/chat-lease-heartbeat-test")


class TestChatServiceLeaseHeartbeat(IsolatedAsyncioTestCase):
    """A heartbeat outage must stop the run rather than run past its lease."""

    async def test_renew_connection_error_cancels_run(self) -> None:
        """Renew errors cancel the parent and still release the lease."""
        user_id = "user-1"
        agent = AgentRecord(
            id="agent-1",
            user_id=user_id,
            data=AgentData(
                name="agent",
                context_config=ContextConfig(),
                react_config=ReActConfig(),
            ),
        )
        session = SessionRecord(
            id="session-1",
            user_id=user_id,
            agent_id=agent.id,
            config=SessionConfig(
                workspace_id="workspace-1",
                chat_model_config=ChatModelConfig(
                    type="test",
                    credential_id="credential-1",
                    model="test-model",
                    parameters={},
                ),
            ),
        )
        storage = _Storage(session)
        agent_cancelled = asyncio.Event()

        class _Agent:
            """Block in the reply until the lease heartbeat cancels us."""

            def __init__(self, **kwargs: object) -> None:
                self.name = kwargs["name"]
                self.state = kwargs["state"]

            async def reply_stream(
                self,
                inputs: object,
            ) -> AsyncGenerator[object, None]:
                """Remain active so the heartbeat has to renew the lease."""
                del inputs
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    agent_cancelled.set()
                    raise
                if False:
                    yield object()

        async def _get_toolkit(**_: object) -> object:
            """Return an inert toolkit handle."""
            return object()

        async def _get_model(*_: object, **__: object) -> object:
            """Return an inert model handle."""
            return object()

        service = ChatService(
            storage=storage,
            workspace_manager=_WorkspaceManager(),
            scheduler_manager=object(),
            background_task_manager=object(),
            message_bus=InMemoryMessageBus(),
            resource_access_service=_Access(agent),
            custom_agent_cls=_Agent,
        )
        with (
            patch.object(MessageBusKeys, "SESSION_RUN_TTL_SECS", 3),
            patch(
                "agentscope.app._service._chat.get_toolkit",
                new=_get_toolkit,
            ),
            patch("agentscope.app._service._chat.get_model", new=_get_model),
        ):
            run = asyncio.create_task(
                service._run_impl(user_id, session.id, agent.id, None),
            )
            await asyncio.wait_for(storage.renew_started.wait(), 2.0)
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(run, 2.0)

        self.assertTrue(agent_cancelled.is_set())
        self.assertEqual(storage.renew_calls, 2)
        self.assertEqual(storage.release_calls, 1)
