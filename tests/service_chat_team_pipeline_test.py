# -*- coding: utf-8 -*-
"""Regression tests for ChatService's durable static TeamPipeline glue."""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from typing import AsyncGenerator

from fastapi import HTTPException

from agentscope.agent import Agent, ContextConfig, ReActConfig
from agentscope.app._service import ChatService
from agentscope.app.message_bus import InMemoryMessageBus
from agentscope.app.storage import (
    AgentData,
    AgentRecord,
    ChatModelConfig,
    SessionConfig,
    SessionRecord,
    TeamPipelineConfig,
)
from agentscope.event import (
    ReplyEndEvent,
    TextBlockDeltaEvent,
    TextBlockStartEvent,
    UserConfirmResultEvent,
)
from agentscope.message import Msg
from agentscope.pipeline import TeamMember, TeamPipeline
from agentscope.state import AgentState
from agentscope.types import ReplyFinishedReason
from agentscope.permission import (
    AdditionalWorkingDirectory,
    PermissionContext,
    PermissionMode,
)


class _Agent(Agent):
    """Small stand-in for Agent, retaining constructor state for assertions."""

    def __init__(self, *, name: str, state: AgentState | None = None, **_: object):
        self.name = name
        self.state = state or AgentState()


class _Workspace:
    async def get_workspace(self, *_: object, **__: object) -> object:
        return SimpleNamespace(workdir="/member/workdir")


class _Storage:
    def __init__(self, sessions: dict[str, list[SessionRecord]]) -> None:
        self.sessions = sessions

    async def list_sessions(self, user_id: str, agent_id: str) -> list[SessionRecord]:
        del user_id
        return [
            item.model_copy(deep=True)
            for item in self.sessions.get(agent_id, [])
        ]


class _Access:
    def __init__(self, records: dict[str, AgentRecord]) -> None:
        self.records = records

    async def resolve_agent(self, user_id: str, agent_id: str) -> AgentRecord:
        del user_id
        return self.records[agent_id].model_copy(deep=True)


class _RunStorage:
    """Small storage double for service-level continuation persistence."""

    def __init__(
        self,
        session: SessionRecord,
        messages: dict[str, Msg] | None = None,
    ) -> None:
        self.session = session
        self.messages = {
            key: value.model_copy(deep=True)
            for key, value in (messages or {}).items()
        }
        self.upserts: list[Msg] = []
        self.persisted_states: list[AgentState] = []

    async def get_session(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
    ) -> SessionRecord | None:
        if (
            user_id != self.session.user_id
            or agent_id != self.session.agent_id
            or session_id != self.session.id
        ):
            return None
        return self.session.model_copy(deep=True)

    async def update_session_state(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        state: AgentState,
    ) -> None:
        del user_id, agent_id, session_id
        self.persisted_states.append(state.model_copy(deep=True))
        self.session.state = state.model_copy(deep=True)

    async def get_message(
        self,
        user_id: str,
        session_id: str,
        message_id: str,
    ) -> Msg | None:
        del user_id, session_id
        msg = self.messages.get(message_id)
        return msg.model_copy(deep=True) if msg is not None else None

    async def upsert_message(
        self,
        user_id: str,
        session_id: str,
        msg: Msg,
    ) -> None:
        del user_id, session_id
        copy = msg.model_copy(deep=True)
        self.messages[msg.id] = copy
        self.upserts.append(copy)


class _RunAgent(Agent):
    """Agent-shaped runtime that leaves all reply work to a fake pipeline."""

    def __init__(self, *, name: str, state: AgentState, **_: object) -> None:
        self.name = name
        self.state = state


class _NoopPipeline:
    """Pipeline which emits no events, useful for persistence-order tests."""

    members: dict = {}

    async def reply_stream(self, inputs: object) -> AsyncGenerator[object, None]:
        del inputs
        if False:
            yield object()


class _ContinuationPipeline:
    """Emit events for both member and leader reply ids in one continuation."""

    members: dict = {}

    async def reply_stream(self, inputs: object) -> AsyncGenerator[object, None]:
        del inputs
        yield TextBlockStartEvent(reply_id="member-reply", block_id="member-block")
        yield TextBlockDeltaEvent(
            reply_id="member-reply",
            block_id="member-block",
            delta="member resumed",
        )
        yield TextBlockStartEvent(reply_id="leader-reply", block_id="leader-block")
        yield TextBlockDeltaEvent(
            reply_id="leader-reply",
            block_id="leader-block",
            delta="leader resumed",
        )
        yield ReplyEndEvent(
            session_id="leader-session",
            reply_id="member-reply",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )
        yield ReplyEndEvent(
            session_id="leader-session",
            reply_id="leader-reply",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )


def _agent(agent_id: str, name: str, *, pipeline: TeamPipelineConfig | None = None):
    return AgentRecord(
        id=agent_id,
        user_id="u",
        data=AgentData(
            name=name,
            context_config=ContextConfig(),
            react_config=ReActConfig(),
            team_pipeline_config=pipeline or TeamPipelineConfig(),
        ),
    )


def _session(session_id: str, agent_id: str, *, state: AgentState | None = None):
    return SessionRecord(
        id=session_id,
        user_id="u",
        agent_id=agent_id,
        config=SessionConfig(
            workspace_id=f"ws-{agent_id}",
            chat_model_config=ChatModelConfig(
                type="test",
                credential_id="credential",
                model="model",
                parameters={},
            ),
        ),
        state=state or AgentState(),
    )


class TestChatServiceTeamPipeline(IsolatedAsyncioTestCase):
    def _service(self, records, sessions):
        return ChatService(
            storage=_Storage(sessions),
            workspace_manager=_Workspace(),
            scheduler_manager=object(),
            background_task_manager=object(),
            message_bus=InMemoryMessageBus(),
            resource_access_service=_Access(records),
            custom_agent_cls=_Agent,
        )

    async def test_member_uses_own_permission_context_without_history(self) -> None:
        leader = _agent(
            "leader",
            "Leader",
            pipeline=TeamPipelineConfig(enabled=True, member_agent_ids=["m"]),
        )
        member = _agent("m", "Member")
        member_state = AgentState(
            context=[Msg(name="old", role="user", content=[])],
            permission_context=PermissionContext(mode=PermissionMode.ACCEPT_EDITS),
        )
        leader_session = _session("leader-session", "leader")
        member_session = _session("member-session", "m", state=member_state)
        service = self._service(
            {"leader": leader, "m": member},
            {"m": [member_session]},
        )
        leader_runtime = _Agent(name="Leader", state=leader_session.state)
        with (
            patch("agentscope.app._service._chat.get_model", new=lambda *a, **k: object()),
            patch("agentscope.app._service._chat.get_toolkit", new=lambda **k: object()),
        ):
            # get_model/get_toolkit are async in production.
            async def _model(*_: object, **__: object) -> object:
                return object()

            async def _toolkit(**_: object) -> object:
                return object()

            with (
                patch("agentscope.app._service._chat.get_model", new=_model),
                patch("agentscope.app._service._chat.get_toolkit", new=_toolkit),
            ):
                pipeline = await service._build_static_team_pipeline(
                    "u",
                    "leader-session",
                    leader,
                    leader_session,
                    leader_runtime,
                )
        assert pipeline is not None
        runtime = pipeline.members["Member"].agent
        self.assertEqual(runtime.state.permission_context.mode, PermissionMode.ACCEPT_EDITS)
        self.assertIn("/member/workdir", runtime.state.permission_context.working_directories)
        self.assertEqual(runtime.state.context, [])

    async def test_duplicate_member_names_rejected_by_service_helper(self) -> None:
        leader = _agent(
            "leader",
            "Leader",
            pipeline=TeamPipelineConfig(enabled=True, member_agent_ids=["a", "b"]),
        )
        records = {"leader": leader, "a": _agent("a", "Same"), "b": _agent("b", "Same")}
        service = self._service(
            records,
            {"a": [_session("a-session", "a")], "b": [_session("b-session", "b")]},
        )
        async def _model(*_: object, **__: object) -> object:
            return object()

        async def _toolkit(**_: object) -> object:
            return object()

        with (
            patch("agentscope.app._service._chat.get_model", new=_model),
            patch("agentscope.app._service._chat.get_toolkit", new=_toolkit),
        ):
            with self.assertRaises(HTTPException) as raised:
                await service._build_static_team_pipeline(
                    "u",
                    "leader-session",
                    leader,
                    _session("leader-session", "leader"),
                    _Agent(name="Leader"),
                )
        self.assertEqual(raised.exception.status_code, 422)

    async def test_disabled_or_empty_pipeline_is_a_noop(self) -> None:
        """The default empty config does not assemble a TeamPipeline."""
        leader = _agent("leader", "Leader")
        service = self._service({"leader": leader}, {})

        runtime = _Agent(name="Leader")
        pipeline = await service._build_static_team_pipeline(
            "u",
            "leader-session",
            leader,
            _session("leader-session", "leader"),
            runtime,
        )

        self.assertIsNone(pipeline)

    async def test_self_reference_rejected_before_member_lookup(self) -> None:
        """A leader cannot delegate to its own agent record."""
        leader = _agent(
            "leader",
            "Leader",
            pipeline=TeamPipelineConfig(
                enabled=True,
                member_agent_ids=["leader"],
            ),
        )

        class _FailIfResolved(_Access):
            async def resolve_agent(
                self,
                user_id: str,
                agent_id: str,
            ) -> AgentRecord:
                raise AssertionError(
                    f"self-reference should be rejected before resolving {agent_id}",
                )

        service = ChatService(
            storage=_Storage({}),
            workspace_manager=_Workspace(),
            scheduler_manager=object(),
            background_task_manager=object(),
            message_bus=InMemoryMessageBus(),
            resource_access_service=_FailIfResolved({}),
            custom_agent_cls=_Agent,
        )
        with self.assertRaises(HTTPException) as raised:
            await service._build_static_team_pipeline(
                "u",
                "leader-session",
                leader,
                _session("leader-session", "leader"),
                _Agent(name="Leader"),
            )
        self.assertEqual(raised.exception.status_code, 422)
        self.assertIn("cannot include itself", raised.exception.detail)

    async def test_missing_member_is_reported_by_access_service(self) -> None:
        """A stale member id preserves the access layer's 404."""
        leader = _agent(
            "leader",
            "Leader",
            pipeline=TeamPipelineConfig(enabled=True, member_agent_ids=["missing"]),
        )

        class _MissingAccess(_Access):
            async def resolve_agent(
                self,
                user_id: str,
                agent_id: str,
            ) -> AgentRecord:
                raise HTTPException(
                    status_code=404,
                    detail=f"Agent {agent_id!r} not found.",
                )

        service = ChatService(
            storage=_Storage({}),
            workspace_manager=_Workspace(),
            scheduler_manager=object(),
            background_task_manager=object(),
            message_bus=InMemoryMessageBus(),
            resource_access_service=_MissingAccess({}),
            custom_agent_cls=_Agent,
        )
        with self.assertRaises(HTTPException) as raised:
            await service._build_static_team_pipeline(
                "u",
                "leader-session",
                leader,
                _session("leader-session", "leader"),
                _Agent(name="Leader"),
            )
        self.assertEqual(raised.exception.status_code, 404)

    async def test_cross_user_member_is_rejected(self) -> None:
        """Shared agents cannot become static members of an owner session."""
        leader = _agent(
            "leader",
            "Leader",
            pipeline=TeamPipelineConfig(enabled=True, member_agent_ids=["shared"]),
        )
        shared = _agent("shared", "Shared").model_copy(
            update={"user_id": "other-user"},
        )
        service = self._service(
            {"leader": leader, "shared": shared},
            {},
        )
        with self.assertRaises(HTTPException) as raised:
            await service._build_static_team_pipeline(
                "u",
                "leader-session",
                leader,
                _session("leader-session", "leader"),
                _Agent(name="Leader"),
            )
        self.assertEqual(raised.exception.status_code, 422)
        self.assertIn("must be owned", raised.exception.detail)

    async def test_member_without_owner_session_is_rejected(self) -> None:
        """A configured member needs at least one owner session."""
        leader = _agent(
            "leader",
            "Leader",
            pipeline=TeamPipelineConfig(enabled=True, member_agent_ids=["m"]),
        )
        member = _agent("m", "Member")
        service = self._service({"leader": leader, "m": member}, {})
        async def _model(*_: object, **__: object) -> object:
            return object()

        async def _toolkit(**_: object) -> object:
            return object()

        with (
            patch("agentscope.app._service._chat.get_model", new=_model),
            patch("agentscope.app._service._chat.get_toolkit", new=_toolkit),
        ):
            with self.assertRaises(HTTPException) as raised:
                await service._build_static_team_pipeline(
                    "u",
                    "leader-session",
                    leader,
                    _session("leader-session", "leader"),
                    _Agent(name="Leader"),
                )
        self.assertEqual(raised.exception.status_code, 422)
        self.assertIn("no owner session", raised.exception.detail)

    async def test_latest_member_session_is_selected_and_snapshotted(self) -> None:
        """First assembly chooses the newest member session deterministically."""
        leader = _agent(
            "leader",
            "Leader",
            pipeline=TeamPipelineConfig(enabled=True, member_agent_ids=["m"]),
        )
        member = _agent("m", "Member")
        older = _session("older", "m")
        newer = _session("newer", "m")
        older.updated_at = datetime.now() - timedelta(minutes=2)
        newer.updated_at = datetime.now()
        service = self._service(
            {"leader": leader, "m": member},
            {"m": [older, newer]},
        )
        async def _model(*_: object, **__: object) -> object:
            return object()

        async def _toolkit(**_: object) -> object:
            return object()

        with (
            patch("agentscope.app._service._chat.get_model", new=_model),
            patch("agentscope.app._service._chat.get_toolkit", new=_toolkit),
        ):
            leader_session = _session("leader-session", "leader")
            runtime = _Agent(name="Leader", state=leader_session.state)
            pipeline = await service._build_static_team_pipeline(
                "u",
                leader_session.id,
                leader,
                leader_session,
                runtime,
            )

        self.assertIsNotNone(pipeline)
        snapshot = runtime.state.middle_context["team_pipeline"]["members"]["m"]
        self.assertEqual(snapshot["session_id"], "newer")

    async def test_continuation_reply_by_id_persists_member_and_leader(self) -> None:
        """A member HITL result updates both reply records on the leader stream."""
        leader = _agent("leader", "Leader")
        state = AgentState()
        state.reply_id = "leader-reply"
        session = _session("leader-session", "leader", state=state)
        member_reply = Msg(
            id="member-reply",
            name="Member",
            role="assistant",
            content=[],
        )
        leader_reply = Msg(
            id="leader-reply",
            name="Leader",
            role="assistant",
            content=[],
        )
        storage = _RunStorage(
            session,
            {member_reply.id: member_reply, leader_reply.id: leader_reply},
        )
        service = ChatService(
            storage=storage,
            workspace_manager=_Workspace(),
            scheduler_manager=object(),
            background_task_manager=object(),
            message_bus=InMemoryMessageBus(),
            resource_access_service=_Access({"leader": leader}),
            custom_agent_cls=_RunAgent,
        )

        async def _model(*_: object, **__: object) -> object:
            return object()

        async def _toolkit(**_: object) -> object:
            return object()

        async def _build(*_: object, **__: object) -> _ContinuationPipeline:
            return _ContinuationPipeline()

        continuation = UserConfirmResultEvent(
            reply_id="member-reply",
            confirm_results=[],
        )
        with (
            patch("agentscope.app._service._chat.get_model", new=_model),
            patch("agentscope.app._service._chat.get_toolkit", new=_toolkit),
            patch.object(service, "_build_static_team_pipeline", new=_build),
        ):
            await service._run_impl(
                "u",
                session.id,
                leader.id,
                continuation,
            )

        persisted = {msg.id: msg for msg in storage.upserts}
        self.assertEqual(set(persisted), {"member-reply", "leader-reply"})
        self.assertIn("member resumed", persisted["member-reply"].get_text_content())
        self.assertIn("leader resumed", persisted["leader-reply"].get_text_content())

    async def test_team_snapshot_is_taken_before_session_state_persist(self) -> None:
        """Member runtime state must be folded into the state being written."""
        leader = _agent("leader", "Leader")
        session = _session("leader-session", "leader")
        storage = _RunStorage(session)
        service = ChatService(
            storage=storage,
            workspace_manager=_Workspace(),
            scheduler_manager=object(),
            background_task_manager=object(),
            message_bus=InMemoryMessageBus(),
            resource_access_service=_Access({"leader": leader}),
            custom_agent_cls=_RunAgent,
        )
        order: list[tuple[str, bool]] = []

        async def _update(*_: object, **kwargs: object) -> None:
            state = kwargs["state"]
            order.append(("update", "snapshot-marker" in state.middle_context))
            storage.persisted_states.append(state.model_copy(deep=True))

        async def _model(*_: object, **__: object) -> object:
            return object()

        async def _toolkit(**_: object) -> object:
            return object()

        async def _build(*_: object, **__: object) -> _NoopPipeline:
            return _NoopPipeline()

        def _snapshot(pipeline: object, runtime: _RunAgent) -> None:
            del pipeline
            order.append(("snapshot", False))
            runtime.state.middle_context["snapshot-marker"] = True

        storage.update_session_state = _update
        with (
            patch("agentscope.app._service._chat.get_model", new=_model),
            patch("agentscope.app._service._chat.get_toolkit", new=_toolkit),
            patch.object(service, "_build_static_team_pipeline", new=_build),
            patch.object(service, "_snapshot_static_team_pipeline", new=_snapshot),
        ):
            await service._run_impl("u", session.id, leader.id, None)

        self.assertEqual(order, [("snapshot", False), ("update", True)])

    def test_snapshot_matches_members_by_agent_id_after_reorder(self) -> None:
        leader = _Agent(name="Leader")
        leader.state.middle_context["team_pipeline"] = {
            "members": {
                "a": {"name": "A", "state": {}},
                "b": {"name": "B", "state": {}},
            },
        }
        first = _Agent(name="B", state=AgentState(summary="state-b"))
        second = _Agent(name="A", state=AgentState(summary="state-a"))
        pipeline = TeamPipeline(
            leader=leader,
            members=[
                TeamMember(agent=first, description=""),
                TeamMember(agent=second, description=""),
            ],
        )
        ChatService._snapshot_static_team_pipeline(pipeline, leader)
        snapshots = leader.state.middle_context["team_pipeline"]["members"]
        self.assertEqual(snapshots["a"]["state"]["summary"], "state-a")
        self.assertEqual(snapshots["b"]["state"]["summary"], "state-b")
