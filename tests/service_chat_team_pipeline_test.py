# -*- coding: utf-8 -*-
"""Regression tests for ChatService's durable static TeamPipeline glue."""

from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

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
from agentscope.message import Msg
from agentscope.pipeline import TeamMember, TeamPipeline
from agentscope.state import AgentState
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
        return [item.model_copy(deep=True) for item in self.sessions[agent_id]]


class _Access:
    def __init__(self, records: dict[str, AgentRecord]) -> None:
        self.records = records

    async def resolve_agent(self, user_id: str, agent_id: str) -> AgentRecord:
        del user_id
        return self.records[agent_id].model_copy(deep=True)


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

    def test_snapshot_matches_members_by_agent_id_after_reorder(self) -> None:
        leader = _Agent(name="Leader")
        leader.state.middle_context["team_pipeline"] = {
            "members": {
                "a": {"state": {}},
                "b": {"state": {}},
            },
        }
        first = _Agent(name="B", state=AgentState(summary="state-b"))
        second = _Agent(name="A", state=AgentState(summary="state-a"))
        pipeline = TeamPipeline(
            leader=leader,
            members=[
                TeamMember(agent=first, description="", agent_id="b"),
                TeamMember(agent=second, description="", agent_id="a"),
            ],
        )
        ChatService._snapshot_static_team_pipeline(pipeline, leader)
        snapshots = leader.state.middle_context["team_pipeline"]["members"]
        self.assertEqual(snapshots["a"]["state"]["summary"], "state-a")
        self.assertEqual(snapshots["b"]["state"]["summary"], "state-b")
