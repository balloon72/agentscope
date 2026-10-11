"""Tests for the app ``extra_agent_tools`` adapter."""

import asyncio
from collections.abc import AsyncIterator
from unittest.async_case import IsolatedAsyncioTestCase

from agentscope.event import ReplyEndEvent
from agentscope.message import Msg, TextBlock
from agentscope.tool import SubagentTool, create_subagent_tool_factory
from agentscope.types import ReplyFinishedReason


class _Worker:
    def __init__(self, context: object) -> None:
        self.context = context

    async def reply_stream(self, inputs: object) -> AsyncIterator[object]:
        yield Msg(name="worker", role="assistant", content=[TextBlock(text="ok")])
        yield ReplyEndEvent(
            session_id="worker-session",
            reply_id="worker-reply",
            finished_reason=ReplyFinishedReason.COMPLETED,
        )


class SubagentToolFactoryTest(IsolatedAsyncioTestCase):
    async def test_creates_one_resolver_backed_tool_per_session(self) -> None:
        resolver = lambda agent_id: agent_id
        factory = create_subagent_tool_factory(
            resolver,
            allowed_agent_ids={"specialist"},
        )

        first = await factory("user-1", "agent-1", "session-1")
        second = await factory("user-1", "agent-1", "session-2")

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertIsInstance(first[0], SubagentTool)
        self.assertIsInstance(second[0], SubagentTool)
        self.assertIsNot(first[0], second[0])
        self.assertIs(first[0]._agent_resolver, resolver)
        self.assertIs(second[0]._agent_resolver, resolver)

    def test_rejects_non_callable_resolver(self) -> None:
        with self.assertRaises(TypeError):
            create_subagent_tool_factory(None)  # type: ignore[arg-type]

    async def test_context_factory_scopes_parent_and_run_context_per_session(self) -> None:
        seen: list[tuple[str, dict[str, object]]] = []

        async def resolver(
            agent_id: str,
            context: dict[str, object] | None = None,
        ) -> _Worker:
            assert context is not None
            # Let the sibling run interleave while retaining the context seen
            # by this resolver invocation.
            snapshot = dict(context)
            await asyncio.sleep(0)
            seen.append((agent_id, snapshot))
            return _Worker(snapshot)

        async def context_factory(
            user_id: str,
            parent_agent_id: str,
            session_id: str,
        ) -> dict[str, object]:
            await asyncio.sleep(0)
            return {
                "user_id": user_id,
                "parent_agent_id": parent_agent_id,
                "session_id": session_id,
                "workspace_id": f"workspace-{session_id}",
            }

        factory = create_subagent_tool_factory(
            resolver,
            context_factory=context_factory,
            allowed_agent_ids={"specialist"},
            cleanup_ttl=None,
        )
        first_tools, second_tools = await asyncio.gather(
            factory("user-1", "parent-1", "session-1"),
            factory("user-2", "parent-2", "session-2"),
        )
        first_tool = first_tools[0]
        second_tool = second_tools[0]
        assert isinstance(first_tool, SubagentTool)
        assert isinstance(second_tool, SubagentTool)
        assert first_tool._agent_resolver is not second_tool._agent_resolver

        first_result, second_result = await asyncio.gather(
            first_tool.run(
                "first",
                agent_id="specialist",
                context={"run_id": "run-1"},
            ),
            second_tool.run(
                "second",
                agent_id="specialist",
                context={"run_id": "run-2"},
            ),
        )

        assert first_result["status"] == "completed"
        assert second_result["status"] == "completed"
        by_run = {entry[1]["run_id"]: entry[1] for entry in seen}
        assert by_run == {
            "run-1": {
                "user_id": "user-1",
                "parent_agent_id": "parent-1",
                "session_id": "session-1",
                "workspace_id": "workspace-session-1",
                "run_id": "run-1",
                "agent_id": "specialist",
            },
            "run-2": {
                "user_id": "user-2",
                "parent_agent_id": "parent-2",
                "session_id": "session-2",
                "workspace_id": "workspace-session-2",
                "run_id": "run-2",
                "agent_id": "specialist",
            },
        }

    async def test_context_factory_supports_sync_context_and_legacy_resolver(self) -> None:
        seen: list[tuple[str, object]] = []

        def resolver(agent_id: str) -> _Worker:
            seen.append((agent_id, "legacy"))
            return _Worker("legacy")

        factory = create_subagent_tool_factory(
            resolver,
            context_factory=lambda user_id, parent_agent_id, session_id: {
                "user_id": user_id,
                "parent_agent_id": parent_agent_id,
                "session_id": session_id,
            },
            allowed_agent_ids={"specialist"},
            cleanup_ttl=None,
        )
        tools = await factory("user", "parent", "session")
        result = await tools[0].run("legacy", agent_id="specialist")
        assert result["status"] == "completed"
        assert seen == [("specialist", "legacy")]
