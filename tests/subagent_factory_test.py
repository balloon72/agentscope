"""Tests for the app ``extra_agent_tools`` adapter."""

from unittest.async_case import IsolatedAsyncioTestCase

from agentscope.tool import SubagentTool, create_subagent_tool_factory


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
