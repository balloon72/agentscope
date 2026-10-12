# -*- coding: utf-8 -*-
"""Regression tests for ordered events from complete model responses."""

from unittest import IsolatedAsyncioTestCase

from agentscope.agent import Agent, InjectionConfig
from agentscope.event import (
    TextBlockDeltaEvent,
    ToolCallStartEvent,
)
from agentscope.message import AssistantMsg, TextBlock, ToolCallBlock, UserMsg
from agentscope.model import ChatResponse
from agentscope.tool import Toolkit, FunctionTool
from agentscope.permission import PermissionBehavior, PermissionDecision
from utils import MockModel


class AgentResponseEventOrderTest(IsolatedAsyncioTestCase):
    async def test_non_streaming_blocks_keep_order_and_boundaries(self) -> None:
        agent = Agent(
            name="assistant",
            system_prompt="You are helpful.",
            model=MockModel(stream=False),
            toolkit=Toolkit(),
        )
        agent.state.reply_id = "reply"
        chunk = ChatResponse(
            content=[
                TextBlock(text="Let me check."),
                ToolCallBlock(id="call", name="lookup", input='{"q":"x"}'),
                TextBlock(text="Done checking."),
            ],
            is_last=True,
        )

        events = [
            event
            async for event in agent._convert_chat_response_to_event({}, chunk)
        ]
        rebuilt = AssistantMsg(id="reply", name="assistant", content=[])
        for event in events:
            rebuilt.append_event(event)

        self.assertIsInstance(events[1], TextBlockDeltaEvent)
        self.assertIsInstance(events[3], ToolCallStartEvent)
        self.assertEqual(
            [
                (type(block).__name__, getattr(block, "text", None))
                for block in rebuilt.content
                if isinstance(block, TextBlock)
            ],
            [
                ("TextBlock", "Let me check."),
                ("TextBlock", "Done checking."),
            ],
        )
        self.assertIsInstance(rebuilt.content[1], ToolCallBlock)

    async def test_reply_stream_preserves_adjacent_text_blocks(self) -> None:
        """Exercise the model wrapper and reasoning loop, not just conversion."""
        model = MockModel()
        model.set_responses(
            [
                ChatResponse(
                    content=[TextBlock(text="First."), TextBlock(text="Next.")],
                    is_last=True,
                ),
            ],
        )
        agent = self._make_agent(model)
        events, rebuilt = await self._reply_and_rebuild(agent)

        self.assertEqual(model.cnt, 1)
        self.assertEqual(self._content_values(rebuilt), ["First.", "Next."])
        self.assertEqual(
            self._content_values(rebuilt),
            self._content_values(agent.state.context[-1]),
        )
        self.assertEqual(
            [event.type for event in events][2:-2],
            ["TEXT_BLOCK_START", "TEXT_BLOCK_DELTA", "TEXT_BLOCK_END"] * 2,
        )
        starts = [e.block_id for e in events if e.type == "TEXT_BLOCK_START"]
        ends = [e.block_id for e in events if e.type == "TEXT_BLOCK_END"]
        self.assertEqual(len(set(starts)), 2)
        self.assertEqual(starts, ends)

    async def test_reply_stream_preserves_text_tool_text(self) -> None:
        """An actual tool round trip preserves order in events and context."""
        model = MockModel()
        model.set_responses(
            [
                ChatResponse(
                    content=[
                        TextBlock(text="Before."),
                        ToolCallBlock(
                            id="call", name="lookup", input='{"q":"x"}',
                        ),
                        TextBlock(text="After."),
                    ],
                    is_last=True,
                ),
                ChatResponse(content=[TextBlock(text="Done.")], is_last=True),
            ],
        )
        calls = []

        def lookup(q: str) -> str:
            """Return the requested value locally."""
            calls.append(q)
            return q

        agent = self._make_agent(model)
        agent.toolkit = Toolkit(
            tools=[
                FunctionTool(
                    lookup,
                    permission=PermissionDecision(
                        behavior=PermissionBehavior.ALLOW,
                        message="Local test tool is allowed.",
                    ),
                ),
            ],
        )
        events, rebuilt = await self._reply_and_rebuild(agent)

        self.assertEqual(calls, ["x"])
        self.assertEqual(model.cnt, 2)
        self.assertEqual(
            self._content_values(rebuilt),
            ["Before.", ("call", "lookup", '{"q":"x"}'), "After.", "Done."],
        )
        self.assertEqual(
            self._content_values(rebuilt),
            self._content_values(agent.state.context[-1]),
        )
        first_end = next(
            i
            for i, event in enumerate(events)
            if event.type == "MODEL_CALL_END"
        )
        self.assertEqual(
            [event.type for event in events][2:first_end],
            [
                "TEXT_BLOCK_START",
                "TEXT_BLOCK_DELTA",
                "TEXT_BLOCK_END",
                "TOOL_CALL_START",
                "TOOL_CALL_DELTA",
                "TOOL_CALL_END",
                "TEXT_BLOCK_START",
                "TEXT_BLOCK_DELTA",
                "TEXT_BLOCK_END",
            ],
        )

    async def test_reply_stream_still_coalesces_deltas(self) -> None:
        """Multi-block deltas coalesce; the final snapshot is not replayed."""
        model = MockModel()
        model.set_responses(
            [
                [
                    ChatResponse(
                        content=[TextBlock(text="Hel"), TextBlock(text="lo")],
                        is_last=False,
                    ),
                    ChatResponse(content=[TextBlock(text="!")], is_last=False),
                    ChatResponse(
                        content=[TextBlock(text="Hello"), TextBlock(text="!")],
                        is_last=True,
                    ),
                ],
            ],
        )
        events, rebuilt = await self._reply_and_rebuild(self._make_agent(model))

        self.assertEqual(model.cnt, 1)
        self.assertEqual(self._content_values(rebuilt), ["Hello!"])
        self.assertEqual(
            [event.type for event in events][2:-2],
            [
                "TEXT_BLOCK_START",
                "TEXT_BLOCK_DELTA",
                "TEXT_BLOCK_DELTA",
                "TEXT_BLOCK_END",
            ],
        )
        self.assertEqual(
            [e.delta for e in events if e.type == "TEXT_BLOCK_DELTA"],
            ["Hello", "!"],
        )
        self.assertEqual(
            len({e.block_id for e in events if hasattr(e, "block_id")}),
            1,
        )

    @staticmethod
    def _make_agent(model: MockModel) -> Agent:
        return Agent(
            name="assistant",
            system_prompt="You are helpful.",
            model=model,
            toolkit=Toolkit(),
            injection_config=InjectionConfig(inject_runtime_state=False),
        )

    @staticmethod
    async def _reply_and_rebuild(agent: Agent) -> tuple[list, AssistantMsg]:
        events = [
            event
            async for event in agent.reply_stream(
                UserMsg(name="user", content="Go."),
            )
        ]
        rebuilt = AssistantMsg(
            id=agent.state.reply_id,
            name="assistant",
            content=[],
        )
        for event in events:
            rebuilt.append_event(event)
        return events, rebuilt

    @staticmethod
    def _content_values(message: AssistantMsg) -> list:
        return [
            block.text
            if isinstance(block, TextBlock)
            else (block.id, block.name, block.input)
            for block in message.content
            if isinstance(block, (TextBlock, ToolCallBlock))
        ]
