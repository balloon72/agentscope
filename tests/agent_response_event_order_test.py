# -*- coding: utf-8 -*-
"""Regression tests for ordered events from complete model responses."""

from unittest import IsolatedAsyncioTestCase

from agentscope.agent import Agent
from agentscope.event import (
    TextBlockDeltaEvent,
    ToolCallStartEvent,
)
from agentscope.message import AssistantMsg, TextBlock, ToolCallBlock
from agentscope.model import ChatResponse
from agentscope.tool import Toolkit
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

