"""Integration tests for the parent-agent subagent bridge."""

import asyncio
from typing import Any, ClassVar
from unittest.async_case import IsolatedAsyncioTestCase

from utils import MockModel

from agentscope.agent import Agent, InjectionConfig
from agentscope.event import (
    ConfirmResult,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.message import TextBlock, ToolCallBlock, UserMsg
from agentscope.model import ChatResponse
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import (
    SubagentTool,
    ToolBase,
    ToolChunk,
    Toolkit,
)


def _text(text: str) -> list[ChatResponse]:
    return [
        ChatResponse(content=[TextBlock(text=text)], is_last=False, usage=None),
        ChatResponse(content=[TextBlock(text=text)], is_last=True, usage=None),
    ]


def _tool_call(call_id: str, name: str, value: str) -> list[ChatResponse]:
    block = ToolCallBlock(id=call_id, name=name, input=value)
    return [
        ChatResponse(content=[block], is_last=False, usage=None),
        ChatResponse(content=[block], is_last=True, usage=None),
    ]


class _NeedsConfirmation(ToolBase):
    """A deterministic child tool that pauses for confirmation."""

    name = "needs_confirmation"
    description = "Ask for confirmation"
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"value": {"type": "string"}},
        "required": ["value"],
    }
    is_concurrency_safe = True
    is_read_only = False

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(
            behavior=PermissionBehavior.ASK,
            message="test confirmation",
        )

    async def call(self, value: str) -> ToolChunk:
        return ToolChunk(content=[TextBlock(text=f"accepted: {value}")])


class SubagentToolTest(IsolatedAsyncioTestCase):
    """Drive a real Agent parent and Agent child through HITL."""

    async def test_parent_round_trip_and_hitl_routing(self) -> None:
        child_model = MockModel()
        child_model.set_responses(
            [
                _tool_call(
                    "child-call",
                    "needs_confirmation",
                    '{"value":"secret"}',
                ),
                _text("child complete"),
            ],
        )

        def child_factory() -> Agent:
            return Agent(
                name="child",
                system_prompt="You are the child.",
                model=child_model,
                toolkit=Toolkit(tools=[_NeedsConfirmation()]),
                injection_config=InjectionConfig(inject_runtime_state=False),
            )

        bridge = SubagentTool(child_factory)
        parent_model = MockModel()
        parent_model.set_responses(
            [
                _tool_call("parent-call", "call_subagent", '{"task":"do it"}'),
                _text("parent summary"),
            ],
        )
        parent = Agent(
            name="parent",
            system_prompt="You are the parent.",
            model=parent_model,
            toolkit=Toolkit(tools=[bridge]),
            injection_config=InjectionConfig(inject_runtime_state=False),
        )

        parent_reply = asyncio.create_task(
            parent.reply(UserMsg(name="user", content="delegate")),
        )
        saw_hitl = False
        async for wrapped in bridge.events():
            if isinstance(wrapped.event, RequireUserConfirmEvent):
                saw_hitl = True
                self.assertIsNotNone(wrapped.reply_id)
                # A wrong reply ID can never resume a sibling/nested run.
                with self.assertRaises(ValueError):
                    await bridge.resume(
                        wrapped.run_id,
                        UserConfirmResultEvent(
                            reply_id="wrong-reply",
                            confirm_results=[],
                        ),
                    )
                await bridge.resume(
                    wrapped.run_id,
                    UserConfirmResultEvent(
                        reply_id=wrapped.reply_id,
                        confirm_results=[
                            ConfirmResult(
                                confirmed=True,
                                tool_call=wrapped.event.tool_calls[0],
                            ),
                        ],
                    ),
                )

        result = await parent_reply
        self.assertTrue(saw_hitl)
        self.assertEqual(result.content[0].text, "parent summary")

    async def test_cancel_closes_child(self) -> None:
        child_model = MockModel()
        # A model call that never completes gives the bridge a live run.
        child_model.set_responses([asyncio.CancelledError()])

        def factory() -> Agent:
            return Agent(
                name="child",
                system_prompt="child",
                model=child_model,
                injection_config=InjectionConfig(inject_runtime_state=False),
            )

        bridge = SubagentTool(factory)
        task = asyncio.create_task(bridge.run("cancel me"))
        await asyncio.sleep(0)
        run_id = next(iter(bridge._runs))
        result = await bridge.cancel(run_id)
        self.assertEqual(result["status"], "cancelled")
        self.assertTrue(task.done())
