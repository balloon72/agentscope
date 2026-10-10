"""Real local-model parent -> subagent -> HITL verification.

This is an executable integration check, not a mocked model test.  It downloads
nothing at runtime: set ``QWEN_GGUF`` to the official Qwen3-0.6B-GGUF file
(or leave the default ``/tmp/qwen3-0.6b-q8_0.gguf``), then run this file with
``/tmp/agentscope-venv/bin/python``.

Download the official Apache-2.0 Qwen artifact once if needed:
``curl -L https://huggingface.co/Qwen/Qwen3-0.6B-GGUF/resolve/main/Qwen3-0.6B-Q8_0.gguf -o /tmp/qwen3-0.6b-q8_0.gguf``.
The Python llama-cpp server version used by this checkout emits Qwen3's native
``<tool_call>`` markup in the response content when tool_choice is auto.  It
does not parse that markup into OpenAI ``tool_calls`` (forced tool_choice does,
but that would no longer test the model's decision).  ``QwenMarkupChatModel``
is therefore a small protocol adapter: it parses only markup that the model
actually emitted.  It never inserts or forces a call.  The script records the
raw markup as evidence and uses AgentScope's real ToolCallBlock/HITL events.

The server and client are created in this same process because this execution
environment isolates localhost between shell invocations.  ``trust_env=False``
is required here: the inherited SOCKS proxy has no socksio dependency.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

# Permit running this example from a source checkout without an editable install.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

import httpx

from agentscope.agent import Agent
from agentscope.credential import OpenAICredential
from agentscope.event import (
    ConfirmResult,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.formatter import OpenAIChatFormatter
from agentscope.message import Msg, TextBlock, ThinkingBlock, ToolCallBlock
from agentscope.model import ChatResponse, OpenAIChatModel
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import FunctionTool, SubagentEvent, SubagentTool, Toolkit

_TOOL_RE = re.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", re.DOTALL)
_THINK_RE = re.compile(r"<think>\s*(.*?)\s*</think>", re.DOTALL)


class PlainTextOpenAIFormatter(OpenAIChatFormatter):
    """Flatten text-only content blocks for llama.cpp's Qwen template."""

    async def format(self, msgs: list[Msg]) -> list[dict[str, Any]]:
        formatted = await super().format(msgs)
        for message in formatted:
            content = message.get("content")
            if isinstance(content, list):
                text = "\n".join(
                    part.get("text", "")
                    for part in content
                    if isinstance(part, dict) and part.get("type") == "text"
                )
                message["content"] = text or None
            # llama.cpp's Pydantic request schema requires a string on
            # assistant messages that contain tool_calls (OpenAI normally
            # permits null).
            if message.get("tool_calls") and message.get("content") is None:
                message["content"] = ""
        return formatted


class QwenMarkupChatModel(OpenAIChatModel):
    """Parse only native Qwen ``<tool_call>`` text emitted by the model."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.raw_tool_markup: list[str] = []

    @staticmethod
    def _parse_response(response: ChatResponse, evidence: list[str]) -> ChatResponse:
        blocks = response.content
        rewritten = []
        for block in blocks:
            if not isinstance(block, TextBlock):
                rewritten.append(block)
                continue
            text = block.text
            calls = list(_TOOL_RE.finditer(text))
            if not calls:
                rewritten.append(block)
                continue
            for match in calls:
                raw = match.group(0)
                evidence.append(raw)
                payload = json.loads(match.group(1))
                rewritten.append(
                    ToolCallBlock(
                        id=f"qwen-{uuid.uuid4().hex}",
                        name=payload["name"],
                        input=json.dumps(payload.get("arguments", {})),
                    ),
                )
            # Keep the model's reasoning as reasoning, but don't pass markup
            # itself as assistant text to the agent loop.
            before = _TOOL_RE.sub("", text)
            think = _THINK_RE.search(before)
            if think:
                rewritten.insert(0, ThinkingBlock(thinking=think.group(1).strip()))
                before = _THINK_RE.sub("", before)
            if before.strip():
                rewritten.insert(0, TextBlock(text=before.strip()))
        response.content = rewritten
        return response

    async def _call_api(self, *args: Any, **kwargs: Any) -> ChatResponse | AsyncGenerator[ChatResponse, None]:
        # llama.cpp defaults to a random seed even at temperature 0. A fixed
        # seed makes this regression reproducible while leaving tool_choice
        # on the model's normal auto path.
        kwargs.setdefault("seed", 424242)
        result = await super()._call_api(*args, **kwargs)
        if inspect.isasyncgen(result):
            async def converted() -> AsyncGenerator[ChatResponse, None]:
                async for response in result:
                    yield self._parse_response(response, self.raw_tool_markup)
            return converted()
        return self._parse_response(result, self.raw_tool_markup)


def make_model(base_url: str) -> QwenMarkupChatModel:
    # No tool_choice is supplied by this script. AgentScope uses the API's
    # normal auto choice, so each call below is selected by the real model.
    return QwenMarkupChatModel(
        credential=OpenAICredential(api_key="local", base_url=base_url),
        model="qwen",
        formatter=PlainTextOpenAIFormatter(),
        stream=False,
        parameters=OpenAIChatModel.Parameters(
            max_tokens=256,
            temperature=0.0,
            parallel_tool_calls=False,
        ),
        client_kwargs={"http_client": httpx.AsyncClient(trust_env=False)},
    )


async def run_case(base_url: str, approve: bool) -> dict[str, Any]:
    calls: list[str] = []
    models: list[QwenMarkupChatModel] = []

    def risky_action(task: str) -> str:
        """Perform the risky worker action after the user approves it."""
        calls.append(task)
        return f"worker completed: {task}"

    def make_worker() -> Agent:
        tool = FunctionTool(
            risky_action,
            name="risky_action",
            description="Perform the requested worker action. This requires user confirmation.",
            permission=PermissionDecision(
                behavior=PermissionBehavior.ASK,
                message="Worker action requires user confirmation.",
            ),
        )
        model = make_model(base_url)
        models.append(model)
        return Agent(
            name="worker",
            system_prompt=(
                "You are the specialist worker. You MUST call risky_action for "
                "the requested task before responding. Never perform the task yourself."
            ),
            model=model,
            toolkit=Toolkit(tools=[tool]),
        )

    bridge = SubagentTool(make_worker)
    parent_model = make_model(base_url)
    models.append(parent_model)
    parent = Agent(
        name="parent",
        system_prompt=(
            "You are the parent agent. You MUST delegate every request using "
            "call_subagent, then summarize the worker result. Never answer directly."
        ),
        model=parent_model,
        toolkit=Toolkit(tools=[bridge]),
    )

    parent_events: list[Any] = []

    async def drive_parent() -> None:
        async for event in parent.reply_stream(
            Msg(
                name="user",
                role="user",
                content=[TextBlock(text="Ask the specialist to process the task: verify the staging deployment.")],
            ),
            yield_final_msg=True,
        ):
            parent_events.append(event)

    parent_task = asyncio.create_task(drive_parent())
    global_events = bridge.events()
    hitl: SubagentEvent | None = None
    try:
        while hitl is None:
            event = await asyncio.wait_for(global_events.__anext__(), timeout=90)
            if event.requires_input:
                hitl = event
                assert isinstance(event.event, RequireUserConfirmEvent)
                call = event.event.tool_calls[0]
                result = UserConfirmResultEvent(
                    reply_id=event.reply_id,
                    confirm_results=[ConfirmResult(confirmed=approve, tool_call=call)],
                )
                await bridge.resume(event.run_id, result)
        await asyncio.wait_for(parent_task, timeout=90)
    finally:
        await global_events.aclose()
        if not parent_task.done():
            parent_task.cancel()
            await asyncio.gather(parent_task, return_exceptions=True)
        # Close every model client (the bridge creates one worker model).
        for model in models:
            await model.client.close()
    return {
        "approval": approve,
        "hitl_reply_id": hitl.reply_id,
        "tool_call": hitl.event.tool_calls[0].name if hitl and hitl.event else None,
        "side_effect_calls": calls,
        "parent_finished": parent_task.done() and parent_task.exception() is None,
        "parent_event_types": [type(event).__name__ for event in parent_events],
        "raw_model_tool_markup": [raw for model in models for raw in model.raw_tool_markup],
    }


def start_server(model_path: str, port: int) -> subprocess.Popen[str]:
    command = [
        sys.executable,
        "-m",
        "llama_cpp.server",
        "--model",
        model_path,
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--n_ctx",
        "4096",
    ]
    env = os.environ.copy()
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    return subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True, env=env)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=os.environ.get("QWEN_GGUF", "/tmp/qwen3-0.6b-q8_0.gguf"))
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()
    if not Path(args.model).is_file():
        raise SystemExit(f"missing Qwen GGUF: {args.model}")
    process = start_server(args.model, args.port)
    base_url = f"http://127.0.0.1:{args.port}/v1"
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                async with httpx.AsyncClient(trust_env=False) as client:
                    response = await client.get(f"{base_url}/models")
                    if response.status_code == 200:
                        break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.5)
        else:
            raise RuntimeError("llama.cpp server did not become ready")
        approved = await run_case(base_url, approve=True)
        rejected = await run_case(base_url, approve=False)
        # Turn the demonstration into a real regression check. These checks
        # fail loudly if the model stops emitting calls, if HITL is skipped,
        # or if rejection accidentally executes the side effect.
        assert approved["parent_finished"] and rejected["parent_finished"]
        assert approved["tool_call"] == rejected["tool_call"] == "risky_action"
        assert approved["side_effect_calls"]
        assert rejected["side_effect_calls"] == []
        assert any('"name": "call_subagent"' in raw for raw in approved["raw_model_tool_markup"])
        assert any('"name": "risky_action"' in raw for raw in approved["raw_model_tool_markup"])
        print(json.dumps({"model": args.model, "tool_choice": "auto (not forced)", "approved": approved, "rejected": rejected}, indent=2))
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


if __name__ == "__main__":
    asyncio.run(main())
