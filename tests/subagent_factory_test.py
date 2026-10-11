"""Tests for the app ``extra_agent_tools`` adapter."""

import asyncio
from collections.abc import AsyncIterator
from unittest.async_case import IsolatedAsyncioTestCase

from agentscope import SubagentExecutionContext
from agentscope.agent import SubagentSession
from agentscope.event import (
    ReplyEndEvent,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
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


class _HitlWorker:
    def __init__(self, context: object) -> None:
        self.context = context
        self.resumed = False

    async def reply_stream(self, inputs: object) -> AsyncIterator[object]:
        if isinstance(inputs, Msg):
            yield RequireUserConfirmEvent(reply_id="context-reply", tool_calls=[])
            return
        assert isinstance(inputs, UserConfirmResultEvent)
        self.resumed = True
        yield ReplyEndEvent(
            session_id="worker-session",
            reply_id="context-reply",
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

    async def test_helper_tool_instances_preserve_a_and_b_identity_refs(self) -> None:
        """Each service helper tool rejects both cached workers after A/B."""
        workers = {
            "a": _Worker("a"),
            "b": _Worker("b"),
        }

        def resolver(agent_id: str) -> _Worker:
            return workers[agent_id]

        factory = create_subagent_tool_factory(
            resolver,
            allowed_agent_ids=set(workers),
            cleanup_ttl=None,
        )
        first_tools, second_tools = await asyncio.gather(
            factory("user-1", "parent-1", "session-1"),
            factory("user-2", "parent-2", "session-2"),
        )
        first_tool = first_tools[0]
        second_tool = second_tools[0]
        self.assertIsInstance(first_tool, SubagentTool)
        self.assertIsInstance(second_tool, SubagentTool)
        self.assertIsNot(first_tool, second_tool)

        async def run_sequence(tool: SubagentTool) -> list[dict[str, object]]:
            return [
                await tool.run(f"task-{agent_id}", agent_id=agent_id)
                for agent_id in ("a", "b", "a", "b")
            ]

        first_results, second_results = await asyncio.gather(
            run_sequence(first_tool),
            run_sequence(second_tool),
        )
        for results in (first_results, second_results):
            self.assertEqual(
                [result["status"] for result in results],
                ["completed", "completed", "error", "error"],
            )
            self.assertEqual(results[2]["error_code"], "agent_instance_reused")
            self.assertEqual(results[3]["error_code"], "agent_instance_reused")

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

    async def test_context_factory_binds_run_context_events_and_hitl_resume(self) -> None:
        """A parent context follows a run even when ``run`` omits context."""
        seen: list[SubagentExecutionContext] = []
        session_contexts: list[SubagentExecutionContext | None] = []
        workers: list[_HitlWorker] = []

        def session_factory(
            agent_factory: object,
            context: SubagentExecutionContext | None = None,
        ) -> SubagentSession:
            session_contexts.append(context)
            return SubagentSession(agent_factory, context=context)  # type: ignore[arg-type]

        def resolver(
            agent_id: str,
            context: SubagentExecutionContext | None = None,
        ) -> _HitlWorker:
            assert agent_id == "specialist"
            assert context is not None
            seen.append(context)
            worker = _HitlWorker(context)
            workers.append(worker)
            return worker

        parent = SubagentExecutionContext(
            "parent-run",
            "",
            principal="user-1",
            metadata={"workspace_id": "workspace-1"},
        )
        factory = create_subagent_tool_factory(
            resolver,
            context_factory=lambda *_: parent,
            allowed_agent_ids={"specialist"},
            cleanup_ttl=None,
            session_factory=session_factory,
        )
        tool = (await factory("user-1", "parent-agent", "session-1"))[0]
        run_task = asyncio.create_task(tool.run("approval", agent_id="specialist"))
        while not tool._runs:
            await asyncio.sleep(0)
        run_id = next(iter(tool._runs))
        assert run_id
        run = tool._runs[run_id]
        while run.pending is None:
            await asyncio.sleep(0)
        pending = run.pending
        assert pending is not None
        assert isinstance(pending.event, RequireUserConfirmEvent)
        assert isinstance(pending.context, SubagentExecutionContext)
        assert pending.context.parent_run_id == "parent-run"
        assert pending.context.subagent_run_id == run_id
        assert seen == [pending.context]
        assert session_contexts == [pending.context]

        await tool.resume(
            run_id,
            UserConfirmResultEvent(reply_id=pending.reply_id, confirm_results=[]),
            context=pending.context,
        )
        result = await run_task
        assert result["status"] == "completed"
        assert result["context"] == pending.context
        assert workers[0].context == pending.context
        assert workers[0].resumed
