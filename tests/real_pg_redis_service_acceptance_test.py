"""Opt-in acceptance checks for the dual-service PG + Redis HITL path.

Run with ``AGENTSCOPE_REAL_BACKENDS=1``.  The test starts disposable local
PostgreSQL and Redis processes, then two independent HTTP worker processes
that share them.  It deliberately does not claim Kubernetes pod migration:
the workers are ordinary processes and the assertion is limited to durable
state/queue/lock takeover.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any, ClassVar
from unittest import IsolatedAsyncioTestCase, SkipTest

from agentscope.agent import Agent, InjectionConfig
from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage import (
    AsyncSQLAlchemyStorage,
    ChatModelConfig,
    SessionConfig,
    SessionRecord,
)
from agentscope.event import (
    ConfirmResult,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
)
from agentscope.message import ToolCallBlock, UserMsg
from agentscope.model import ChatResponse
from agentscope.permission import (
    PermissionBehavior,
    PermissionContext,
    PermissionDecision,
)
from agentscope.pipeline import TeamMember, TeamPipeline
from agentscope.tool import ToolBase, ToolChunk, Toolkit

# Tests run from a source checkout; avoid importing the test helper by path.
REPO = Path(__file__).resolve().parents[1]
VENV = Path(os.environ.get("AGENTSCOPE_TEST_PYTHON", sys.executable))


def _default_pg_bin() -> Path:
    """Locate pgserver's disposable binaries without team-specific paths."""
    try:
        import pgserver

        candidate = Path(pgserver.__file__).resolve().parent / "pginstall" / "bin"
        if (candidate / "initdb").exists() and (candidate / "postgres").exists():
            return candidate
    except ImportError:
        pass
    initdb = shutil.which("initdb")
    return Path(initdb).resolve().parent if initdb else Path("/nonexistent")


PG_BIN = (
    Path(os.environ["AGENTSCOPE_PG_BIN"])
    if "AGENTSCOPE_PG_BIN" in os.environ
    else _default_pg_bin()
)
_redis_server = os.environ.get("AGENTSCOPE_REDIS_SERVER") or shutil.which("redis-server")
REDIS_BIN = Path(_redis_server) if _redis_server else Path("/nonexistent/redis-server")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_port(port: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"port {port} did not become ready")


def _http(port: int, path: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        headers={"content-type": "application/json"},
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read())


def _text_response(text: str) -> list[ChatResponse]:
    from agentscope.message import TextBlock

    return [
        ChatResponse(content=[TextBlock(text=text)], is_last=False, usage=None),
        ChatResponse(content=[TextBlock(text=text)], is_last=True, usage=None),
    ]


def _tool_call(call_id: str, name: str, value: str) -> list[ChatResponse]:
    return [
        ChatResponse(
            content=[ToolCallBlock(id=call_id, name=name, input=value)],
            is_last=False,
            usage=None,
        ),
        ChatResponse(
            content=[ToolCallBlock(id=call_id, name=name, input=value)],
            is_last=True,
            usage=None,
        ),
    ]


class _AskTool(ToolBase):
    """Deterministic tool whose permission check always parks for HITL."""

    name = "ask_tool"
    description = "asks the user"
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"input": {"type": "string"}},
        "required": ["input"],
    }
    is_concurrency_safe = True
    is_read_only = False

    async def check_permissions(
        self, tool_input: dict[str, Any], context: PermissionContext
    ) -> PermissionDecision:
        del tool_input, context
        return PermissionDecision(behavior=PermissionBehavior.ASK, message="confirm")

    async def call(self, input: str) -> ToolChunk:
        from agentscope.message import TextBlock

        return ToolChunk(content=[TextBlock(text=input)])


def _deterministic_pipeline() -> tuple[TeamPipeline, Agent, Agent]:
    """Build an actual TeamPipeline that deterministically parks a member."""
    from tests.utils import MockModel

    leader = Agent(
        name="leader",
        system_prompt="leader",
        model=MockModel(),
        injection_config=InjectionConfig(inject_runtime_state=False),
    )
    member = Agent(
        name="researcher",
        system_prompt="researcher",
        model=MockModel(),
        toolkit=Toolkit(tools=[_AskTool()]),
        injection_config=InjectionConfig(inject_runtime_state=False),
    )
    leader.model.set_responses(
        [
            _tool_call(
                "assign-1", "TeamAssign", '{"member":"researcher","prompt":"Find A"}'
            ),
            _text_response("done"),
        ]
    )
    member.model.set_responses(
        [
            _tool_call("ask-1", "ask_tool", '{"input":"A"}'),
            _text_response("A=42"),
        ]
    )
    return (
        TeamPipeline(
            leader=leader,
            members=[TeamMember(agent=member, description="researches")],
            reset_members=False,
        ),
        leader,
        member,
    )


class RealPGRedisAcceptance(IsolatedAsyncioTestCase):
    """Acceptance checks intentionally gated from ordinary unit runs."""

    async def asyncSetUp(self) -> None:
        if os.environ.get("AGENTSCOPE_REAL_BACKENDS") != "1":
            raise SkipTest(
                "set AGENTSCOPE_REAL_BACKENDS=1 to run real backend acceptance"
            )
        self.tmp = Path(tempfile.mkdtemp(prefix="agentscope-real-", dir=REPO))
        self.pg_port = _free_port()
        self.redis_port = _free_port()
        self.pg = self.tmp / "pgdata"
        initdb = PG_BIN / "initdb"
        postgres = PG_BIN / "postgres"
        if not initdb.exists() or not postgres.exists():
            raise SkipTest(f"PostgreSQL binaries unavailable under {PG_BIN}")
        if not REDIS_BIN.is_file() or not os.access(REDIS_BIN, os.X_OK):
            raise SkipTest(f"Redis server unavailable at {REDIS_BIN}")
        subprocess.run(  # noqa: ASYNC221
            [
                str(initdb),
                "-D",
                str(self.pg),
                "-U",
                "agent",
                "--auth=trust",
                "--no-locale",
            ],
            check=True,
            capture_output=True,
        )
        self.pg_proc = subprocess.Popen(  # noqa: ASYNC220
            [
                str(postgres),
                "-D",
                str(self.pg),
                "-p",
                str(self.pg_port),
                "-c",
                "unix_socket_directories=",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.redis_proc = subprocess.Popen(  # noqa: ASYNC220
            [
                str(REDIS_BIN),
                "--port",
                str(self.redis_port),
                "--save",
                "",
                "--appendonly",
                "no",
                "--bind",
                "127.0.0.1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            _wait_port(self.pg_port)
            _wait_port(self.redis_port)
        except Exception:
            self.pg_proc.terminate()
            self.redis_proc.terminate()
            raise
        self.pg_url = f"postgresql+asyncpg://agent@127.0.0.1:{self.pg_port}/postgres"

    async def asyncTearDown(self) -> None:
        for process in (
            getattr(self, "pg_proc", None),
            getattr(self, "redis_proc", None),
        ):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
        shutil.rmtree(getattr(self, "tmp", REPO / "does-not-exist"), ignore_errors=True)

    async def test_real_team_pipeline_parks_on_confirmation(self) -> None:
        """The SDK TeamPipeline emits a real HITL event and preserves state."""
        pipeline, leader, member = _deterministic_pipeline()
        events = [
            event
            async for event in pipeline.reply_stream(UserMsg(name="user", content="go"))
        ]
        self.assertTrue(
            any(isinstance(event, RequireUserConfirmEvent) for event in events)
        )
        self.assertEqual(
            member.state.get_awaiting_tool_calls("researcher")[0].state, "asking"
        )
        confirmation = UserConfirmResultEvent(
            reply_id=member.state.reply_id,
            confirm_results=[
                ConfirmResult(
                    confirmed=True, tool_call=member.state.context[-1].content[0]
                )
            ],
        )
        resumed = [event async for event in pipeline.reply_stream(confirmation)]
        self.assertTrue(resumed)
        self.assertFalse(member.state.get_awaiting_tool_calls("researcher"))
        self.assertFalse(leader.state.get_awaiting_tool_calls("leader"))

    async def test_two_http_service_processes_take_over_parked_hitl(self) -> None:
        """HTTP process A queues a confirmation; process B drains and resumes it."""
        storage = AsyncSQLAlchemyStorage(self.pg_url, create_tables=True)
        async with storage:
            session = SessionRecord(
                id="acceptance-session",
                user_id="acceptance-user",
                agent_id="acceptance-agent",
                config=SessionConfig(
                    workspace_id="acceptance-workspace",
                    chat_model_config=ChatModelConfig(
                        type="test",
                        credential_id="unused",
                        model="unused",
                        parameters={},
                    ),
                ),
            )
            await storage.upsert_session(
                user_id=session.user_id,
                agent_id=session.agent_id,
                config=session.config,
                state=session.state,
                session_id=session.id,
            )

        ports = [_free_port(), _free_port()]
        env = {
            **os.environ,
            "PYTHONPATH": f"{REPO / 'src'}:{REPO / 'tests'}:{REPO}",
            "ACCEPTANCE_PG_URL": self.pg_url,
            "ACCEPTANCE_REDIS_PORT": str(self.redis_port),
            "ACCEPTANCE_WORKER_B_PORT": str(ports[1]),
        }
        workers = []
        for port in ports:
            process = subprocess.Popen(  # noqa: ASYNC220
                [str(VENV), str(Path(__file__)), "--worker", str(port)],
                cwd=REPO,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            workers.append(process)
            _wait_port(port)
        try:
            # A is the process that originally owned the parked turn. Holding
            # its lease makes B prove it waits/retries rather than dropping the
            # HTTP confirmation trigger.
            self.assertEqual(_http(ports[0], "/hold", {"seconds": 8.0})["held"], True)
            event = UserConfirmResultEvent(
                reply_id="member-reply",
                confirm_results=[],
            ).model_dump(mode="json")
            trigger = _http(
                ports[0],
                "/chat",
                {
                    "agent_id": session.agent_id,
                    "session_id": session.id,
                    "input": event,
                },
            )
            self.assertEqual(trigger["status"], "started")
            # Simulate the original service process dying while its parked
            # lease is held. Redis lock cleanup on process exit makes the
            # durable trigger immediately eligible for process B.
            workers[0].terminate()
            workers[0].wait(timeout=5)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                state = _http(ports[1], "/state")
                if state.get("status") == "resumed":
                    self.assertEqual(state["worker"], "B")
                    ledger = _http(ports[1], "/ledger")
                    records = [
                        value if isinstance(value, dict) else json.loads(value)
                        for value in ledger.values()
                    ]
                    self.assertTrue(records, "wakeup ledger entry was not persisted")
                    self.assertTrue(
                        any(
                            record.get("state") == "completed"
                            and record.get("payload", {}).get("session_id")
                            == session.id
                            for record in records
                        ),
                        f"wakeup ledger did not complete: {records!r}",
                    )
                    break
                await asyncio.sleep(0.1)
            else:
                self.fail("process B never resumed the durable HITL trigger")
        finally:
            for process in workers:
                process.terminate()
                process.wait(timeout=5)


async def _worker_main(port: int) -> None:
    import uvicorn
    from fastapi import FastAPI

    from agentscope.app._manager import ChatRunRegistry, WakeupDispatcher
    from agentscope.app._router._chat import chat
    from agentscope.app._router._schema import ChatRequest
    from agentscope.app.message_bus import RedisMessageBus
    from agentscope.app.storage import AsyncSQLAlchemyStorage

    worker_name = (
        "B" if port == int(os.environ.get("ACCEPTANCE_WORKER_B_PORT", "-1")) else "A"
    )
    storage = AsyncSQLAlchemyStorage(
        os.environ["ACCEPTANCE_PG_URL"], create_tables=False
    )
    bus = RedisMessageBus(port=int(os.environ["ACCEPTANCE_REDIS_PORT"]), db=15)
    registry = ChatRunRegistry()

    class _Service:
        async def run(
            self, *, user_id: str, session_id: str, agent_id: str, input_msg: object
        ) -> None:
            async with bus.acquire_lock(
                MessageBusKeys.session_lock(session_id), ttl_secs=5
            ):
                session = await storage.get_session(user_id, agent_id, session_id)
                assert session is not None
                session.state.middle_context.update(
                    {
                        "status": "resumed",
                        "worker": worker_name,
                        "reply_id": getattr(input_msg, "reply_id", ""),
                    }
                )
                await storage.update_session_state(
                    user_id, agent_id, session_id, session.state
                )
            # WakeupDispatcher only marks a durable trigger completed when
            # the service reports success. Returning an explicit bool keeps
            # this acceptance worker compatible with the real ChatService
            # contract and exercises the crash-safe ledger transition.
            return True

    async with storage, bus, registry:
        service = _Service()
        async with WakeupDispatcher(bus, storage, service, registry):
            app = FastAPI()

            @app.post("/chat")
            async def trigger(body: dict) -> dict:
                request = ChatRequest.model_validate(body)
                result = await chat(
                    request,
                    user_id="acceptance-user",
                    chat_service=service,
                    chat_run_registry=registry,
                    message_bus=bus,
                )
                return result.model_dump()

            @app.post("/hold")
            async def hold(body: dict) -> dict:
                acquired = asyncio.Event()

                async def _hold() -> None:
                    async with bus.acquire_lock(
                        MessageBusKeys.session_lock("acceptance-session"), ttl_secs=5
                    ):
                        acquired.set()
                        await asyncio.sleep(float(body.get("seconds", 1.0)))

                asyncio.create_task(_hold())
                # Do not report success until process A really owns the lease;
                # otherwise the test could pass without exercising takeover.
                await acquired.wait()
                return {"held": True}

            @app.get("/ledger")
            async def ledger() -> dict:
                records = await bus.registry_getall(MessageBusKeys.wakeup_ledger())
                decoded = {}
                for field, value in records.items():
                    try:
                        decoded[field] = json.loads(value)
                    except (TypeError, ValueError):
                        decoded[field] = value
                return decoded

            @app.get("/state")
            async def state() -> dict:
                session = await storage.get_session(
                    "acceptance-user", "acceptance-agent", "acceptance-session"
                )
                return (
                    session.state.middle_context if session else {"status": "missing"}
                )

            uvicorn_config = uvicorn.Config(
                app, host="127.0.0.1", port=port, log_level="error"
            )
            server = uvicorn.Server(uvicorn_config)
            await server.serve()


if __name__ == "__main__" and len(sys.argv) >= 3 and sys.argv[1] == "--worker":
    asyncio.run(_worker_main(int(sys.argv[2])))
