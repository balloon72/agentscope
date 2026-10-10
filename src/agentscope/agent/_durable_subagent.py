"""Durable, cross-process subagent runs.

This module deliberately lives beside (rather than changing) ``_subagent.py``.
``SubagentSession`` is an in-process convenience; ``DurableSubagentRunner``
checkpoints the real :class:`~agentscope.state.AgentState` and rebuilds an
Agent in a later process.  SQLite is included as a dependency-free shared
store (a WAL database on a shared local volume); ``RedisSubagentStore`` uses
Redis hashes and Lua compare-and-swap for deployments that already use Redis.

The delivery protocol is *at-least-once* around an external tool side effect:
a process can die after invoking a tool and before committing its checkpoint.
Delivery IDs, ownership tokens and fencing prevent duplicate committed turns,
but cannot make an arbitrary non-transactional external side effect exactly
once.  External tools should therefore be idempotent or use their own
idempotency key (the delivery ID).
"""

# The durable path intentionally swallows cleanup errors after recording a terminal state.
# ruff: noqa: BLE001, C408, S110
from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from ..event import (
    AgentEvent,
    ExternalExecutionResultEvent,
    ReplyEndEvent,
    RequireExternalExecutionEvent,
    RequireUserConfirmEvent,
    UserConfirmResultEvent,
    UserInterruptEvent,
)
from ..message import Msg, TextBlock
from ..state import AgentState


class DurableRunError(RuntimeError):
    """Base class for durable runner errors."""


class RunBusyError(DurableRunError):
    """Another process currently owns the run lease."""


class LostOwnershipError(DurableRunError):
    """A stale worker attempted to commit after fencing."""


class RunNotFoundError(DurableRunError):
    """The requested run does not exist."""


class RunStatus(StrEnum):
    RUNNING = "running"
    WAITING = "waiting_hitl"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    task: str
    factory_id: str
    state: AgentState
    status: RunStatus
    pending_reply_id: str | None
    pending_event: dict[str, Any] | None
    error: str | None
    fence: int
    updated_at: float


@dataclass(frozen=True)
class DurableTurnResult:
    """The atomically checkpointed result of one delivery."""

    run_id: str
    status: RunStatus
    events: tuple[AgentEvent | Msg, ...]
    pending_reply_id: str | None = None
    error: str | None = None
    duplicate: bool = False


@dataclass(frozen=True)
class _Claim:
    kind: str  # claimed, duplicate, terminal
    record: RunRecord
    fence: int = 0
    owner: str = ""
    response: DurableTurnResult | None = None


class DurableStore(Protocol):
    async def claim(
        self,
        run_id: str,
        task: str,
        factory_id: str,
        delivery_id: str,
        owner: str,
        lease_seconds: float,
    ) -> _Claim: ...

    async def commit(
        self,
        run_id: str,
        owner: str,
        fence: int,
        state: AgentState,
        status: RunStatus,
        pending_reply_id: str | None,
        pending_event: dict[str, Any] | None,
        events: list[dict[str, Any]],
        delivery_id: str,
        error: str | None = None,
    ) -> DurableTurnResult: ...

    async def cancel(self, run_id: str, reason: str) -> DurableTurnResult: ...

    async def get(self, run_id: str) -> RunRecord: ...

    async def renew(
        self, run_id: str, owner: str, fence: int, lease_seconds: float
    ) -> bool: ...


# ---------------------------------------------------------------------------
# JSON codecs. AgentState is intentionally persisted through its public
# pydantic schema, so a process restart restores the actual Agent state.


def _class_path(value: object) -> str:
    cls = type(value)
    return f"{cls.__module__}:{cls.__qualname__}"


def _pack(value: AgentEvent | Msg) -> dict[str, Any]:
    return {"class": _class_path(value), "data": value.model_dump(mode="json")}


def _unpack(value: dict[str, Any]) -> AgentEvent | Msg:
    module_name, qualname = value["class"].split(":", 1)
    obj: Any = importlib.import_module(module_name)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj.model_validate(value["data"])


def _serialized(func: Callable[..., Any]) -> Callable[..., Any]:
    """Serialize operations sharing one sqlite connection within a process."""

    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            return func(self, *args, **kwargs)

    return wrapped


def _record_from_row(row: sqlite3.Row) -> RunRecord:
    pending = json.loads(row["pending_event"]) if row["pending_event"] else None
    return RunRecord(
        run_id=row["run_id"],
        task=row["task"],
        factory_id=row["factory_id"],
        state=AgentState.model_validate(json.loads(row["state_json"])),
        status=RunStatus(row["status"]),
        pending_reply_id=row["pending_reply_id"],
        pending_event=pending,
        error=row["error"],
        fence=int(row["fence"]),
        updated_at=float(row["updated_at"]),
    )


def _result_from_delivery(
    run_id: str, raw: str, duplicate: bool = True
) -> DurableTurnResult:
    value = json.loads(raw)
    return DurableTurnResult(
        run_id=run_id,
        status=RunStatus(value["status"]),
        events=tuple(_unpack(item) for item in value.get("events", [])),
        pending_reply_id=value.get("pending_reply_id"),
        error=value.get("error"),
        duplicate=duplicate,
    )


def _result_json(result: DurableTurnResult) -> str:
    return json.dumps(
        {
            "status": result.status.value,
            "events": [_pack(event) for event in result.events],
            "pending_reply_id": result.pending_reply_id,
            "error": result.error,
        },
        separators=(",", ":"),
    )


class SQLiteSubagentStore:
    """A WAL SQLite store with transactional ownership and fencing.

    Multiple processes may open the same file. Each claim/commit runs in an
    IMMEDIATE transaction; ``fence`` increases on every takeover, making a
    delayed old worker unable to overwrite a newer checkpoint.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.path, timeout=30, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS subagent_runs (
            run_id TEXT PRIMARY KEY, task TEXT NOT NULL, factory_id TEXT NOT NULL,
            state_json TEXT NOT NULL, status TEXT NOT NULL,
            pending_reply_id TEXT, pending_event TEXT, error TEXT,
            fence INTEGER NOT NULL DEFAULT 0, owner TEXT, owner_expires REAL,
            updated_at REAL NOT NULL, deliveries TEXT NOT NULL DEFAULT '{}'
        )""")

    def close(self) -> None:
        self._conn.close()

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

    @_serialized
    def _tx_claim(
        self,
        run_id: str,
        task: str,
        factory_id: str,
        delivery_id: str,
        owner: str,
        lease_seconds: float,
    ) -> _Claim:
        now = time.time()
        con = self._conn
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute(
                "SELECT * FROM subagent_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                state = AgentState()
                con.execute(
                    "INSERT INTO subagent_runs(run_id,task,factory_id,state_json,status,updated_at) VALUES(?,?,?,?,?,?)",
                    (
                        run_id,
                        task,
                        factory_id,
                        json.dumps(state.model_dump(mode="json")),
                        RunStatus.RUNNING.value,
                        now,
                    ),
                )
                row = con.execute(
                    "SELECT * FROM subagent_runs WHERE run_id=?", (run_id,)
                ).fetchone()
            elif row["task"] != task or row["factory_id"] != factory_id:
                raise ValueError(
                    "run_id already exists with a different task or agent factory"
                )
            deliveries = json.loads(row["deliveries"] or "{}")
            if delivery_id in deliveries:
                record = _record_from_row(row)
                con.execute("COMMIT")
                return _Claim(
                    "duplicate",
                    record,
                    response=_result_from_delivery(run_id, deliveries[delivery_id]),
                )
            status = RunStatus(row["status"])
            if status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
                record = _record_from_row(row)
                con.execute("COMMIT")
                return _Claim("terminal", record)
            expiry = row["owner_expires"]
            if row["owner"] and expiry is not None and float(expiry) > now:
                raise RunBusyError(f"run {run_id!r} is owned by another worker")
            fence = int(row["fence"]) + 1
            con.execute(
                "UPDATE subagent_runs SET owner=?,owner_expires=?,fence=?,status=?,updated_at=? WHERE run_id=?",
                (
                    owner,
                    now + lease_seconds,
                    fence,
                    RunStatus.RUNNING.value,
                    now,
                    run_id,
                ),
            )
            row = con.execute(
                "SELECT * FROM subagent_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            con.execute("COMMIT")
            return _Claim("claimed", _record_from_row(row), fence=fence, owner=owner)
        except BaseException:
            con.execute("ROLLBACK")
            raise

    async def claim(
        self,
        run_id: str,
        task: str,
        factory_id: str,
        delivery_id: str,
        owner: str,
        lease_seconds: float,
    ) -> _Claim:
        return await asyncio.to_thread(
            self._tx_claim, run_id, task, factory_id, delivery_id, owner, lease_seconds
        )

    @_serialized
    def _tx_commit(
        self,
        run_id: str,
        owner: str,
        fence: int,
        state: AgentState,
        status: RunStatus,
        pending_reply_id: str | None,
        pending_event: dict[str, Any] | None,
        events: list[dict[str, Any]],
        delivery_id: str,
        error: str | None,
    ) -> DurableTurnResult:
        now = time.time()
        con = self._conn
        result = DurableTurnResult(
            run_id, status, tuple(_unpack(e) for e in events), pending_reply_id, error
        )
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute(
                "SELECT * FROM subagent_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise RunNotFoundError(run_id)
            if row["owner"] != owner or int(row["fence"]) != fence:
                raise LostOwnershipError(f"run {run_id!r} ownership was fenced")
            deliveries = json.loads(row["deliveries"] or "{}")
            deliveries[delivery_id] = _result_json(result)
            cur = con.execute(
                "UPDATE subagent_runs SET state_json=?,status=?,pending_reply_id=?,pending_event=?,error=?,owner=NULL,owner_expires=NULL,updated_at=?,deliveries=? WHERE run_id=? AND owner=? AND fence=?",
                (
                    json.dumps(state.model_dump(mode="json")),
                    status.value,
                    pending_reply_id,
                    json.dumps(pending_event) if pending_event is not None else None,
                    error,
                    now,
                    json.dumps(deliveries, separators=(",", ":")),
                    run_id,
                    owner,
                    fence,
                ),
            )
            if cur.rowcount != 1:
                raise LostOwnershipError(run_id)
            con.execute("COMMIT")
            return result
        except BaseException:
            con.execute("ROLLBACK")
            raise

    async def commit(self, *args: Any, **kwargs: Any) -> DurableTurnResult:
        return await asyncio.to_thread(self._tx_commit, *args, **kwargs)

    @_serialized
    def _tx_get(self, run_id: str) -> RunRecord:
        row = self._conn.execute(
            "SELECT * FROM subagent_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise RunNotFoundError(run_id)
        return _record_from_row(row)

    async def get(self, run_id: str) -> RunRecord:
        return await asyncio.to_thread(self._tx_get, run_id)

    @_serialized
    def _tx_renew(
        self, run_id: str, owner: str, fence: int, lease_seconds: float
    ) -> bool:
        cur = self._conn.execute(
            "UPDATE subagent_runs SET owner_expires=? WHERE run_id=? AND owner=? AND fence=? AND status=?",
            (
                time.time() + lease_seconds,
                run_id,
                owner,
                fence,
                RunStatus.RUNNING.value,
            ),
        )
        return cur.rowcount == 1

    async def renew(
        self, run_id: str, owner: str, fence: int, lease_seconds: float
    ) -> bool:
        return await asyncio.to_thread(
            self._tx_renew, run_id, owner, fence, lease_seconds
        )

    @_serialized
    def _tx_cancel(self, run_id: str, reason: str) -> DurableTurnResult:
        con = self._conn
        now = time.time()
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute(
                "SELECT * FROM subagent_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise RunNotFoundError(run_id)
            status = RunStatus(row["status"])
            if status == RunStatus.CANCELLED:
                con.execute("COMMIT")
                return DurableTurnResult(
                    run_id, status, tuple(), error=row["error"], duplicate=True
                )
            if status in (RunStatus.COMPLETED, RunStatus.FAILED):
                con.execute("COMMIT")
                return DurableTurnResult(
                    run_id, status, tuple(), error=row["error"], duplicate=True
                )
            new_fence = int(row["fence"]) + 1
            result = DurableTurnResult(
                run_id, RunStatus.CANCELLED, tuple(), error=reason
            )
            deliveries = json.loads(row["deliveries"] or "{}")
            deliveries.setdefault("__cancel__", _result_json(result))
            con.execute(
                "UPDATE subagent_runs SET status=?,error=?,owner=NULL,owner_expires=NULL,fence=?,updated_at=?,deliveries=? WHERE run_id=?",
                (
                    RunStatus.CANCELLED.value,
                    reason,
                    new_fence,
                    now,
                    json.dumps(deliveries),
                    run_id,
                ),
            )
            con.execute("COMMIT")
            return result
        except BaseException:
            con.execute("ROLLBACK")
            raise

    async def cancel(self, run_id: str, reason: str) -> DurableTurnResult:
        return await asyncio.to_thread(self._tx_cancel, run_id, reason)


class RedisSubagentStore:
    """Redis-backed store using atomic Lua ownership/fencing operations.

    Pass an existing ``redis.asyncio.Redis`` client. The store never closes
    the client. Redis remains an optional dependency until this class is used.
    """

    def __init__(
        self, client: Any, prefix: str = "agentscope:durable_subagent"
    ) -> None:
        self.client, self.prefix = client, prefix.rstrip(":")

    def _key(self, run_id: str) -> str:
        return f"{self.prefix}:{run_id}"

    async def _read(self, run_id: str) -> tuple[RunRecord, dict[str, str]]:
        raw = await self.client.hgetall(self._key(run_id))
        if not raw:
            raise RunNotFoundError(run_id)

        def get(k: str, default: str = "") -> str:
            v = raw.get(
                k.encode() if isinstance(next(iter(raw), ""), bytes) else k, default
            )
            return v.decode() if isinstance(v, bytes) else (v or default)

        return RunRecord(
            run_id,
            get("task"),
            get("factory_id"),
            AgentState.model_validate(json.loads(get("state"))),
            RunStatus(get("status")),
            get("pending_reply_id") or None,
            json.loads(get("pending_event")) if get("pending_event") else None,
            get("error") or None,
            int(get("fence", "0")),
            float(get("updated_at", "0")),
        ), raw

    async def claim(
        self,
        run_id: str,
        task: str,
        factory_id: str,
        delivery_id: str,
        owner: str,
        lease_seconds: float,
    ) -> _Claim:
        key = self._key(run_id)
        now = time.time()
        state = AgentState().model_dump_json()
        script = """local exists=redis.call('EXISTS',KEYS[1]); if exists==0 then redis.call('HSET',KEYS[1],'task',ARGV[1],'factory_id',ARGV[2],'state',ARGV[3],'status','running','fence','0','updated_at',ARGV[4],'deliveries','{}') end; local task=redis.call('HGET',KEYS[1],'task'); local fac=redis.call('HGET',KEYS[1],'factory_id'); if task~=ARGV[1] or fac~=ARGV[2] then return {'mismatch'} end; local ds=redis.call('HGET',KEYS[1],'deliveries'); local d=cjson.decode(ds or '{}'); if d[ARGV[5]] then return {'duplicate',d[ARGV[5]]} end; local st=redis.call('HGET',KEYS[1],'status'); if st=='completed' or st=='failed' or st=='cancelled' then return {'terminal'} end; local owner=redis.call('HGET',KEYS[1],'owner'); local exp=tonumber(redis.call('HGET',KEYS[1],'owner_expires') or '0'); if owner and owner~='' and exp>tonumber(ARGV[4]) then return {'busy'} end; local f=redis.call('HINCRBY',KEYS[1],'fence',1); redis.call('HSET',KEYS[1],'owner',ARGV[6],'owner_expires',tonumber(ARGV[4])+tonumber(ARGV[7]),'status','running','updated_at',ARGV[4]); return {'claimed',tostring(f)}"""
        out = await self.client.eval(
            script,
            1,
            key,
            task,
            factory_id,
            state,
            str(now),
            delivery_id,
            owner,
            str(lease_seconds),
        )
        code = out[0].decode() if isinstance(out[0], bytes) else out[0]
        record, _ = await self._read(run_id)
        if code == "mismatch":
            raise ValueError(
                "run_id already exists with a different task or agent factory"
            )
        if code == "busy":
            raise RunBusyError(run_id)
        if code == "duplicate":
            return _Claim(
                code,
                record,
                response=_result_from_delivery(
                    run_id, out[1].decode() if isinstance(out[1], bytes) else out[1]
                ),
            )
        if code == "terminal":
            return _Claim(code, record)
        return _Claim(code, record, fence=int(out[1]), owner=owner)

    async def commit(
        self,
        run_id: str,
        owner: str,
        fence: int,
        state: AgentState,
        status: RunStatus,
        pending_reply_id: str | None,
        pending_event: dict[str, Any] | None,
        events: list[dict[str, Any]],
        delivery_id: str,
        error: str | None = None,
    ) -> DurableTurnResult:
        result = DurableTurnResult(
            run_id, status, tuple(_unpack(e) for e in events), pending_reply_id, error
        )
        key = self._key(run_id)
        payload = json.dumps(state.model_dump(mode="json"))
        delivery = _result_json(result)
        # Keep optional fields absent when their value is ``None``.  In
        # particular, serializing ``pending_event=None`` as ``{}`` makes a
        # completed run look as though it still has a pending HITL event and
        # diverges from the SQLite store's ``None`` representation.
        script = """if redis.call('HGET',KEYS[1],'owner')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[1],'fence'))~=tonumber(ARGV[2]) then return {'lost'} end; local d=cjson.decode(redis.call('HGET',KEYS[1],'deliveries') or '{}'); d[ARGV[9]]=ARGV[10]; redis.call('HSET',KEYS[1],'state',ARGV[3],'status',ARGV[4],'pending_reply_id',ARGV[5],'error',ARGV[7],'updated_at',ARGV[8],'deliveries',cjson.encode(d)); if ARGV[6]=='' then redis.call('HDEL',KEYS[1],'pending_event') else redis.call('HSET',KEYS[1],'pending_event',ARGV[6]) end; redis.call('HDEL',KEYS[1],'owner','owner_expires'); return {'ok'}"""
        out = await self.client.eval(
            script,
            1,
            key,
            owner,
            str(fence),
            payload,
            status.value,
            pending_reply_id or "",
            json.dumps(pending_event) if pending_event is not None else "",
            error or "",
            str(time.time()),
            delivery_id,
            delivery,
        )
        code = out[0].decode() if isinstance(out[0], bytes) else out[0]
        if code != "ok":
            raise LostOwnershipError(run_id)
        return result

    async def get(self, run_id: str) -> RunRecord:
        return (await self._read(run_id))[0]

    async def renew(
        self, run_id: str, owner: str, fence: int, lease_seconds: float
    ) -> bool:
        return bool(
            await self.client.eval(
                "if redis.call('HGET',KEYS[1],'owner')==ARGV[1] and tonumber(redis.call('HGET',KEYS[1],'fence'))==tonumber(ARGV[2]) then redis.call('HSET',KEYS[1],'owner_expires',tonumber(ARGV[3])+tonumber(ARGV[4])); return 1 end; return 0",
                1,
                self._key(run_id),
                owner,
                str(fence),
                str(time.time()),
                str(lease_seconds),
            )
        )

    async def cancel(self, run_id: str, reason: str) -> DurableTurnResult:
        key = self._key(run_id)
        script = """local st=redis.call('HGET',KEYS[1],'status'); if not st then return {'missing'} end; if st=='completed' or st=='failed' or st=='cancelled' then return {'terminal',st,redis.call('HGET',KEYS[1],'error') or ''} end; local f=redis.call('HINCRBY',KEYS[1],'fence',1); redis.call('HSET',KEYS[1],'status','cancelled','error',ARGV[1],'owner','','owner_expires','0','updated_at',ARGV[2]); return {'cancelled',tostring(f)}"""
        out = await self.client.eval(script, 1, key, reason, str(time.time()))
        code = out[0].decode() if isinstance(out[0], bytes) else out[0]
        if code == "missing":
            raise RunNotFoundError(run_id)
        if code == "terminal":
            status = RunStatus(out[1].decode() if isinstance(out[1], bytes) else out[1])
            err = out[2].decode() if isinstance(out[2], bytes) else out[2]
            return DurableTurnResult(
                run_id, status, tuple(), error=err or None, duplicate=True
            )
        return DurableTurnResult(run_id, RunStatus.CANCELLED, tuple(), error=reason)


class DurableSubagentRunner:
    """Run and resume subagents with durable checkpoints."""

    def __init__(
        self,
        store: DurableStore,
        agent_factory: Callable[..., Any],
        factory_id: str = "default",
        lease_seconds: float = 60.0,
    ) -> None:
        self.store, self.agent_factory, self.factory_id, self.lease_seconds = (
            store,
            agent_factory,
            factory_id,
            lease_seconds,
        )

    async def get(self, run_id: str) -> RunRecord:
        return await self.store.get(run_id)

    async def start(
        self, run_id: str, task: str, delivery_id: str | None = None
    ) -> DurableTurnResult:
        if not task.strip():
            raise ValueError("task must not be empty")
        return await self._execute(
            run_id,
            task,
            delivery_id or str(uuid.uuid4()),
            Msg(name="parent", role="user", content=[TextBlock(text=task)]),
        )

    async def resume(
        self,
        run_id: str,
        event: UserConfirmResultEvent
        | ExternalExecutionResultEvent
        | UserInterruptEvent,
        delivery_id: str | None = None,
    ) -> DurableTurnResult:
        record = await self.store.get(run_id)
        return await self._execute(
            run_id, record.task, delivery_id or str(uuid.uuid4()), event
        )

    async def cancel(
        self, run_id: str, reason: str = "cancelled by parent"
    ) -> DurableTurnResult:
        return await self.store.cancel(run_id, reason)

    async def _execute(
        self, run_id: str, task: str, delivery_id: str, inputs: Any
    ) -> DurableTurnResult:
        owner = uuid.uuid4().hex
        claim = await self.store.claim(
            run_id, task, self.factory_id, delivery_id, owner, self.lease_seconds
        )
        if claim.response is not None:
            return claim.response
        if claim.kind == "terminal":
            return DurableTurnResult(
                run_id,
                claim.record.status,
                tuple(),
                claim.record.pending_reply_id,
                claim.record.error,
                duplicate=True,
            )
        state = claim.record.state
        agent: Any | None = None
        outputs: list[AgentEvent | Msg] = []
        pending_id = None
        pending_event = None
        ended = False
        heartbeat = asyncio.create_task(self._heartbeat(run_id, owner, claim.fence))
        stream = None
        try:
            agent = self._make_agent(state)
            stream = agent.reply_stream(inputs)
            async for output in stream:
                outputs.append(output)
                if isinstance(
                    output, (RequireUserConfirmEvent, RequireExternalExecutionEvent)
                ):
                    pending_id, pending_event = (
                        output.reply_id,
                        output.model_dump(mode="json"),
                    )
                    break
                if isinstance(output, ReplyEndEvent):
                    ended = True
            if stream is not None and pending_id is not None:
                closer = getattr(stream, "aclose", None)
                if closer:
                    await closer()
            if not ended and pending_id is None:
                # A stream ending without a terminal event is a failed run;
                # never leave a process-owned run looking resumable.
                status = RunStatus.FAILED
                error = "agent stream ended without ReplyEndEvent or HITL request"
            else:
                status = RunStatus.WAITING if pending_id else RunStatus.COMPLETED
                error = None
            return await self.store.commit(
                run_id,
                owner,
                claim.fence,
                agent.state,
                status,
                pending_id,
                pending_event,
                [_pack(e) for e in outputs],
                delivery_id,
                error,
            )
        except asyncio.CancelledError:
            # Best effort: cancellation is durable if ownership is still ours.
            try:
                await asyncio.shield(
                    self.store.commit(
                        run_id,
                        owner,
                        claim.fence,
                        getattr(agent, "state", state) if agent is not None else state,
                        RunStatus.CANCELLED,
                        None,
                        None,
                        [_pack(e) for e in outputs],
                        delivery_id,
                        "worker task cancelled",
                    )
                )
            except Exception:
                pass
            raise
        except Exception as exc:
            try:
                await self.store.commit(
                    run_id,
                    owner,
                    claim.fence,
                    getattr(agent, "state", state) if agent is not None else state,
                    RunStatus.FAILED,
                    None,
                    None,
                    [_pack(e) for e in outputs],
                    delivery_id,
                    f"{type(exc).__name__}: {exc}",
                )
            except Exception:
                pass
            raise
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except BaseException:
                pass
            if stream is not None and pending_id is None:
                closer = getattr(stream, "aclose", None)
                if closer:
                    try:
                        await closer()
                    except Exception:
                        pass

    async def _heartbeat(self, run_id: str, owner: str, fence: int) -> None:
        interval = max(0.1, self.lease_seconds / 3)
        try:
            while True:
                await asyncio.sleep(interval)
                if not await self.store.renew(run_id, owner, fence, self.lease_seconds):
                    return
        except asyncio.CancelledError:
            return

    def _make_agent(self, state: AgentState) -> Any:
        try:
            signature = inspect.signature(self.agent_factory)
            if len(signature.parameters) == 0:
                return self.agent_factory()
        except (TypeError, ValueError):
            pass
        return self.agent_factory(state)


__all__ = [
    "DurableSubagentRunner",
    "DurableTurnResult",
    "LostOwnershipError",
    "RedisSubagentStore",
    "RunBusyError",
    "RunNotFoundError",
    "RunRecord",
    "RunStatus",
    "SQLiteSubagentStore",
]
