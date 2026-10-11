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
from ._execution_context import (
    SubagentExecutionContext,
    call_factory_with_context,
    canonical_context_json,
    context_for_run,
)


class DurableRunError(RuntimeError):
    """Base class for durable runner errors."""


class RunBusyError(DurableRunError):
    """Another process currently owns the run lease."""


class LostOwnershipError(DurableRunError):
    """A stale worker attempted to commit after fencing."""


class RunNotFoundError(DurableRunError):
    """The requested run does not exist."""


def _validate_resume_event(
    record: RunRecord,
    event: UserConfirmResultEvent | ExternalExecutionResultEvent | UserInterruptEvent,
) -> None:
    """Validate a continuation against the durable parked event.

    A durable inbox may outlive the process that emitted the HITL request,
    so the in-process :class:`SubagentSession` checks are not available when
    a different worker handles ``resume``.  Keep the run parked when a stale
    or cross-run result arrives; feeding it to a freshly reconstructed agent
    would otherwise turn a routing error into a terminal run failure.

    ``UserInterruptEvent`` is intentionally accepted for either HITL family:
    interrupting a parked confirmation and interrupting a parked external
    execution share the same cleanup path in ``Agent``.
    """
    if record.status is not RunStatus.WAITING or not record.pending_reply_id:
        raise ValueError(
            f"run {record.run_id!r} is not waiting for a HITL result",
        )
    if event.reply_id != record.pending_reply_id:
        raise ValueError(
            f"resume reply_id {event.reply_id!r} does not match "
            f"pending reply {record.pending_reply_id!r}",
        )
    if isinstance(event, UserInterruptEvent):
        return

    pending_type = (record.pending_event or {}).get("type")
    if pending_type == RequireUserConfirmEvent.model_fields["type"].default:
        expected = UserConfirmResultEvent
    elif pending_type == RequireExternalExecutionEvent.model_fields["type"].default:
        expected = ExternalExecutionResultEvent
    else:
        # Older/corrupt records may not include a discriminated event. The
        # reply-id check above still protects routing; let Agent perform its
        # normal payload validation for this legacy shape.
        return
    if not isinstance(event, expected):
        raise TypeError(
            f"pending subagent reply requires {expected.__name__}",
        )


def _pending_event_type(pending_event: dict[str, Any] | None) -> str | None:
    """Return the serialized HITL event type from a pending checkpoint."""
    if not isinstance(pending_event, dict):
        return None
    value = pending_event.get("type")
    if value is not None:
        return str(value)
    # Older/custom stores may persist the class path instead of the event's
    # enum value.  Keep the check generic and independent of any application
    # tenant/RBAC model.
    value = pending_event.get("class")
    return None if value is None else str(value).rsplit(".", 1)[-1]


def _context_matches(
    expected: SubagentExecutionContext | None,
    supplied: SubagentExecutionContext | None,
    run_id: str,
) -> bool:
    """Check the full execution binding for a run.

    Context equality intentionally covers the opaque principal and metadata
    as well as parent/child IDs.  This is a generic routing check: hosts can
    put their own authorization data in the context without this layer
    interpreting tenant or RBAC fields.
    """
    if supplied is None:
        return expected is None
    if supplied.subagent_run_id != run_id:
        return False
    return expected == supplied


def _context_json(context: SubagentExecutionContext | None) -> str | None:
    """Serialize context canonically for store-level compare-and-swap."""
    if context is None:
        return None
    return canonical_context_json(context)


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
    context: SubagentExecutionContext | None = None


@dataclass(frozen=True)
class DurableTurnResult:
    """The atomically checkpointed result of one delivery."""

    run_id: str
    status: RunStatus
    events: tuple[AgentEvent | Msg, ...]
    pending_reply_id: str | None = None
    error: str | None = None
    duplicate: bool = False
    context: SubagentExecutionContext | None = None


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
        context: SubagentExecutionContext | None = None,
        expected_status: RunStatus | None = None,
        expected_pending_reply_id: str | None = None,
        expected_pending_event_type: str | None = None,
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
        context: SubagentExecutionContext | None = None,
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
    # sqlite3.Row exposes keys() for schema-version compatibility.
    raw_context = (
        row["context_json"] if "context_json" in row.keys() else None  # noqa: SIM118
    )
    context = (
        SubagentExecutionContext(**json.loads(raw_context))
        if raw_context
        else None
    )
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
        context=context,
    )


def _result_from_delivery(
    run_id: str, raw: str, duplicate: bool = True
) -> DurableTurnResult:
    value = json.loads(raw)
    context = (
        SubagentExecutionContext(**value["context"])
        if value.get("context") is not None
        else None
    )
    return DurableTurnResult(
        run_id=run_id,
        status=RunStatus(value["status"]),
        events=tuple(_unpack(item) for item in value.get("events", [])),
        pending_reply_id=value.get("pending_reply_id"),
        error=value.get("error"),
        duplicate=duplicate,
        context=context,
    )


def _result_json(result: DurableTurnResult) -> str:
    payload = {
            "status": result.status.value,
            "events": [_pack(event) for event in result.events],
            "pending_reply_id": result.pending_reply_id,
            "error": result.error,
        }
    if result.context is not None:
        # Reuse the durable context codec so result deliveries fail with the
        # same stable error as SQLite/Redis context fields.
        payload["context"] = json.loads(canonical_context_json(result.context))
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
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
            context_json TEXT,
            fence INTEGER NOT NULL DEFAULT 0, owner TEXT, owner_expires REAL,
            updated_at REAL NOT NULL, deliveries TEXT NOT NULL DEFAULT '{}'
        )""")
        # Databases created by older versions remain readable and writable.
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(subagent_runs)")
        }
        if "context_json" not in columns:
            self._conn.execute("ALTER TABLE subagent_runs ADD COLUMN context_json TEXT")

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
        context: SubagentExecutionContext | None = None,
        expected_status: RunStatus | None = None,
        expected_pending_reply_id: str | None = None,
        expected_pending_event_type: str | None = None,
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
                    "INSERT INTO subagent_runs(run_id,task,factory_id,state_json,status,updated_at,context_json) VALUES(?,?,?,?,?,?,?)",
                    (
                        run_id,
                        task,
                        factory_id,
                        json.dumps(state.model_dump(mode="json")),
                        RunStatus.RUNNING.value,
                        now,
                        _context_json(context),
                    ),
                )
                row = con.execute(
                    "SELECT * FROM subagent_runs WHERE run_id=?", (run_id,)
                ).fetchone()
            elif row["task"] != task or row["factory_id"] != factory_id:
                raise ValueError(
                    "run_id already exists with a different task or agent factory"
                )
            if context is not None:
                stored_context = row["context_json"]
                try:
                    stored_value = (
                        SubagentExecutionContext(**json.loads(stored_context))
                        if stored_context
                        else None
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    stored_value = None
                if stored_value != context:
                    raise ValueError("subagent execution context does not match run")
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
            if expected_status is not None and status is not expected_status:
                raise ValueError("subagent run status does not allow this operation")
            if expected_pending_reply_id is not None and (
                row["pending_reply_id"] != expected_pending_reply_id
            ):
                raise ValueError("resume reply_id does not match the pending subagent reply")
            if expected_pending_event_type is not None:
                pending = json.loads(row["pending_event"]) if row["pending_event"] else None
                if _pending_event_type(pending) != expected_pending_event_type:
                    raise ValueError("resume event type does not match the pending HITL request")
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
        context: SubagentExecutionContext | None = None,
        expected_status: RunStatus | None = None,
        expected_pending_reply_id: str | None = None,
        expected_pending_event_type: str | None = None,
    ) -> _Claim:
        return await asyncio.to_thread(
            self._tx_claim,
            run_id,
            task,
            factory_id,
            delivery_id,
            owner,
            lease_seconds,
            context,
            expected_status,
            expected_pending_reply_id,
            expected_pending_event_type,
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
        context: SubagentExecutionContext | None = None,
    ) -> DurableTurnResult:
        now = time.time()
        con = self._conn
        result = DurableTurnResult(
            run_id,
            status,
            tuple(_unpack(e) for e in events),
            pending_reply_id,
            error,
            context=context,
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
                "UPDATE subagent_runs SET state_json=?,status=?,pending_reply_id=?,pending_event=?,error=?,context_json=?,owner=NULL,owner_expires=NULL,updated_at=?,deliveries=? WHERE run_id=? AND owner=? AND fence=?",
                (
                    json.dumps(state.model_dump(mode="json")),
                    status.value,
                    pending_reply_id,
                    json.dumps(pending_event) if pending_event is not None else None,
                    error,
                    _context_json(context),
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
                    run_id,
                    status,
                    tuple(),
                    error=row["error"],
                    duplicate=True,
                    context=_record_from_row(row).context,
                )
            if status in (RunStatus.COMPLETED, RunStatus.FAILED):
                con.execute("COMMIT")
                return DurableTurnResult(
                    run_id,
                    status,
                    tuple(),
                    error=row["error"],
                    duplicate=True,
                    context=_record_from_row(row).context,
                )
            new_fence = int(row["fence"]) + 1
            result = DurableTurnResult(
                run_id,
                RunStatus.CANCELLED,
                tuple(),
                error=reason,
                context=_record_from_row(row).context,
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
            (
                SubagentExecutionContext(**json.loads(get("context")))
                if get("context")
                else None
            ),
        ), raw

    async def claim(
        self,
        run_id: str,
        task: str,
        factory_id: str,
        delivery_id: str,
        owner: str,
        lease_seconds: float,
        context: SubagentExecutionContext | None = None,
        expected_status: RunStatus | None = None,
        expected_pending_reply_id: str | None = None,
        expected_pending_event_type: str | None = None,
    ) -> _Claim:
        key = self._key(run_id)
        now = time.time()
        state = AgentState().model_dump_json()
        context_json = _context_json(context) or ""
        script = """local exists=redis.call('EXISTS',KEYS[1]); if exists==0 then redis.call('HSET',KEYS[1],'task',ARGV[1],'factory_id',ARGV[2],'state',ARGV[3],'status','running','fence','0','updated_at',ARGV[4],'deliveries','{}'); if ARGV[8]~='' then redis.call('HSET',KEYS[1],'context',ARGV[8]) end end; local task=redis.call('HGET',KEYS[1],'task'); local fac=redis.call('HGET',KEYS[1],'factory_id'); if task~=ARGV[1] or fac~=ARGV[2] then return {'mismatch'} end; if ARGV[8]~='' and (redis.call('HGET',KEYS[1],'context') or '')~=ARGV[8] then return {'context'} end; local ds=redis.call('HGET',KEYS[1],'deliveries'); local d=cjson.decode(ds or '{}'); if d[ARGV[5]] then return {'duplicate',d[ARGV[5]]} end; local st=redis.call('HGET',KEYS[1],'status'); if ARGV[9]~='' and st~=ARGV[9] then return {'state'} end; if ARGV[10]~='' and (redis.call('HGET',KEYS[1],'pending_reply_id') or '')~=ARGV[10] then return {'reply'} end; if ARGV[11]~='' then local pe=redis.call('HGET',KEYS[1],'pending_event') or ''; if pe=='' or cjson.decode(pe).type~=ARGV[11] then return {'type'} end end; if st=='completed' or st=='failed' or st=='cancelled' then return {'terminal'} end; local owner=redis.call('HGET',KEYS[1],'owner'); local exp=tonumber(redis.call('HGET',KEYS[1],'owner_expires') or '0'); if owner and owner~='' and exp>tonumber(ARGV[4]) then return {'busy'} end; local f=redis.call('HINCRBY',KEYS[1],'fence',1); redis.call('HSET',KEYS[1],'owner',ARGV[6],'owner_expires',tonumber(ARGV[4])+tonumber(ARGV[7]),'status','running','updated_at',ARGV[4]); return {'claimed',tostring(f)}"""
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
            context_json,
            expected_status.value if expected_status is not None else "",
            expected_pending_reply_id or "",
            expected_pending_event_type or "",
        )
        code = out[0].decode() if isinstance(out[0], bytes) else out[0]
        record, _ = await self._read(run_id)
        if code == "mismatch":
            raise ValueError(
                "run_id already exists with a different task or agent factory"
            )
        if code == "context":
            raise ValueError("subagent execution context does not match run")
        if code == "state":
            raise ValueError("subagent run status does not allow this operation")
        if code == "reply":
            raise ValueError("resume reply_id does not match the pending subagent reply")
        if code == "type":
            raise ValueError("resume event type does not match the pending HITL request")
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
        context: SubagentExecutionContext | None = None,
    ) -> DurableTurnResult:
        result = DurableTurnResult(
            run_id,
            status,
            tuple(_unpack(e) for e in events),
            pending_reply_id,
            error,
            context=context,
        )
        key = self._key(run_id)
        payload = json.dumps(state.model_dump(mode="json"))
        delivery = _result_json(result)
        # Keep optional fields absent when their value is ``None``.  In
        # particular, serializing ``pending_event=None`` as ``{}`` makes a
        # completed run look as though it still has a pending HITL event and
        # diverges from the SQLite store's ``None`` representation.
        context_json = _context_json(context) or ""
        script = """if redis.call('HGET',KEYS[1],'owner')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[1],'fence'))~=tonumber(ARGV[2]) then return {'lost'} end; local d=cjson.decode(redis.call('HGET',KEYS[1],'deliveries') or '{}'); d[ARGV[9]]=ARGV[10]; redis.call('HSET',KEYS[1],'state',ARGV[3],'status',ARGV[4],'pending_reply_id',ARGV[5],'error',ARGV[7],'updated_at',ARGV[8],'deliveries',cjson.encode(d)); if ARGV[6]=='' then redis.call('HDEL',KEYS[1],'pending_event') else redis.call('HSET',KEYS[1],'pending_event',ARGV[6]) end; if ARGV[11]=='' then redis.call('HDEL',KEYS[1],'context') else redis.call('HSET',KEYS[1],'context',ARGV[11]) end; redis.call('HDEL',KEYS[1],'owner','owner_expires'); return {'ok'}"""
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
            context_json,
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
                run_id,
                status,
                tuple(),
                error=err or None,
                duplicate=True,
                context=(await self._read(run_id))[0].context,
            )
        return DurableTurnResult(
            run_id,
            RunStatus.CANCELLED,
            tuple(),
            error=reason,
            context=(await self._read(run_id))[0].context,
        )


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
        self,
        run_id: str,
        task: str,
        delivery_id: str | None = None,
        context: SubagentExecutionContext | None = None,
    ) -> DurableTurnResult:
        if not task.strip():
            raise ValueError("task must not be empty")
        context = context_for_run(context, run_id)
        return await self._execute(
            run_id,
            task,
            delivery_id or str(uuid.uuid4()),
            Msg(name="parent", role="user", content=[TextBlock(text=task)]),
            context,
            expected_status=RunStatus.RUNNING,
        )

    async def resume(
        self,
        run_id: str,
        event: UserConfirmResultEvent
        | ExternalExecutionResultEvent
        | UserInterruptEvent,
        delivery_id: str | None = None,
        context: SubagentExecutionContext | None = None,
    ) -> DurableTurnResult:
        record = await self.store.get(run_id)
        if not isinstance(
            event,
            (UserConfirmResultEvent, ExternalExecutionResultEvent, UserInterruptEvent),
        ):
            raise TypeError(
                "event must be a HITL result or UserInterruptEvent",
            )
        # Validate before claiming the run.  This keeps a stale or
        # cross-run inbox delivery retryable and, importantly, does not leave
        # an ownership lease behind when validation fails.  Terminal records
        # are allowed through to ``_execute`` so duplicate deliveries retain
        # the normal idempotent terminal response semantics.
        if record.status is RunStatus.WAITING:
            _validate_resume_event(record, event)
        if context is None:
            context = record.context
        else:
            context = context_for_run(context, run_id)
            if not _context_matches(record.context, context, run_id):
                raise ValueError("subagent execution context does not match run")
        expected_reply = (
            record.pending_reply_id if record.status is RunStatus.WAITING else None
        )
        expected_type = (
            _pending_event_type(record.pending_event)
            if record.status is RunStatus.WAITING
            else None
        )
        return await self._execute(
            run_id,
            record.task,
            delivery_id or str(uuid.uuid4()),
            event,
            context,
            expected_status=RunStatus.WAITING,
            expected_pending_reply_id=expected_reply,
            expected_pending_event_type=expected_type,
        )

    async def cancel(
        self,
        run_id: str,
        reason: str = "cancelled by parent",
        context: SubagentExecutionContext | None = None,
    ) -> DurableTurnResult:
        if context is not None:
            record = await self.store.get(run_id)
            if context_for_run(context, run_id) != record.context:
                raise ValueError("subagent execution context does not match run")
        return await self.store.cancel(run_id, reason)

    async def _execute(
        self,
        run_id: str,
        task: str,
        delivery_id: str,
        inputs: Any,
        context: SubagentExecutionContext | None = None,
        *,
        expected_status: RunStatus | None = None,
        expected_pending_reply_id: str | None = None,
        expected_pending_event_type: str | None = None,
    ) -> DurableTurnResult:
        owner = uuid.uuid4().hex
        claim_args = (
            run_id,
            task,
            self.factory_id,
            delivery_id,
            owner,
            self.lease_seconds,
        )
        # Keep the protocol source-compatible with host stores implemented
        # before the optional CAS binding arguments were added.  Built-in
        # stores expose the extended signature; legacy stores retain the
        # original context-only call shape.
        try:
            claim_signature = inspect.signature(self.store.claim)
            claim_parameters = claim_signature.parameters
            supports_cas = (
                any(
                    parameter.kind is inspect.Parameter.VAR_KEYWORD
                    for parameter in claim_parameters.values()
                )
                or "expected_status" in claim_parameters
            )
        except (TypeError, ValueError):
            supports_cas = True
        if supports_cas:
            claim = await self.store.claim(
                *claim_args,
                context=context,
                expected_status=expected_status,
                expected_pending_reply_id=expected_pending_reply_id,
                expected_pending_event_type=expected_pending_event_type,
            )
        elif context is None:
            claim = await self.store.claim(*claim_args)
        else:
            claim = await self.store.claim(*claim_args, context)
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
                context=claim.record.context,
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
            context = context or claim.record.context
            agent = self._make_agent(state, context)
            reply_stream = agent.reply_stream
            try:
                parameters = inspect.signature(reply_stream).parameters
            except (TypeError, ValueError):
                parameters = {}
            stream = (
                reply_stream(inputs, yield_final_msg=True)
                if "yield_final_msg" in parameters
                else reply_stream(inputs)
            )
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
            commit_args = (
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
            return (
                await self.store.commit(*commit_args)
                if context is None
                else await self.store.commit(*commit_args, context)
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
                        *(() if context is None else (context,)),
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
                    *(() if context is None else (context,)),
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

    def _make_agent(
        self,
        state: AgentState,
        context: SubagentExecutionContext | None = None,
    ) -> Any:
        return call_factory_with_context(self.agent_factory, context, state)


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
