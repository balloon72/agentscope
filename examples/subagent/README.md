# Stateless subagent with resumable HITL

This example adds a small composition layer on top of the AgentScope 2.0.9 reply protocol:

- `SubagentSession` creates one worker for one parent run.
- `start(task)` streams the worker events.
- If the worker emits `RequireUserConfirmEvent` or `RequireExternalExecutionEvent`, the session remains parked and exposes the worker `reply_id`.
- The service/UI sends the matching `UserConfirmResultEvent` or `ExternalExecutionResultEvent` to `resume()`.
- `ReplyEndEvent` marks the run complete; the caller can then discard the session.
- `SubagentManager` isolates multiple parent run IDs and rejects a result routed to the wrong `reply_id`.

This is intentionally different from a persistent Agent Team. The worker is retained only while the current turn is running or waiting for HITL. It is not a cross-request conversation member.

```python
from agentscope.agent import SubagentManager, ReActAgent
from agentscope.model import DashScopeChatModel


def make_worker() -> ReActAgent:
    return ReActAgent(
        name="specialist",
        sys_prompt="You are a specialist. Ask for confirmation before risky tools.",
        model=DashScopeChatModel(...),
        toolkit=build_specialist_toolkit(),
    )

manager = SubagentManager(make_worker)
run = manager.create(parent_run_id)

# First request: forward every event to the parent stream/UI.
async for event in run.start(task):
    if event.type == "REQUIRE_USER_CONFIRM":
        show_confirmation_card(event.reply_id, event.tool_calls)
    publish(event)

# On the user's response, route by the worker reply_id.
async for event in run.resume(user_confirm_result):
    publish(event)

if run.finished:
    manager.discard(parent_run_id)
```

The parent service owns the manager and can persist the pending `{parent_run_id,
worker_reply_id}` mapping in its normal session store. For a distributed deployment,
forward the confirmation to the worker process through the AgentScope service message
bus or the platform's equivalent. Do not fabricate a new reply ID: AgentScope uses the
worker event's `reply_id` to resume the parked tool call.

## Distributed deployment boundary

`SubagentManager` is an in-process composition helper. It is **not** a distributed
run registry and does not provide crash recovery. The following values live only in
the Python process:

- the `_runs` map and each `SubagentSession` lifecycle flags;
- the worker `Agent` object, any active `reply_stream` execution, toolkit and
  middleware instances; and
- the pending `reply_id`/HITL continuation and any model/tool call currently in
  flight.

Consequently, persisting only `{parent_run_id, worker_reply_id}` is insufficient:
another process cannot reconstruct the worker object or its active execution from
those two IDs. A process restart, or routing a resume request to a different node,
loses the parked worker unless the application supplies a durable run record and a way
to rebuild the worker.

For a genuinely distributed implementation, persist a versioned run record keyed by
`parent_run_id` that includes the worker/session identity, factory/config version,
serialized `AgentState` (including context and reply context; middleware state only
when it is serializable), the pending HITL event and its `reply_id`, lifecycle status,
and an ownership lease/fencing token. Persist the reply/event record before exposing
a resume token.
On resume, atomically claim the run, validate both `parent_run_id` and the exact
pending `reply_id`, rebuild a fresh worker from the record, apply the result once,
and checkpoint state again. Tool and external side effects need an idempotency key
(for example `parent_run_id/reply_id/tool_call_id`) because a crash can happen after
the side effect and before the checkpoint.

A distributed mutex only prevents overlapping turns while its lease is held; it does
not serialize the worker object or make an in-flight turn recoverable after a crash.

When using the AgentScope 2.0.9 service, keep the service's responsibilities distinct:

- `StorageBase` (SQL/Redis or another shared implementation) is the durable source
  for session messages and `AgentState`; `ChatService` reconstructs an agent under a
  distributed session lock on every turn.
- `RedisMessageBus` inbox/wakeup queues are live cross-process delivery. Queue drain
  is competing-consumer, at-most-once: an entry is removed when read, so a consumer
  crash can lose it. The Pub/Sub wake signal is transient. Session event logs are
  replay streams for observers, not a durable worker object or actor mailbox.
- Route a HITL result through a durable run/inbox entry and a distributed lock; do
  not treat an SSE/session event or a Pub/Sub notification as the resume itself.
- The service's `BackgroundTaskManager` may register task metadata in Redis, but the
  actual `asyncio.Task` and its result are owned by one process; an owner crash loses
  that execution unless the tool is backed by an external worker with its own durable
  result protocol. Likewise, `ChatRunRegistry` only contains local task handles.
- `SchedulerManager`'s APScheduler timers are in memory. In a multi-node service,
  designate exactly one scheduler owner (`enable_scheduler=True`) and keep it backed
  by shared schedule records; enabling timers on every replica can fire a cron job
  more than once.

This example currently does **not** implement that durable run record, worker
reconstruction, fencing, or side-effect idempotency. It is safe as written for one
process (or for a deployment that guarantees sticky routing and accepts loss on
process failure). Do not advertise it as HA or multi-node resumable until those
contracts are implemented. Local/in-memory managers and host-local workspaces (for
example `LocalWorkspaceManager` or Docker-on-host) likewise cannot be shared safely
by independent nodes; use a shared storage/message bus and a remote/shared workspace
backend when the surrounding service is scaled out.
`InMemoryMessageBus` is process-local and has the same single-process boundary; the
service example recommends `RedisMessageBus` for multi-process deployments.

### Suggested multi-node tests

Before claiming distributed support, run at least these integration tests against two
service processes and a shared SQL/Redis backend:

1. Start a run on node A, park it on HITL, then deliver the exact result to node B;
   assert one continuation, one persisted reply, and no duplicate tool call.
2. Kill the owner while parked, restart it (or let another node claim the lease),
   rebuild from the durable run record, and resume successfully; repeat a duplicate
   delivery and assert idempotent handling.
3. Reject an unknown `parent_run_id`, a stale fencing token, and a mismatched
   `reply_id` without changing the parked state.
4. Disconnect an SSE client, reconnect from its event cursor, and verify replay is
   complete within the configured log-retention window; separately verify that a
   missed Pub/Sub wake signal is recovered from the durable queue.
5. Run the same tests with a local/in-memory manager and document the intentional
   single-process limitation rather than treating it as distributed coverage.

For fixed parallel work with no per-member HITL, `FanoutPipeline` remains simpler.
Use `TeamPipeline` or the AgentScope service Agent Team when you need preconfigured
members, member lifecycle, or built-in participant HITL projection.
