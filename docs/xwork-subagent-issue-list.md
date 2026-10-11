# xWork collaborator subagent 总问题清单

本文是 xWork collaborator subagent 的唯一任务清单。后续修改、测试和验收以本文为准，其他分散的 subagent review 文档不再作为独立任务入口。

## 1. 目标和范围

xWork 的 `feature/agentscope-replace/opsloop-multipod` 计划用 ephemeral subagent 承担普通专业智能体协作：页面配置 collaborator agent，父 agent 运行时选择已配置的 agent，创建临时 worker，任务结束后释放。

```text
parent agent
  -> call_subagent(agent_id, task)
  -> resolve configured collaborator
  -> create temporary worker
  -> stream / HITL / result
  -> release worker
```

普通协作不创建 Agent Team、Team session 或用户可见独立会话。Agent Team 继续用于持久化成员、AgentInvite、跨回合 TeamSay 和长期任务。

本任务分为两部分：

1. AgentScope SDK / service 提供通用 resolver、context、事件、HITL 和生命周期接缝。
2. OpsLoop 使用 CP collaborator pool 将已配置的专业智能体接入 `SubagentTool`。

## 2. 当前已具备的能力

`codex/subagent-v2.0.9` 最新提交为 `d86daff3`，已有：

- `OneShotSubagent`
- `SubagentSession`
- `SubagentTool`
- collaborator resolver / allowlist 基础能力
- HITL pause/resume
- 并发限制、递归限制、结果长度限制、TTL 清理
- durable runner 的 SQLite/Redis 实现
- durable HITL correlation
- run-id collision 防护

当前验证结果：

- 定向 collaborator/context/factory/limits/tool 测试：15 passed
- `tests/subagent`：32 passed
- Windows durable 跨进程测试仍输出 `multiprocessing.popen_spawn_win32.py` 的 `access violation`，pytest 最终 exit code 为 0
- Linux CI 和真实 PostgreSQL + Redis OpsLoop 集成尚未验证

## 3. 官方 AgentScope 模式和本任务的关系

AgentScope 官方 2.x 的推荐选择是：

| 场景 | 官方模式 |
|---|---|
| 应用代码决定顺序 | 多个 Agent 显式传递消息 |
| 父 Agent 决定是否委派 | worker 包装成 `FunctionTool` |
| executor / verifier 循环 | `GoalPipeline` |
| 持久化 leader/worker 协作 | Agent Team |
| 远程 Agent | A2A |

本分支的 `OneShotSubagent`、`SubagentSession` 和 `SubagentTool` 是在官方 worker-as-tool、`reply_stream()` 和 `resume()` 之上的 xWork 扩展，不是 AgentScope 官方原生的 Agent Team 替代 API。

实现应保持两档：

- 无敏感工具、无 HITL 的简单任务：每次创建 worker，返回结构化结果，完成即释放。
- 有工具或 HITL 的任务：使用 `SubagentTool`，转发事件并通过 `resume()` 继续。

不要让普通 ephemeral 协作默认加载 SQLite、durable runner、lease/fence 或完整 Team 机制。

## 4. AgentScope SDK 必须修复的问题

### P1. `create_subagent_tool_factory` 丢弃父会话上下文

文件：`src/agentscope/tool/_subagent_factory.py`

`create_subagent_tool_factory()` 收到 `user_id`、父 `agent_id`、`session_id` 后仍然丢弃，resolver 只能收到 collaborator `agent_id`。

这会阻止 xWork 按父会话解析正确的：

- user / principal
- tenant
- parent agent
- parent session / parent run
- workspace
- permission scope
- collaborator pool

要求：

- 每次 `extra_agent_tools` 调用创建独立 resolver wrapper，或提供明确的 context factory。
- resolver 能收到父会话上下文和选中的 collaborator ID。
- 不使用跨 session 的可变全局上下文。
- 增加两个并发 session 的身份和 workspace 隔离测试。
- 保持旧的无 context factory 调用兼容。

### P1. 普通执行异常泄露原始错误

文件：`src/agentscope/tool/_subagent_tool.py`

通用异常路径仍可能返回：

```python
f"{type(exc).__name__}: {exc}"
```

provider、MCP、workspace 内部路径或敏感参数可能进入父 Agent 上下文。

要求：

- 完整 traceback 只写日志。
- 对调用方返回稳定 `error_code`、安全 message 和 retryable 标识。
- 增加测试，确认原始异常文本不会进入 tool result 或 parent context。

### P2. resolver 返回值的生命周期合同不清晰

当前 resolver 可以返回 Agent 实例或 factory。若应用缓存 Agent 实例，不同 run 可能共享 AgentState、middleware、workspace 句柄或 pending HITL。

要求二选一：

- resolver 只返回 factory，每个 run 创建新 Agent；或
- 保留实例兼容，但明确规定 resolver 必须为当前 run 新建实例，并增加防复用测试。

文档、类型标注和运行时行为必须一致。

### P2. durable context 序列化合同

`SubagentExecutionContext.metadata` / `principal` 不能声明任意对象可用，同时直接进入 JSON / Redis。

要求：

- 限制为 JSON-safe 值并 fail fast；或
- 提供明确 serializer，遇到不可序列化值返回稳定错误。
- SQLite 和 Redis 路径的序列化测试保持一致。

## 5. 多节点生产必须完成的分布式合同

生产环境是多节点、多 Pod 部署，不能依赖进程内状态或 sticky routing 作为正确性保证。

`SubagentTool` / `SubagentManager` 本身保存 `_runs` registry、worker Agent、活跃 `reply_stream`、pending HITL continuation、asyncio task 和 event queue。这些状态只能用于一个 Pod 内的同步执行；Pod 重启或请求切到另一个 Pod 后，不能依靠本地 `_runs` 恢复。

多节点生产必须复用现有 PostgreSQL + Redis 路径：

- PostgreSQL 保存父 session、message、AgentState、subagent run metadata 和 pending HITL 记录。
- Redis 保存跨 Pod inbox、wakeup、lease、fence 和 idempotency coordination。
- 不使用 SQLite、不使用共享 PVC WAL、不使用进程内 memory/fakeredis 作为生产协调层。

即使目标是“用完即消失”，生产也要持久化最小运行记录：parent run、subagent run、collaborator agent/revision、身份和 workspace binding、status、owner/lease/fence、pending reply/event、delivery id 和 terminal result 摘要。完成、失败、取消或 timeout 后按 TTL 清理运行态。

HITL 或跨节点 resume 必须：

1. 先持久化 pending state，再发布父会话事件。
2. 通过 parent run / subagent run 找到唯一记录并用 Redis lease 原子 claim。
3. 校验 `reply_id`、event type、parent context 和 status。
4. 在持有 lease 的 Pod 重建 fresh worker，加载 AgentState 和 workspace binding。
5. 只执行一次 resume，并使用 delivery id 保证幂等。
6. commit 新状态并释放 lease；旧 Pod 的 late commit 必须被 fence 拒绝。

Redis Pub/Sub、SSE 和 wakeup 只能做通知，不能作为唯一的 resume 数据源。

无敏感工具、无 HITL、单次同步完成的任务可以在当前 Pod 内直接执行，但仍需记录 parent/subagent run 的开始、完成、失败和 collaborator 身份。该优化不能用于可能暂停的工具调用。

禁止：

- 只把 `SubagentTool` 放到每个 Pod，然后依赖 load balancer sticky session。
- 只持久化 `parent_run_id + reply_id`，不持久化 AgentState、pending event 和 config version。
- 在多 Pod 之间共享内存 Agent 实例。

## 6. OpsLoop 必须完成的接入

当前 `feature/agentscope-replace/opsloop-multipod` 尚未接入：

- `SubagentTool`
- `create_subagent_tool_factory`
- collaborator resolver
- parent/subagent event bridge
- subagent HITL resume

现有 `xspark_subagent_delta` / `team_stream_projector` 是 Agent Team worker 投影，不能视为 ephemeral subagent 已接通。

需要完成：

1. 在 per-session tool factory 中注册 `SubagentTool`。
2. 复用 CP collaborator pool / allowlist。
3. 使用 `cp_agent_id` 作为模型可见选择，服务端解析 `sdk_agent_id + owner_id`。
4. 创建独立 AgentScope worker，不复用父 AgentState 或完整父消息历史。
5. 继承父会话允许的身份、workspace、技能和权限上限。
6. 把子事件投影到父 run 的 SSE / timeline。
7. HITL resume 校验 parent run、subagent run、reply id 和 event type。
8. 完成、失败、取消、timeout 后释放 worker、stream、queue 和 registry。
9. 默认递归深度为 1，禁止 child 默认创建 child。
10. ephemeral run 不创建 Team session 或用户可见独立 session。

## 7. 当前项目的存储和部署边界

依据 `opsloop-multipod` 实际配置：

| 场景 | AgentScope storage | coordination / message bus |
|---|---|---|
| 多 Pod 生产/演练 | `AGENT_STORAGE=sql`，远程 PostgreSQL，独立 AgentScope schema | Redis direct 或 Sentinel，由 `AGENT_DISTRIBUTED_ENABLED` 和 `AGENT_BUS_*` 控制 |
| 单机开发默认 | `AGENT_STORAGE=memory`，InMemory/fakeredis | InMemoryMessageBus |
| 单机可选 | `AGENT_STORAGE=redis` | Redis storage / bus |

当前 OpsLoop 没有把 SQLite 配置为生产或 multipod storage，`SQLiteSubagentStore` 也尚未接入实际 subagent 链路。

因此：

- 不新增 SQLite 生产配置、PVC 或 migration。
- 不把 SQLite WAL 放到多个 Pod 共享目录。
- ephemeral subagent 复用父会话已有的 PostgreSQL/Redis 路径。
- 只有明确启用跨请求 durable subagent 时，才接入 `RedisSubagentStore` 的 run state、TTL、lease、fence 和跨 Pod resume。
- SQLite 只保留为 AgentScope SDK 的可选本地回归，不作为当前 OpsLoop 发布依赖。

## 8. 验收门槛

### Linux CI：当前 ephemeral 路径

```bash
pytest tests/subagent_collaborator_acceptance_test.py \
       tests/subagent_execution_context_test.py \
       tests/subagent_factory_test.py \
       tests/subagent_limits_test.py -q
pytest tests/subagent/test_subagent_session.py \
       tests/subagent/test_one_shot_subagent.py -q
```

要求 resolver、HITL、取消、清理、并发、递归和 run-id collision 通过。

### Linux + PostgreSQL + Redis 多节点集成

必须验证：

- 父子 run 不串线。
- collaborator allowlist 不能被模型参数绕过。
- 两个或更多 Pod 使用同一 Redis coordination 时状态正确。
- HITL resume、取消、timeout 和清理闭合。
- ephemeral subagent 不创建 Team session。
- PostgreSQL 中的既有 session / message / agent 数据合同不变。
- resume 请求可以路由到不同 Pod，不能依赖 sticky session。
- owner Pod 重启后，另一个 Pod 可以接管并继续 pending run。
- lease、fence、delivery 幂等和 late commit 拒绝通过。

### Durable 仅在明确启用时验证

如果本轮启用 durable subagent，额外验证：

```bash
pytest tests/subagent/test_durable_subagent.py -q
```

并在 Linux 多 Pod 环境验证 Redis run state、lease、fence、delivery 幂等、Pod 重启和 pending HITL resume。

### Windows

Windows `access violation` 必须修复，或明确记录为测试环境 blocker；不能静默忽略。无论 Windows 结论如何，Linux CI 和 PostgreSQL + Redis 集成门槛都不能降低。

## 9. 发布阻断条件

以下任一条件未满足，都不能宣布 subagent 适合 `opsloop-multipod`：

- 两个 P1 未修复或没有回归测试。
- OpsLoop 没有真正注册和运行 `SubagentTool`。
- resolver 没有收到父会话上下文。
- 原始异常仍能进入父 Agent 结果。
- Linux ephemeral 测试或 PostgreSQL + Redis 集成没有结果。
- 多 Pod resume 仍依赖 sticky routing 或本地 `_runs`。
- owner Pod 重启后 pending HITL 无法被另一个 Pod 接管。
- lease、fence 或 delivery idempotency 没有真实 Redis 验证。
- HITL、取消、timeout 或清理存在未闭合运行态。
- 生产路径引入共享 PVC SQLite。
- Windows access violation 被静默忽略。
