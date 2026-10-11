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

## 2. 当前已具备的能力（2026-10-11 更新）

实现分支 `codex/subagent-v2.0.9` 已完成并同步以下 SDK 能力：

- `OneShotSubagent`、`SubagentSession`、`SubagentTool`
- collaborator resolver / allowlist / per-session context factory
- parent/subagent/run/reply/event type 关联与 HITL pause/resume
- Durable SQLite/Redis lease、fence、delivery 幂等和 durable HITL CAS
- 递归、按 parent run 并发、输入/结果/事件队列限制、TTL 清理
- resolver worker 防复用、run-id collision 防护
- JSON-safe ExecutionContext（principal/metadata/workspace/sandbox metadata）
- 安全异常结果（稳定 error_code、safe message、retryable；原始 traceback 只进日志）

本地持久工作区验证结果：

- SDK/回归相关测试：176 passed（在安装 `pytest-asyncio` 的验证环境中执行）
- 最后一轮 issue-list 定向测试、Ruff、compileall、diff-check：全部通过
- 真实 Redis：跨进程状态、HITL 恢复、错误 reply/type/context 拒绝、重放保护、workspace/sandbox metadata 持久化通过
- 本地 Qwen 真实模型：HITL 同意/拒绝 E2E 通过，拒绝场景无副作用执行

SDK 代码改造和本地回归已完成；仍未完成的是干净 CI 可复现性以及平台验收：真实 PostgreSQL + Redis 的 OpsLoop 集成、真实 K8s 沙箱和两节点无粘性路由。当前执行环境没有容器运行时、kubectl 或 root 权限。

### 2026-10-11 验证依赖修正

- 发现新增异步测试依赖 `pytest-asyncio`，已加入 `pyproject.toml` 的 `dev` extra（提交 `d29ee748bc0fbc91b401370ce5f943a9da01df46`），避免干净 CI 环境将异步测试误报为环境失败。
- 共享验证环境的定向 Subagent/Factory/Resolver/Context/Limits/Workspace/Tool 测试通过；fakeredis Lua、真实 PostgreSQL、双 Pod、K8s 和 Windows 仍按本文平台验收项执行。
- 新增 Redis durable 接管回归：fakeredis/多进程协议测试 15 passed；真实 `redis-server` 双进程用例在当前环境因无 Redis 二进制跳过。
- 用户态 PostgreSQL 16.2 + asyncpg/SQLAlchemy 已完成真实 TCP smoke：建表、credential upsert/read/delete 和用户隔离通过；这属于 SDK 存储 smoke，不等同于完整 OpsLoop 多 Pod 验收。

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

### P1. `create_subagent_tool_factory` 丢弃父会话上下文：已修复

已新增可选的 sync/async `context_factory(user_id, parent_agent_id, session_id)`。每个
session 创建独立 resolver wrapper，并将父会话上下文与本次 run context 合并；run
context 和实际 `agent_id` 具有更高优先级。旧的三参数 factory 和旧 resolver
签名保持兼容。新增两个并发 session 的身份/workspace 隔离测试。

### P1. 普通执行异常泄露原始错误：已修复

`SubagentTool` 和 `OneShotSubagent` 不再把 provider、MCP、workspace 路径或
敏感参数放进 tool result。异常返回稳定 `error_code`、安全 message 和
`retryable`；完整异常只写日志。新增 secret/path/provider 错误回归测试。

### P2. resolver 返回值的生命周期合同不清晰：已修复

保留返回 Agent 实例的兼容路径，但明确 resolver 每次调用必须返回当前 run 新建的
factory 或 Agent。SDK 会记录 worker identity，检测并拒绝缓存实例跨 run 复用，
返回稳定 `agent_instance_reused`；新增串行和并发防复用测试。

### P2. durable context 序列化合同：已修复

`principal` 和 `metadata` 现在要求 JSON-safe，拒绝不可序列化对象、NaN 和
Infinity，并使用 SQLite/Redis 共用的 canonical JSON（排序 key、禁止 NaN）。
新增不可序列化、嵌套 mutation 和 SQLite/Redis 一致性测试。

## 5. 多节点生产必须完成的分布式合同

生产环境是多节点、多 Pod 部署，不能依赖进程内状态或 sticky routing 作为正确性保证。

`SubagentTool` / `SubagentManager` 本身保存 `_runs` registry、worker Agent、活跃 `reply_stream`、pending HITL continuation、asyncio task 和 event queue。这些状态只能用于一个 Pod 内的同步执行；Pod 重启或请求切到另一个 Pod 后，不能依靠本地 `_runs` 恢复。

`K8sWorkspaceManager` 的 `_cache` 和锁是进程内状态，`K8sWorkspace` 使用 workspace_id 复用 Pod/PVC，PVC 通常是 ReadWriteOnce。单 manager 生命周期可以工作，但双 API Pod 无粘性路由时不能把这些进程内锁当作跨副本 ownership/fencing；必须在 OpsLoop 部署层增加协调，并验证一个 Pod 重启或释放不会误删另一个 Pod 正在使用的 workspace。

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

当前 `feature/agentscope-replace/opsloop-multipod` 的接入状态仍需在该仓库和真实环境中确认；按本清单目前记录，尚未接入：

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
11. 明确 sandbox policy：默认只继承父会话允许的 workspace/sandbox 权限上限；独立 sandbox 只能作为显式配置启用，不能由模型参数自行升级或跨用户复用。

## 7. 当前项目的存储和部署边界

以下边界是基于当前清单记录的目标配置，最终以 `opsloop-multipod` 仓库实际配置和部署清单为准：

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

### AgentScope SDK 本地门槛：已通过

- issue-list 定向测试、`tests/subagent`、resolver/factory/context/limits/workspace
  测试全部通过
- 当前完整相关回归为 176 passed
- Ruff、compileall、diff-check 通过
- 真实 Redis CAS/HITL/context 场景通过
- 本地真实 Qwen approve/reject E2E 已通过

### Linux + PostgreSQL + Redis 集成：待平台环境执行

在进入平台验收前，CI 必须从 `.[dev]` 安装 `pytest-asyncio`；否则顶层异步测试会被环境误报为失败。


必须在 `opsloop-multipod` 真实环境验证：

- 父子 run 不串线。
- collaborator allowlist 不能被模型参数绕过。
- 两个 Pod 使用同一 Redis coordination 时状态正确。
- HITL resume、取消、timeout 和清理闭合。
- ephemeral subagent 不创建 Team session。
- PostgreSQL 中既有 session/message/agent 数据合同不变。

### Durable 仅在明确启用时验证

SDK 的 SQLite/Redis durable 单元和真实 Redis 场景已通过；仍需在 Linux 多 Pod
环境验证 Redis run state、lease、fence、delivery 幂等、Pod 重启和 pending HITL
resume。

### Windows / K8s

Windows durable 跨进程的历史 access violation 仍需在目标 Windows CI 环境复现并
明确结论。当前开发容器没有 kubectl、容器运行时或 root，因此真实 K8s 沙箱和
两节点无粘性路由尚未验证，不能把本地进程测试当作 K8s 结果。

## 9. 发布阻断条件

当前仍命中的阻断条件主要是：OpsLoop 尚未完成真实接入、Linux PostgreSQL/Redis 集成无结果、双 Pod 无粘性接管未验证、真实 K8s 未验证、Windows access violation 无结论。SDK P1 修复和本地回归不再是当前阻断项。


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
