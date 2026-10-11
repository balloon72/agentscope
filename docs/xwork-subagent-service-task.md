# xWork collaborator subagent：AgentScope SDK / service 改造任务

## 背景

xWork 的 `feature/agentscope-replace/opsloop-multipod` 已经在 OpsLoop 侧实现了 collaborator pool：Control Plane 返回当前父 Agent 允许使用的协作 Agent，并提供 `cp_agent_id`、`sdk_agent_id`、`owner_id`、发布状态和 `agent:run` 结果。

目标是让父专业智能体通过一次性 subagent 调用协作 Agent：

```text
parent agent
  -> call_subagent(agent_id, task)
  -> resolve configured collaborator
  -> create temporary worker
  -> stream / HITL / result
  -> release worker
```

普通协作任务不应创建 Agent Team、Team session 或用户可见的独立会话。Agent Team 仍然保留给持久成员、邀请、跨回合通信和长期任务。

## 当前分支已经具备的能力

本分支已有：

- `OneShotSubagent`：无 HITL 的一次性 worker。
- `SubagentSession`：基于 `reply_stream()` 的可暂停、可恢复 worker。
- `SubagentTool`：父 agent 工具、子事件、HITL resume、cancel 和并发 run。
- `DurableSubagentRunner`：SQLite/Redis lease、fence、幂等 delivery 和恢复。
- `SubAgentTemplate`：服务启动时注册静态子 agent 模板。

这些能力还没有表达“从应用提供的 collaborator pool 中按 Agent ID 创建 worker”。`SubAgentTemplate` 也不能替代 xWork 的 Agent/Revision 配置，因为它不是运行时 Agent registry。

## 本任务范围

只修改 AgentScope SDK / service 的通用能力。不要把 xWork 的数据库、Control Plane HTTP 客户端、租户业务规则或 AgentHub 表结构写进 AgentScope。

### 1. 增加 collaborator resolver 接缝

为 `SubagentTool` 增加可选的运行时 resolver，支持应用按受信的 collaborator ID 返回一个新的 worker factory。

建议接口：

```python
CollaboratorResolver = Callable[
    [str],
    Callable[[], Agent],
]
```

建议构造参数：

```python
SubagentTool(
    default_agent_factory,
    agent_resolver=resolver,
    allowed_agent_ids={"researcher": "...", "coder": "..."},
)
```

要求：

- `agent_id` 出现在工具 input schema 中。
- schema 可以暴露 allowlist enum，但服务端执行时必须再次校验。
- 不允许模型传递或覆盖 system prompt、model、toolkit、权限或 revision。
- 没有 resolver 时保持现有 `SubagentTool(agent_factory)` 兼容行为。
- resolver 异常转换成结构化 subagent error，不泄露内部堆栈。
- 结果中回传实际使用的 `agent_id`，便于父层审计和事件关联。

### 2. 增加显式子任务执行上下文

新增通用、无业务含义的上下文对象，例如：

```python
@dataclass(frozen=True)
class SubagentExecutionContext:
    parent_run_id: str
    subagent_run_id: str
    principal: Any | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
```

SDK 只负责携带和关联上下文，不解释 tenant、workspace 或 xWork 权限字段。

上下文必须贯穿：

- resolver 调用
- worker factory
- 子事件
- HITL resume
- cancel
- 结果和错误

如果为了兼容现有 API 保留 `Callable[[str], Callable[[], Agent]]`，应同时提供带 context 的扩展接口，不要把身份信息放入全局变量。

### 3. 标准化子任务状态和结果

建议统一状态：

```text
CREATED
RUNNING
WAITING_CONFIRMATION
WAITING_EXTERNAL_EXECUTION
COMPLETED
FAILED
CANCELLED
TIMEOUT
```

结果至少包含：

```json
{
  "status": "completed",
  "run_id": "...",
  "agent_id": "...",
  "result": "..."
}
```

失败结果必须有稳定 code、safe message 和 retryable；不得把异常堆栈、密钥、文件路径或 provider 原始错误直接交给模型。

### 4. 事件和 HITL 路由必须可关联

`SubagentEvent` 至少要能关联：

- parent run id
- subagent run id
- selected agent id
- child reply id
- event type

resume 前必须校验：

- run id 匹配
- reply id 匹配
- pending event 类型匹配
- 当前仍处于 waiting 状态
- 重复 resume 幂等拒绝

错误 resume 必须保留原 pending continuation，允许调用方重试。

### 5. 生命周期和自动清理

默认 subagent 是 ephemeral：

- completed、failed、cancelled、timeout 都必须关闭 worker 和 stream。
- 父任务取消时级联取消子任务。
- HITL 等待必须有 TTL。
- queue、future、event subscription 和 run registry 不能永久保留。
- `discard()` 可以保留作为显式 API，但正常完成不能依赖调用方记得执行它。
- 清理失败不能覆盖原始结果，但需要日志和 metrics。

注意：如果为了支持事件消费而延迟删除 run，必须定义明确的消费完成标记或 TTL，不能无限保留 `_runs`。

### 6. 递归、并发和资源限制

提供通用配置：

- 最大递归深度，默认建议为 1。
- 每个父 run 最大并发子任务数。
- 单个 subagent timeout。
- 最大 task 输入长度。
- 最大结果长度。
- 最大事件队列长度。

第一期不要求子 agent 再创建子 agent；SDK 应能通过 context 或 policy 明确禁止递归。

### 7. ephemeral 与 durable 明确分离

保留 `DurableSubagentRunner`，但不要让普通 `SubagentTool` 自动写入 durable store。

要求：

- ephemeral 是默认路径，完成后释放运行态。
- durable 由应用显式选择。
- durable 的状态恢复、GC、容量、版本迁移由 Durable runner 自己负责。
- 两种模式共享状态和事件语义，避免父层写两套 resume 逻辑。

### 8. service 层注册和注入接缝

AgentScope service 应提供应用注册 collaborator subagent tool 的清晰入口，例如：

```python
create_app(
    extra_agent_tools=..., 
    subagent_resolver=..., 
)
```

或允许应用在 per-session tool factory 中直接构造 `SubagentTool`。

要求：

- resolver 可以按 session / parent run 创建，不能作为跨用户共享的可变 Agent 实例。
- 不把 xWork 的 CP client、数据库模型或业务权限判断内置进 SDK。
- service 不应把 ephemeral subagent 暴露为 Team CRUD 或用户 session 列表。
- worker 可以使用应用注入的 toolkit、middleware、workspace 和 permission context。

## 不应在 AgentScope 中实现的内容

以下属于 xWork Control Plane / OpsLoop 适配层：

- `cp_agent_id`、`sdk_agent_id`、`owner_id` 的业务含义。
- collaborator 配置表和 Agent revision 快照。
- tenant、Workspace、AgentHub RBAC、`agent:run` 检查。
- Control Plane HTTP API 和服务 token。
- xWork 的 agent_runs、agent_run_steps、审计和配额表。
- 前端协作 agent 下拉框。
- Team lifecycle 和 AgentInvite 治理。

SDK 只提供 allowlist、resolver、context、事件、HITL 和生命周期接缝。

## 测试要求

### 单元测试

- 无 resolver 的旧调用兼容。
- resolver + allowlist 生成正确 schema。
- 未授权 agent id 被拒绝。
- resolver 异常得到结构化错误。
- 结果包含 agent id 和 run id。
- 子 agent 正常完成后 worker 关闭。
- failed、cancelled、timeout 后 worker 关闭。
- 错误 reply id 不会消耗 pending continuation。
- 错误 HITL event type 可重试。
- 重复 resume 幂等拒绝。
- run registry 有限生命周期。
- 并发和递归限制生效。

### 集成测试

- 父 Agent 通过 `SubagentTool` 选择两个不同 collaborator。
- 两个并发 child 的事件和 resume 不串线。
- 子 agent 的 HITL 事件可以暂停和恢复。
- 父取消可以关闭所有 child。
- ephemeral 模式不触发 Team storage。
- durable 模式可以复用相同的状态和事件契约。

## 验收标准

- `SubagentTool` 能按应用提供的 allowlist 选择不同 worker。
- AgentScope 不需要知道 xWork 的业务数据库和权限模型。
- 父子事件、HITL、取消和错误都能稳定关联。
- ephemeral worker 在 terminal 状态后自动释放。
- 现有静态 factory、OneShotSubagent、DurableSubagentRunner 和 Agent Team API 保持兼容。
- 测试覆盖正常、失败、并发、HITL、取消和清理路径。

