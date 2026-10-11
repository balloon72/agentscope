# xWork subagent follow-up fixes

基于 `codex/subagent-v2.0.9` 最新实现和 `opsloop-multipod` 的实际接入方式，以下问题需要在下一轮修改中处理。

## P1：`create_subagent_tool_factory` 必须保留 per-session 上下文

文件：`src/agentscope/tool/_subagent_factory.py`

当前 `factory(user_id, agent_id, session_id)` 收到父会话参数后直接丢弃：

```python
del user_id, agent_id, session_id
return [SubagentTool(agent_resolver=agent_resolver, **tool_kwargs)]
```

这使 resolver 实际只能收到 `agent_id`，无法知道当前父会话的 user、parent agent、session、tenant、workspace 或 parent run。

这会阻塞 xWork 的真实接入：OpsLoop 需要依据父会话调用 collaborator pool，并用 `sdk_agent_id + owner_id` 解析正确的目标 Agent、Workspace 和权限。

要求：

- 每次 `extra_agent_tools` 调用创建独立的 resolver wrapper。
- wrapper 必须保留 `user_id`、父 `agent_id` 和 `session_id`。
- resolver 可以接收 `agent_id + context`，或者由 wrapper 闭包传入父会话上下文。
- 不使用跨 session 的可变全局上下文。
- 增加测试，验证两个 session 使用同一个 resolver 时不会串用身份。

## P1：普通执行异常不得把原始异常返回给父 Agent

文件：`src/agentscope/tool/_subagent_tool.py`

当前 `_execute()` 的通用异常路径仍然把异常字符串返回给父 Agent：

```python
"error": f"{type(exc).__name__}: {exc}"
```

异常可能包含 provider 响应、内部路径、MCP 返回内容、workspace 信息或敏感参数。

要求：

- 完整异常只写日志，日志保留 traceback。
- 返回稳定的 `error_code`。
- 返回固定的安全 message。
- 取消、超时和 resolver 拒绝继续保留明确状态。
- 增加测试，确认异常内容不会出现在 `SubagentTool.run()` 或 tool result 中。

建议错误结构：

```json
{
  "status": "error",
  "error_code": "subagent_execution_failed",
  "error": "subagent execution failed",
  "retryable": true
}
```

## P2：明确 resolver 返回值的生命周期合同

文件：`src/agentscope/tool/_subagent_tool.py`

当前 resolver 既可以返回 Agent 实例，也可以返回 factory。返回 Agent 实例时，代码会把这个实例包装成 factory。如果应用缓存了该实例，不同 run 会共享 AgentState、middleware 状态、workspace 句柄和 pending HITL 状态。

要求二选一：

1. resolver 只允许返回 `Callable[[], Agent]`，每次 run 创建全新的 Agent；或
2. 保留实例返回兼容，但明确要求实例必须由 resolver 为当前 run 新建，并增加防复用测试或运行时保护。

文档、类型标注和测试必须采用同一个合同。

## P2：收紧 durable context 的 JSON 合同

文件：`src/agentscope/_execution_context.py`、`src/agentscope/agent/_durable_subagent.py`

`SubagentExecutionContext.metadata` 和 `principal` 当前允许 `Any`，但 SQLite/Redis durable store 直接执行 `json.dumps(context.to_dict())`。不可序列化的 principal 或 metadata 会在 durable start/commit 期间失败。

要求二选一：

- context 只接受 JSON primitive、dict 和 list，并在构造时 fail fast；或
- 增加明确的 context serializer，持久化前拒绝不可序列化值并返回稳定错误。

不能同时声明任意对象可用，又直接假定它可 JSON 序列化。

增加 SQLite 和 Redis 两条路径的不可序列化 context 测试。

## 验证要求：Windows 跨进程测试

定向测试已经通过：

```text
11 passed：collaborator / context / factory / limits
27 passed：tests/subagent
```

但 Windows durable 跨进程测试期间出现 Python multiprocessing 的：

```text
Windows fatal exception: access violation
```

pytest 最终退出码为 0，但不能把这当成跨进程验证完成。

要求：

- 在 Linux CI 或目标部署环境重新运行 durable 跨进程测试。
- 记录实际测试命令和结果。
- 如果 Windows 专属问题是测试启动方式导致，补充兼容处理或明确标注测试环境限制。

## 完成标准

- per-session resolver 能拿到正确父会话上下文。
- 两个并发 session 的 collaborator 身份不会互串。
- 父 Agent 看不到原始异常内容。
- 每个 subagent run 使用独立 worker 生命周期。
- durable context 的序列化合同明确且经过 SQLite/Redis 测试。
- Linux CI 或目标环境的跨进程 durable 测试通过。

## 发布前验收门槛

### Linux CI：SQLite 单机跨进程

必须在 Linux runner 上执行：

```bash
pytest tests/subagent/test_durable_subagent.py -q
pytest tests/subagent_collaborator_acceptance_test.py \
       tests/subagent_execution_context_test.py \
       tests/subagent_factory_test.py \
       tests/subagent_limits_test.py -q
```

要求：

- 所有测试通过。
- `spawn` 跨进程启动、状态恢复、delivery 幂等和 fence 测试通过。
- 记录 Python、操作系统和 pytest 版本。
- 不能只使用 Windows 的 0 exit code 作为替代证据。

### Linux CI / 集成环境：Redis 多 Pod

`opsloop-multipod` 的 durable 运行必须使用 `RedisSubagentStore`。必须验证：

- 两个 Linux worker 同时 claim 同一个 `run_id`，只有一个获得 lease。
- lease 到期后另一个 worker 可以接管。
- 旧 worker 的 late commit 被 fence 拒绝。
- 相同 delivery id 不会重复执行。
- Pod 重启后可以读取 pending HITL 并 resume。
- completed、failed、cancelled 和过期 run 按策略清理。
- Redis key 使用明确的环境/租户前缀，不与其他业务 key 冲突。

多 Pod 验收不能用共享 PVC 上的 SQLite 替代 Redis。SQLite 只允许作为单机、多进程测试存储；不能把 WAL 文件放在 NFS、对象存储挂载或多个 Pod 共同写入的目录。

### xWork OpsLoop 集成

必须在真实 Linux OpsLoop 部署中验证：

- 父会话的 user、agent、session、tenant 和 workspace 上下文传到 resolver。
- 两个并发父会话不会互用 collaborator 身份或 workspace。
- collaborator 只来自 CP allowlist，不能通过模型参数绕过。
- 子 agent 完成、失败、取消、超时后 worker 和临时运行态被释放。
- HITL resume 使用正确的 parent run、subagent run 和 reply id。
- ephemeral subagent 不创建 Agent Team 或用户可见独立会话。
- 子事件可以回到父 run 的 SSE / timeline。

### Windows 验证

Windows spawn 测试出现 `access violation` 时，不能标记为已验证。必须完成以下任一项：

- 修复测试启动方式，使同一测试在 Windows 稳定通过；或
- 将 Windows 问题作为明确的环境 blocker 记录，并确保 Linux CI 和生产 Linux 验证完整通过。

即使 Windows 测试最终被标记为环境限制，也不能降低 Linux CI、Redis 多 Pod 和真实 OpsLoop 集成门槛。

### 发布阻断条件

以下任一条件未满足，都不能宣布 subagent 已经适合 `opsloop-multipod`：

- Linux durable 跨进程测试失败或没有结果。
- Redis 多 Pod lease / fence / resume 没有验证。
- 生产路径仍使用共享 PVC SQLite。
- resolver 上下文没有经过真实父会话验证。
- HITL、取消或清理存在未闭合运行态。
- Windows access violation 被静默忽略，没有记录环境结论。
