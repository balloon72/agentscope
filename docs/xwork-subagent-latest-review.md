# xWork subagent latest review

复核基线：`codex/subagent-v2.0.9`，最新提交 `d86daff3`。

## 已验证

定向测试：

```text
15 passed：collaborator acceptance、execution context、factory、limits、tool bridge
```

完整 `tests/subagent`：

```text
32 passed
```

Windows durable 跨进程测试仍输出 Python `multiprocessing.popen_spawn_win32.py` 的 `access violation`，虽然 pytest 最终退出码为 0。必须在 Linux CI 和真实 PostgreSQL + Redis 环境继续验证。

## 仍未完成的 SDK 问题

### P1：per-session context 仍被丢弃

`src/agentscope/tool/_subagent_factory.py` 的 `create_subagent_tool_factory()` 仍然删除 `user_id`、父 `agent_id` 和 `session_id`，resolver 只能收到 collaborator `agent_id`。

xWork 无法仅靠这个 helper 得到当前父会话的 user、tenant、workspace、parent run 和权限上下文。必须改为 per-session resolver wrapper，或者提供明确的 context factory，并增加两个并发 session 不串身份的测试。

### P1：普通执行异常仍泄露原始错误

`src/agentscope/tool/_subagent_tool.py` 的通用异常路径仍返回：

```python
f"{type(exc).__name__}: {exc}"
```

provider、MCP、workspace 或内部路径信息可能进入父 Agent 上下文。应只把完整 traceback 写日志，对调用方返回稳定错误码和安全文案。

## xWork 接入状态

远端 `feature/agentscope-replace/opsloop-multipod` 当前仍未接入：

- `SubagentTool`
- `create_subagent_tool_factory`
- collaborator resolver
- parent/subagent event bridge
- subagent HITL resume

该分支现有的 `xspark_subagent_delta`、`team_stream_projector` 属于 Agent Team worker 的历史投影，不等于 ephemeral subagent 已接通。

因此当前结论是：**AgentScope SDK 的 subagent 基础能力和 durable HITL 防护已经基本具备，但 xWork 专业智能体协作功能尚未完成端到端接入。**

## 发布阻断

- Linux CI 的 ephemeral subagent 测试未通过前不能发布。
- PostgreSQL + Redis 的真实 OpsLoop 父子 run、HITL、取消和清理未验证前不能发布。
- `create_subagent_tool_factory` 未传递父会话上下文前不能接入生产 resolver。
- 原始异常仍能进入父 Agent 结果前不能发布。
- 不得把 SQLite durable runner 作为当前 `opsloop-multipod` 生产依赖；当前实际路径是 PostgreSQL + Redis。

