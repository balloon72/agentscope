# AgentScope 2.0.8 Subagent 实现与验证

## 范围

本分支以官方 `v2.0.8`（`c7d8e31b`）为基线，增加父 Agent 调用一次性或可恢复子 Agent 的 SDK 能力：

- `OneShotSubagent`：一次任务、独立 worker、返回结构化结果。
- `SubagentSession`：父端持有 worker 会话，支持 `RequireUserConfirmEvent` / `RequireExternalExecutionEvent` 暂停和结果事件恢复。
- `SubagentTool`：把会话注册为父 Agent 工具，提供带 `run_id` 的事件流和 HITL 路由。
- `create_subagent_tool_factory`：适配 `create_app(extra_agent_tools=...)` 的 agent resolver、allowlist 和执行上下文。
- `DurableSubagentRunner`：SQLite WAL 和 Redis Lua/CAS 存储，支持租约、fence、delivery-id 幂等、跨进程恢复和 HITL。
- `SubagentExecutionContext`：父/子 run ID、principal、metadata 的不可变绑定；持久化前强制 JSON 校验。

## 使用边界

`SubagentTool` 是进程内桥接。AgentScope Service 每个 HTTP turn 重新装配 `extra_agent_tools` 时，不能依靠它保存跨请求或跨 Pod 的 pending worker。需要跨 turn、无粘性负载均衡或 Pod 故障接管时，使用 `DurableSubagentRunner`，并由应用层保存 factory 注册表、事件投影和 HITL 路由。

`DurableSubagentRunner` 持久化 AgentState、pending HITL、delivery ID 和 ownership fence，但任意外部副作用仍需使用 delivery ID 做幂等；它不宣称把非事务性外部调用变成 exactly-once。

子 Agent 默认不会自动继承父 Agent 的 MCP、Skill、沙箱或权限。resolver/factory 必须显式构造目标 Agent，并把已批准的 workspace、sandbox、MCP、Skill 和权限上下文传入。`agent_id` allowlist 只负责路由约束，不替代宿主 RBAC。

## 关键兼容性处理

2.0.8 的 `Agent.reply_stream` 默认不产出最终 `Msg`。本实现对支持该参数的 worker 使用 `yield_final_msg=True`，对旧式自定义 worker 通过签名检测保持兼容，因此父端事件流和工具结果均能获得最终文本。

子 Agent HITL 结果必须携带匹配的 `reply_id`，并且 confirmation 与 external-execution 事件不能混用。取消、超时、resolver 失败、并发槽位释放和 worker 实例复用均有明确清理路径。

## 验证记录

在 Linux 工作区使用 Python 3.12、项目虚拟环境完成：

- Subagent 专项测试：54 passed。
- durable SQLite、跨进程恢复、HITL 相关测试：10 passed。
- Agent/Tool/Service 关键回归测试：139 passed。
- 真实 Redis 6/7 兼容服务（TCP localhost）：完成 start → HITL waiting → resume → completed、context 校验和 delivery-id 重复投递验证。
- `compileall` 和 `git diff --check` 通过。

未验证：真实 Kubernetes 双 Pod、真实 PostgreSQL 适配（本分支没有 PostgreSQL durable store）、外部 LLM 网络调用。生产部署前应先完成应用层事件投影、持久 factory 注册、RBAC/沙箱策略和双 Pod 故障演练。
