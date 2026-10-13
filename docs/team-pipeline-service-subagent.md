# AgentScope Service 层 TeamPipeline Subagent

本分支基于官方 AgentScope `v2.0.10`，Subagent 编排放在 AgentScope Service 层完成。SDK 源码未修改，TeamPipeline 仍使用官方实现。

## 实现边界

- Service 从主 Agent 的 `team_pipeline_config` 读取已存在 Agent ID。
- Service 为每个成员加载自己的最近会话配置：模型、fallback 模型、工作区、MCP/Skill 工具上下文和权限策略。
- 成员回复事件通过主会话的消息总线发布，前端按 `reply_id` 归并到主 Agent 回复下并折叠展示。
- 成员 HITL/外部执行继续使用成员自己的 `reply_id`，HTTP 续接由 Service 恢复成员状态。
- 成员状态快照存入主会话状态；成员名称用于快照匹配，避免依赖 SDK 扩展字段。
- 成员名称重复、自选主 Agent、跨用户成员、无会话、无模型等情况在资源创建前返回 422。

## SDK 改动记录

本分支相对官方 v2.0.10 没有修改 `src/agentscope/agent`、`src/agentscope/pipeline` 或其他 SDK 算法代码。只修改了 `src/agentscope/app` 的 Service、路由和存储模型，以及 Web UI 和测试。

因此升级 SDK 时，Service 侧改动可以独立审查；如果未来官方 TeamPipeline 事件协议发生变化，只需检查 Service 的事件投影和前端 `reply_id` 归并逻辑。

## 已知边界

TeamPipeline 本身仍是进程内 asyncio 调度器。此分支不承诺正在运行的成员任务跨 Pod 迁移；多副本部署应使用官方 Workspace/消息总线方案并保证请求路由和状态存储一致。
