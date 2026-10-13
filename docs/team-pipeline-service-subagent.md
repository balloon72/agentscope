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

### PG + Redis 多节点验收边界

- PostgreSQL 负责持久化 Agent、Session、消息和状态；必须使用同一数据库，不能让每个 Pod 使用本地 SQLite。
- RedisMessageBus 负责会话锁、事件回放、Inbox 和唤醒队列；所有 Pod 必须连接同一 Redis 实例/集群，并使用一致的 DB、前缀和序列化配置。
- 本分支补强了 Redis 锁续租/释放的 compare-and-set：旧 Pod 在租约过期并被新 Pod 接管后，迟到的心跳不会续租或删除新 Pod 的锁。
- 双 Pod 无粘性请求可以接管已持久化的下一次请求；正在运行的 Python/asyncio TeamPipeline 不会跨 Pod 迁移。Pod 故障后的接管依赖 Inbox/唤醒重试，不能把“事件已发布”误认为“成员任务已迁移”。
- 当前自动化验证覆盖 fake Redis 和 SQL 层契约；执行真实 PG + Redis 双 Pod 验收前，需要在 Linux/Kubernetes 环境跑部署清单中的连接、锁抢占、SSE 重放、故障重启和 HITL 接管用例。

### HITL 接管流程（本分支已实现）

1. 成员产生 HITL 请求后，Service 在本次运行收尾阶段先持久化成员回复、成员状态和 leader 的 pipeline 快照，再释放 session lock。
2. 用户确认结果进入共享 Redis Inbox；任意 Pod 获取 leader session lock 后处理该续接请求，避免依赖原 Pod 或粘性会话。
3. 新 Pod 根据 leader snapshot 恢复成员会话配置、权限上下文、工作区和成员状态，再按原 reply id 续接 TeamPipeline。

该流程保证“已停在 HITL 的任务”可以跨 Pod 继续；它不等价于把正在执行模型调用中的 Python 协程迁移到另一个 Pod。
