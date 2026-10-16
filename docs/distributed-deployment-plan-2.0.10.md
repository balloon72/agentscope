# AgentScope 2.0.10 分布式实现计划与交付边界

## 官方现状

AgentScope 2.0.10 的 Service 分布式能力仍标为 WIP。官方建议共享 Redis Storage/MessageBus；PostgreSQL 可作为共享 SQL 持久化。Redis 队列、锁和 Pub/Sub 不能单独作为最终事实源，队列消费后进程崩溃会丢失待处理唤醒。Local/Docker workspace 不适合跨 Pod，生产应使用 K8s/OpenSandbox/Daytona 等可重新绑定的沙箱。

参考：

- https://github.com/agentscope-ai/agentscope/issues/1722
- https://docs.agentscope.io/en/versions/2.0.10/deploy/agent-service
- https://docs.agentscope.io/en/versions/2.0.10/deploy/workspace-manager
- https://github.com/agentscope-ai/agentscope/issues/2227
- https://github.com/agentscope-ai/agentscope/pull/2476

## 本分支实现

1. PostgreSQL/Redis session lease fencing：`lease_epoch` 递增，状态写入要求 owner + epoch 条件，旧 Pod 迟到写入被拒绝。
2. HITL/运行状态恢复：session/member snapshot 继续使用 Service 的持久化状态；Redis 唤醒增加 durable trigger ledger，启动时可从 ledger 恢复 queue drain 后崩溃的任务，重复 resume 幂等。
3. K8s workspace fencing：Pod/PVC metadata 包含 tenant/session/run/epoch，旧 epoch 不允许复用 workspace；多节点拒绝 Local/Docker/InMemory workspace。
4. 多节点配置护栏：显式共享 download secret；API、scheduler、index、channel worker 角色互斥，避免每个副本重复消费副作用。
5. Service chat 在获取 Redis session lock 后再 claim durable session lease，并使用 fenced state update；单节点和自定义旧 Storage 仍保持兼容路径。

## 验证边界

- 单元/集成：Redis/SQL storage fencing、wake ledger、K8s metadata、worker role guardrails。
- 已执行本地回归：144 项通过，含 SQLite/Redis storage、wake recovery、K8s guardrail 与 fencing 相关测试。
- 真实 PG/Redis 双进程验收仍需在持久环境重新执行；当前临时工作区曾完成过 2 项真实验收，但提交前工作区被回收，不能把旧结果当作本次分支的最终证据。
- K8s Pod eviction、无粘性负载、PVC 重挂载和真实模型调用必须在目标集群执行；本分支提供 metadata/fencing 代码和测试，不声称本地无集群验证已覆盖。

## 运行建议

- API 副本：RedisMessageBus + PostgreSQL，关闭 scheduler/index/channel worker。
- Scheduler：只启一个副本；Index/Channel 使用独立 worker Deployment。
- 所有副本使用同一 Redis、PG、download secret 和 K8s workspace 配置。
- 生产入口替换默认 X-User-ID，增加 JWT/RBAC；Redis 锁只做协调，PG epoch 才是最终 fencing。
- 外部写副作用仍需 provider 幂等 key 或 outbox；模型/工具调用中途崩溃无法迁移 Python 协程，只能从安全 snapshot 重建。
