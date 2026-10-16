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
2. HITL/运行状态恢复：session/member snapshot 继续使用 Service 的持久化状态；Redis 唤醒增加 durable trigger ledger。启动时仅恢复尚未执行的 queued 记录；执行中进程崩溃导致结果未知的 claimed 记录不会自动重放，而是标记为 `manual_review`（显式失败结果标记为 `failed_unknown`），避免外部写操作重复。只读或明确幂等的生产者可通过 `recovery_policy="replay_safe"` 选择恢复；这不是 Exactly Once 保证。
3. K8s workspace fencing：Pod/PVC metadata 包含 tenant/session/run/epoch，旧 epoch 不允许复用 workspace；多节点拒绝 Local/Docker/InMemory workspace。
4. 多节点配置护栏：显式共享 download secret；API、scheduler、index、channel worker 角色互斥，避免每个副本重复消费副作用。
5. Service chat 在获取 Redis session lock 后再 claim durable session lease，并使用 fenced state update；单节点和自定义旧 Storage 仍保持兼容路径。
6. Workspace 初始化按 workspace ID 去重：缓存锁只保护 map，K8s Pod/PVC 初始化在锁外执行，不同 workspace 可并发；关闭 manager 时会取消并等待未完成初始化，避免遗留沙箱。
7. WakeupDispatcher 增加订阅断连自动重连、启动 ready 超时、断连后 durable queue 补扫；队列按 64 条批次持续 drain，进程内 dispatch 并发上限为 16。
8. Trigger claim 增加定期过期恢复和执行期间续租；恢复循环对单次 Redis 异常隔离，claim token 继续用于完成状态 fencing。
9. Trigger 结果分类：进入 Agent/TeamPipeline 执行后的业务错误或未知结果标记为 `failed_unknown`，不再自动重放；执行前只对 `CONNECTION`、`UPSTREAM`、`RATE_LIMIT` 做最多 3 次指数退避重试，配置/权限/请求错误直接终止。续租丢失时取消旧 Chat run，避免继续发出外部写操作。
10. K8s Pod/PVC 创建遇到 409 AlreadyExists 时重新读取并校验 workspace ID、epoch 和身份标签，匹配才复用，否则 fail-closed。
11. Dispatcher 使用每次运行专属的 outcome token，避免直接 HTTP ChatService 运行结果被错误消费；损坏的 ledger 记录会终态丢弃，避免周期恢复无限重放。
12. Workspace 初始化失败、取消和 manager 关闭都会清理已创建的 workspace；namespace/PVC/Pod 的并发创建冲突按身份校验后复用。

## 验证边界

- 单元/集成：Redis/SQL storage fencing、wake ledger、K8s metadata、worker role guardrails。
- 已执行核心回归：202 项通过，另有 4 个 subtests 通过；覆盖 dispatcher outcome/claim fencing、损坏 ledger、有限重试、K8s 创建竞态和 workspace 关闭测试。
- 本轮真实 PG 16 + Redis 双 worker/HITL 验收：2 项通过（`scripts/run_real_pg_redis_acceptance.sh`）。
- K8s Pod eviction、无粘性负载、PVC 重挂载和真实模型调用必须在目标集群执行；本分支提供 metadata/fencing 代码和测试，不声称本地无集群验证已覆盖。

## 运行建议

- API 副本：RedisMessageBus + PostgreSQL，关闭 scheduler/index/channel worker。
- Scheduler：只启一个副本；Index/Channel 使用独立 worker Deployment。
- 所有副本使用同一 Redis、PG、download secret 和 K8s workspace 配置。
- 生产入口替换默认 X-User-ID，增加 JWT/RBAC；Redis 锁只做协调，PG epoch 才是最终 fencing。
- 外部写副作用仍需 provider 幂等 key 或 outbox；模型/工具调用中途崩溃无法迁移 Python 协程，只能从安全 snapshot 重建。
