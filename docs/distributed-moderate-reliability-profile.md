# 分布式中等可靠性部署改造说明

## 目标

本项目需要支持多 API Pod 和较大的用户量，但当前不是金融、交易或强一致控制面，不要求跨地域容灾、Exactly Once 或运行中的 Python 协程跨 Pod 迁移。因此本阶段采用“分布式基础保留、极端故障处理适度”的方案。

## 必须保留的分布式能力

| 能力 | 保留原因 | 当前实现 |
| --- | --- | --- |
| PostgreSQL 共享状态 | 多 Pod 必须看到同一份会话、Agent 和状态 | SQL Storage 与 session 状态表 |
| Redis MessageBus | 跨 Pod 事件、Inbox、唤醒和锁协调 | Redis MessageBus |
| Session Lease | 避免同一会话被多个 Pod 同时执行 | 基本租约与状态归属校验 |
| Workspace 隔离 | 避免用户和会话之间串空间 | K8s Workspace 元数据和生命周期 |
| TeamPipeline 基本状态同步 | 保障成员回复和 HITL 结果可以被其他 Pod 继续处理 | Service 层快照和消息总线 |
| 有界重连和重试 | Redis 短暂断连时不立即失败，也不无限重试 | Dispatcher 重连和有限重试 |
| Gateway 启动互斥 | 共享 PVC 下避免两个 Pod 同时重启同一 Gateway | 共享目录锁和健康复查 |

## 本阶段不继续扩展的能力

以下能力保留已有安全保护，但不再新增更复杂的恢复框架：

1. 跨地域容灾和自动故障转移。
2. 外部副作用的 Exactly Once 语义；外部系统仍需幂等键或人工复核。
3. 正在运行的模型调用或 TeamPipeline 协程跨 Pod 迁移。
4. 极端网络分区下的全链路自动恢复。
5. 为每一种罕见取消顺序增加独立状态机。
6. WebRTC、旧 Agent Team、RedisStorage 扩展和官方示例前端。

## 取舍原则

- Lease/Fencing 只用于防止多 Pod 同时处理同一会话，不把它扩展成全系统事务协议。
- Wakeup 采用有界重试和可观测失败；不承诺无限恢复或未知外部结果自动重放。
- K8s Workspace 保证创建、归属校验、释放和超时清理；Pod 被强制删除后的复杂重建留给运维重试和后续版本。
- TeamPipeline 保证已持久化的成员状态和 HITL 请求可被其他 Pod 接续；不声称迁移正在执行的成员协程。

## 当前代码处理结论

本轮不删除已经用于多 Pod 正确性的 PG、Redis、Session Lease、Workspace 隔离和 TeamPipeline 基础链路。删除这些内容会把系统退化为“多 Pod 外壳、单节点状态”，风险高于收益。

当前新增的 Gateway 健康复用、跨进程启动锁、断连重连属于低成本且直接降低线上故障的保护，继续保留。后续若要进一步减量，应优先通过配置关闭高级恢复扫描，而不是删除基础 fencing 或共享状态代码。

### Wakeup 周期恢复开关

默认情况下，API 进程只执行启动时的唤醒账本恢复和正常队列消费，不启动周期性 claim 扫描。这样可以避免每个 API Pod 都长期运行后台恢复任务，适合本项目的中等可靠性目标。

需要更强的进程内故障恢复时，在所有 API Pod 上统一设置：

```text
AGENTSCOPE_WAKEUP_PERIODIC_RECOVERY=true
```

该开关只影响周期性恢复，不影响 Redis 队列、Session Lease、启动恢复、claim token 或正常重连。

## 验收边界

上线前只要求验证：多 Pod 请求分摊、会话不串、Redis/PG 连接、基本租约接管、Workspace 隔离、TeamPipeline 基本 HITL 接续和 Redis 短暂断连恢复。跨地域、强故障注入、Exactly Once 和大规模极限压测不作为本阶段上线阻断。

## 改造范围与代码量基线

以下数字均以官方 `v2.0.10` 标签为基线，便于后续审查和回滚：

- 正式代码：30 个文件，新增 4,912 行、删除 1,034 行，净增加 3,878 行。
- 测试代码：18 个文件，新增 4,304 行、删除 3 行，净增加 4,301 行。
- 文档和脚本：5 个文件，新增 221 行。
- 总计：53 个文件，新增 9,437 行、删除 1,037 行。

本轮中等可靠性调整只新增一个可配置的降级点：默认关闭 Wakeup claim 的周期性后台扫描，保留启动恢复、队列消费、Session Lease、claim token 和正常重连；需要更强恢复时通过 `AGENTSCOPE_WAKEUP_PERIODIC_RECOVERY=true` 开启。

以下项目本轮明确不删除，原因是它们属于多 Pod 正确性基础：

- Redis MessageBus、Session Lease 和 PG 状态归属。
- K8s Workspace 的 epoch/身份校验和条件删除。
- TeamPipeline 成员快照、HITL 状态同步和跨 Pod 接续。
- Gateway 健康复用、启动互斥和 CancelDispatcher 重连。

以下项目暂缓进一步扩展，而不是直接删除：

- Wakeup 的复杂周期恢复和极端 claim 接管。
- K8s Workspace 关闭竞态的完整状态机。
- 旧 Storage/MessageBus 实现的兼容别名。
- TeamPipeline 成员重命名、重排等兼容路径。
