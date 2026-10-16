# 临时真实 PG + Redis 联调

`scripts/run_real_pg_redis_acceptance.sh` 会把 PostgreSQL、Redis 运行时下载到 `/tmp/asvenv`，用测试 fixture 创建临时数据目录和随机端口，运行结束自动停止并清理进程。

```bash
./scripts/run_real_pg_redis_acceptance.sh
```

这是真实 PostgreSQL/Redis，不是 SQLite/fakeredis。测试覆盖双 HTTP worker、HITL 请求跨进程接管、PG 租约和 Redis 锁/唤醒。Kubernetes Pod 驱逐与 PVC 重建仍需在目标集群验收。
