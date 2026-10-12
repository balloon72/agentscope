# TeamPipeline 子 Agent 功能

本分支基于官方 AgentScope 2.0.10（`v2.0.10`），使用官方 `TeamPipeline` 实现主 Agent 调用多个已存在 Agent 的能力。改造只增加配置、装配、会话持久化和前端配置页面，没有复制或修改官方 TeamPipeline 的调度算法。

## 使用方式

1. 启动 AgentScope Service 和官方 web_ui。
2. 创建两个或更多普通 Agent，并为每个 Agent 创建至少一个可用会话和模型配置。
3. 新建或编辑主 Agent，打开“TeamPipeline / 子 Agent”，搜索并选择多个已有 Agent。
4. 保存后，在主 Agent 会话中发送消息。主 Agent 和选中的成员会产生独立的回复事件，前端按回复 ID 展示各成员的过程。

配置通过 Agent API 的 `team_pipeline_config` 字段保存：

```json
{
  "enabled": true,
  "member_agent_ids": ["agent-a", "agent-b"]
}
```

启用时至少需要一个成员；成员 ID 不允许重复，也不能选择主 Agent 自身。成员必须属于当前用户，避免跨用户复用模型、MCP、Skill 或工作区。

## 资源继承规则

- 成员使用自己 Agent 配置中的 system prompt、上下文和 ReAct 配置。
- 成员使用自己最近的会话配置，包括模型、工作区、MCP 和 Skill。
- 成员的 TeamPipeline 配置不会递归展开，因此不会意外形成嵌套 TeamPipeline。
- 主 Agent 只保存成员会话的必要快照，成员仍然是独立 Agent。

## HITL 与会话恢复

成员触发工具审批或外部执行审批时，事件会沿主 Agent 会话流发布，继续请求使用成员自己的 `reply_id` 路由回 TeamPipeline。每轮完成后会把成员会话 ID、配置和 AgentState 快照写入主会话状态，因此普通的跨 HTTP 请求 HITL 可以继续。

## 部署边界

官方 TeamPipeline 是进程内 asyncio 调度器，不是分布式队列。当前改造支持同一服务进程内的多成员并行执行和 HTTP 续接，但不承诺以下场景：

- 任务执行中进程崩溃后自动恢复；
- 无粘性多 Pod 间迁移正在执行的成员任务；
- 多 Pod 共享同一进行中的 TeamPipeline 对象。

生产环境建议先使用单 Pod或为会话配置粘性路由；需要真正的无粘性接管时，应在外层增加可恢复任务队列/状态机，并把成员执行拆成独立持久化任务。

同一轮中某个成员失败时，TeamPipeline 会取消并等待其余成员任务完成清理，再把原始异常向上抛出。这样可以避免失败请求结束后仍遗留后台成员任务；该行为由回归测试 `tests/pipeline_team_cleanup_test.py` 覆盖。

## 免费模型验收配置

可以使用任何 OpenAI 兼容模型。无需付费的公开路由可选 OpenRouter 的 `openrouter/free`，但仍需要用户自己的 API Key，且免费模型受限流和可用性影响。示例：

```bash
export OPENAI_API_KEY="<your-openrouter-key>"
export OPENAI_BASE_URL="https://openrouter.ai/api/v1"
export MODEL_NAME="openrouter/free"
```

没有 API Key 时，使用项目已有 MockModel/本地模型完成自动化测试；不要把测试 Key 写入仓库或前端。

## 验收命令

```bash
python -m pytest -q tests/team_pipeline_config_test.py tests/pipeline_team_test.py
python -m compileall -q src
git diff --check
```

前端：

```bash
cd examples/web_ui/frontend
pnpm build
pnpm lint
```


## Response event ordering verification (2026-10-11)

The Agent response-event ordering fix was verified through the public `Agent.reply_stream` path with the repository's offline `MockModel`; no live LLM, browser, credentials, or deployment acceptance was involved. Coverage includes adjacent text blocks, a text/tool/text response with a real local `FunctionTool` round trip, and streaming deltas whose terminal snapshot is not replayed. The focused command passed all 40 tests:

```bash
.venv/bin/python -m pytest tests/agent_response_event_order_test.py tests/agent_basic_test.py tests/agent_structured_output_test.py tests/agent_interrupt_test.py -q
# 40 passed
```

A mutation check restoring the pre-fix converter caused both non-streaming end-to-end ordering tests to fail, confirming they exercise the regression path rather than only the helper directly.
