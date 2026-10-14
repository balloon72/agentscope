# Web UI Ant Design X 集成

## 范围

当前分支基于 `service-team-pipeline-subagent-2.0.10`，在官方 Web UI 中接入 Ant Design X 2.9.0：

- `@ant-design/x` 2.9.0 和 `antd` 6.x 作为前端依赖。
- 根组件使用 `XProvider`，统一管理 Ant Design X 的样式上下文。
- 普通文本消息使用 X 的 `Bubble`，流式回复通过 `streaming` 状态驱动。
- Bubble 内容颜色继承现有消息容器，兼容 Web UI 的浅色和深色主题。
- TeamPipeline 的成员消息、工具调用、附件、HITL 确认卡仍保留现有渲染器，避免丢失 AgentScope 专有事件字段和审批交互。

## 为什么没有直接使用 X SDK

AgentScope Service 的 SSE 消息不是 OpenAI Chat Completions 格式，且包含工具调用、工具结果、思考块、附件和 HITL 事件。当前版本先采用 Ant Design X 的展示组件，不强行套用 `@ant-design/x-sdk` 的 `useXChat`，避免重复解析、丢失事件顺序或改变分布式 HITL 恢复语义。后续如果统一成 X SDK，需要新增自定义 ChatProvider，并为每种 Service 事件定义可逆的 transform。

## 本地验证

在 `examples/web_ui` 目录执行：

```bash
pnpm install
pnpm --filter frontend build
pnpm --filter frontend lint
```

也可以只执行前端回归：

```bash
cd frontend
node --test tests/ant-design-x.test.mjs tests/team-messages.test.mjs tests/team-pipeline.test.mjs
```

页面验收重点：

1. 普通用户消息和主 Agent 文本回复显示为 Ant Design X Bubble。
2. 正在生成的主 Agent 回复继续显示流式状态，完成后稳定收尾。
3. 子 Agent 消息仍在主 Agent 回复范围内折叠展示。
4. 工具调用、附件、HITL 确认和拒绝流程保持原有行为。

## 版本依据

Ant Design X 2.9.0 的 peer dependency 是 Ant Design 6.x。升级 X 时需要同时检查 `antd` 主版本和官方迁移说明，不能只替换 `@ant-design/x`。
