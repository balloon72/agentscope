# Web UI Ant Design X 集成

## 范围

当前分支基于 `service-team-pipeline-subagent-2.0.10`，在官方 Web UI 中接入 Ant Design X 2.9.0：

- `@ant-design/x` 2.9.0 和 `antd` 6.x 作为前端依赖。
- 根组件使用 `XProvider`，统一管理 Ant Design X 的样式上下文。
- 普通文本消息使用 X 的 `Bubble`，流式回复通过 `streaming` 状态驱动。
- Bubble 内容颜色继承现有消息容器，兼容 Web UI 的浅色和深色主题。
- TeamPipeline 的成员分组、工具调用、附件、HITL 确认卡仍保留现有控制逻辑；只有纯文本块交给 X Bubble 展示，避免丢失 AgentScope 专有事件字段和审批交互。

## 机器人浮动入口

`AppLayout` 全局挂载 `RobotAssistant`：

- 右下角使用 Ant Design `FloatButton`（机器人图标），键盘和屏幕阅读器可通过 `aria-label` 访问。
- 点击后打开右侧 Ant Design `Drawer`，内容使用 X 的 `Welcome`、`Bubble` 和 `Conversations`，展示当前智能体、当前会话和最近会话。
- 会话列表读取现有 `useAgents` / `useSessions` API；选择会话只导航到 `/chat/:agentId/:sessionId`，由 `ChatPage` 继续负责 SSE、HITL、TeamPipeline 分组和发送逻辑，不在抽屉中复制聊天状态。
- 抽屉打开前不请求会话列表；没有智能体或会话时显示可操作的空状态，加载和 API 错误也有明确反馈。

因此浮动入口适合快速回到工作中的会话；需要发送消息、查看完整历史或处理确认时，使用“打开完整聊天”进入现有聊天页面。

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
node --test tests/ant-design-x.test.mjs tests/robot-assistant.test.mjs tests/team-messages.test.mjs tests/team-pipeline.test.mjs
```

页面验收重点：

1. 普通用户消息和主 Agent 文本回复显示为 Ant Design X Bubble。
2. 正在生成的主 Agent 回复继续显示流式状态，完成后稳定收尾。
3. 子 Agent 消息仍在主 Agent 回复范围内折叠展示。
4. 工具调用、附件、HITL 确认和拒绝流程保持原有行为。

## 版本依据

Ant Design X 2.9.0 的 peer dependency 是 Ant Design 6.x。升级 X 时需要同时检查 `antd` 主版本和官方迁移说明，不能只替换 `@ant-design/x`。

## Docker 部署

分支新增 `examples/web_ui/Dockerfile.frontend`，它只打包前端静态站点，由 Nginx 提供服务；AgentScope Service、BFF、Redis 和 PostgreSQL 仍应作为独立服务部署。前端启动后在设置页填写实际 AgentScope Service 或 BFF 地址，避免把示例 Express 后端误当成生产 API。

本地构建和运行：

```bash
cd examples/web_ui
docker build -f Dockerfile.frontend -t agentscope-web-ui:antdx .
docker run --rm -p 8080:80 agentscope-web-ui:antdx
```

GitHub Actions 会把该分支发布到 GitHub Container Registry：

```bash
docker pull ghcr.io/balloon72/agentscope-web-ui:latest
docker run --rm -p 8080:80 ghcr.io/balloon72/agentscope-web-ui:latest
```

首次发布是否成功以该分支的 `Publish Ant Design X Web Image` 工作流为准。当前执行环境没有 Docker daemon，因此本地已完成前端生产构建和 TypeScript 校验，Docker 镜像构建由 GitHub Actions 执行。
