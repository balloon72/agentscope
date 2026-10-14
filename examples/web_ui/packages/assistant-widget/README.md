# `@agentscope-ai/assistant-widget`

可复用的 AgentScope React 助手入口。组件提供一个浮动按钮和右侧抽屉，展示最近会话；点击会话后把控制权交还给宿主应用的聊天路由。组件本身不实现完整聊天页、不保存凭证，也不创建第二套 API client。

> 当前版本是第一版集成边界。实现位于 Web UI 的 `RobotAssistant` 组件，发布包导出名与实现保持一致；若使用源码工作区，请从包入口导入，不要深引入 `src` 文件。

## 安装

在当前仓库中，组件包是 `examples/web_ui/packages/assistant-widget` 下的 private workspace package：

```bash
cd examples/web_ui
pnpm install
pnpm --filter frontend build
```

宿主自己的 workspace 只需声明 `"@agentscope-ai/assistant-widget": "workspace:*"`。包发布到 registry 后，再改用：

```bash
pnpm add @agentscope-ai/assistant-widget
```

运行时 peer dependency：React 18/19、`react-dom`、`antd` 和 `@ant-design/x`。宿主应用负责安装并初始化这些依赖；不要在应用里再安装一份不同主版本的 Ant Design。

## 最小接入

```tsx
import { RobotAssistant } from '@agentscope-ai/assistant-widget';
import { client } from './api/client';

export function AppShell() {
	return (
		<>
			{/* 页面布局和 Router 仍由宿主负责 */}
			<Outlet />
			<RobotAssistant
				apiClient={client}
				route={{ agentId: currentAgentId, sessionId: currentSessionId }}
				onNavigateToChat={(agentId, sessionId) => {
					navigate(
						sessionId ? `/chat/${agentId}/${sessionId}` : `/chat/${agentId ?? ''}`,
					);
				}}
			/>
		</>
	);
}
```

`RobotAssistant` 自己渲染 `FloatButton` 和 `Drawer`。抽屉通过 portal 挂载，浮动按钮使用 Ant Design 的 fixed 定位，不会在布局中占据一行或改变页面滚动；组件应在应用 shell 中挂载一次，而不是每个页面各挂载一次。

`copy` 可覆盖按钮、标题、空状态和错误文案，`maxSessions` 可调整抽屉中显示的会话数量；两者都是展示层选项，不会改变数据请求：

```tsx
<RobotAssistant
	apiClient={client}
	maxSessions={5}
	copy={{ open: '打开助手', title: '智能助手' }}
	onNavigateToChat={onNavigateToChat}
/>
```

## Provider 与 API Client

### 不隐藏 Provider

v1 没有隐式全局 Provider。把组件放在宿主已有的 `ConfigProvider` / `XProvider` 内即可继承 locale、token 和主题：

```tsx
<ConfigProvider theme={theme}>
	<XProvider locale={locale}>
		<AppShell />
	</XProvider>
</ConfigProvider>
```

如果宿主没有这些 Provider，Ant Design 的默认主题仍可用；组件不会替宿主创建 Query、Router 或认证上下文。这样可以在已有 React Router、TanStack Query 或其他状态方案中安全复用。

### API Client 契约

`apiClient` 是宿主传入的只读 client。组件只调用 `GET /agent/` 和 `GET /sessions/?agent_id=...`，鉴权、`X-User-ID`、base URL、错误提示和请求重试都由宿主处理。

```ts
export interface RobotAssistantApiClient {
	get<T>(path: string, params?: Record<string, string>): Promise<T>;
}
```

现有 Web UI 的 `client` 已满足该契约，因此可直接传入。若使用 Axios 或自定义 fetch，适配成同样的 `get<T>` 形状即可；不要把 access token、密码或持久化凭证放入组件 props。

包同时导出 `createRobotAssistantDataSource` 和 `RobotAssistantDataSource`，供测试或需要复用同一读取逻辑的宿主适配层使用。组件本身会从传入的 `apiClient` 创建数据源；无需额外注入第二个请求对象。完整类型也从包入口导出：`RobotAssistantApiClient`、`RobotAssistantDataSource`、`RobotAssistantProps`。

## 浮动按钮与路由交接

- 默认按钮在右下角，使用 Ant Design `FloatButton`，并带有可访问的 aria label。
- 抽屉内最多展示最近的会话；组件不会复制聊天消息、流式连接或 HITL 状态。
- 选择会话、点击“打开聊天”都会调用 `onNavigateToChat(agentId?, sessionId?)`。宿主应在回调中执行导航并让主聊天页负责后续数据加载。
- `route` 是宿主当前路由的只读快照，用于高亮当前会话：

    ```ts
    type RobotAssistantRoute = {
    	agentId?: string;
    	sessionId?: string;
    };
    ```

不传 `route` 也能打开抽屉，但无法高亮当前会话。`onNavigateToChat` 是必需回调；如果宿主暂不需要跳转，也应显式传入 no-op，并接受会话选择不会改变路由。

## 主题与位置

主题由外层 Ant Design `ConfigProvider` / `XProvider` 提供，组件不覆盖宿主 token。宿主的暗色模式切换只需更新外层 Provider：

```tsx
<ConfigProvider theme={{ algorithm: dark ? theme.darkAlgorithm : theme.defaultAlgorithm }}>
	<RobotAssistant apiClient={client} onNavigateToChat={onNavigateToChat} />
</ConfigProvider>
```

按钮带有稳定 class `robot-assistant-float-button`，位置可通过 CSS 调整，不需要修改组件源码：

```css
.robot-assistant-float-button {
	inset-inline-end: 24px;
	inset-block-end: 24px;
}
```

请为窄屏保留安全区域（例如 `max(16px, env(safe-area-inset-right))`），并避免把按钮放在宿主固定导航或 Cookie 提示条之下。

## 事件与可观测性

v1 不发送隐式 analytics，也没有全局 event emitter。公开的交互回调只有 `onNavigateToChat`，这样宿主可以决定是否记录导航事件：

```tsx
<RobotAssistant
	apiClient={client}
	onNavigateToChat={(agentId, sessionId) => {
		analytics.track('assistant_navigate_to_chat', { agentId, sessionId });
		navigateToChat(agentId, sessionId);
	}}
/>
```

网络错误由 `apiClient` 处理；组件通过加载和错误状态展示反馈。若需要统一的 `onEvent` 总线，请在宿主做一层薄适配，把 `onNavigateToChat` 转换成自己的事件（例如 `assistant:navigate`），不要依赖未承诺的内部 DOM 事件。

## 导出与版本边界

包入口应至少导出以下公开符号：

```ts
export { RobotAssistant, createRobotAssistantDataSource };
export type { RobotAssistantApiClient, RobotAssistantDataSource, RobotAssistantProps };
```

入口会自动带上组件样式；需要由宿主统一收集 CSS 时，也可使用 `@agentscope-ai/assistant-widget/styles.css` 子路径。

不要从 `components/robot-assistant/*` 或构建产物的内部路径导入。实现可能继续调整抽屉文案和内部 Ant Design 结构；`apiClient`、`route`、`onNavigateToChat` 以及上述类型是第一版接入边界。

## 安全与部署提示

- 组件不会上传或存储认证凭证；统一在宿主 client 中注入认证 header。
- 将服务地址、CORS 和用户身份配置在宿主 API 层，不要通过 URL query 或 `copy` 文案传递 token。
- 如果 `/agent/` 或 `/sessions/` 返回非 2xx，保留宿主 client 的错误处理，不要在组件外重复发起请求。
