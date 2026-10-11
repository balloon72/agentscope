# TeamPipeline Linux 部署与验收

本文针对本分支的 `examples/agent_service` + `examples/web_ui`，给出一份可以在 Linux 上照做的本地部署流程。它不是生产多副本部署指南：TeamPipeline 的调度器在 API 进程内运行（边界见 [TeamPipeline 子 Agent](./team-pipeline-subagent.md)）。

## 1. 运行前提

- Linux、Python 3.11 或更新版本（仓库 `pyproject.toml` 的 `requires-python` 为 `>=3.11`）。
- Node.js 20 或更新版本，以及 `pnpm`。Web UI 的前端依赖和锁文件位于 `examples/web_ui`。
- Redis 在 `localhost:6379` 可访问。示例服务在 `examples/agent_service/main.py` 中把 `RedisStorage` 固定为这个地址和端口；它不会从命令行参数或本文未定义的环境变量读取。
- 一个 OpenAI API 或 OpenAI-compatible 服务的 API key、base URL（如果不是默认 OpenAI 地址）和模型名。不要把 key 提交到仓库或写进 shell 历史以外的共享文件。

安装系统依赖（Debian/Ubuntu 示例）：

```bash
sudo apt update
sudo apt install -y redis-server curl
sudo systemctl enable --now redis-server
```

安装 `uv`、Node.js 20+、`pnpm` 请按各项目官方安装方式完成；随后确认：

```bash
python3 --version
node --version
pnpm --version
redis-cli ping                 # 预期 PONG
```

## 2. 安装 Python 与前端依赖

在仓库根目录执行（将路径替换成自己的 checkout）：

```bash
cd /path/to/agentscope
uv venv
source .venv/bin/activate
uv pip install -e ".[full]"

cd examples/web_ui
pnpm install --frozen-lockfile
cd ../..
```

`.[full]` 来自仓库 `pyproject.toml`，包含示例服务所需的 FastAPI/Uvicorn、Redis 等可选依赖。这里使用 `uv pip`，项目没有一个独立的 AgentScope Linux CLI；服务入口是示例 Python 文件。

## 3. 启动后端服务（端口 8000）

终端 A：

```bash
cd /path/to/agentscope/examples/agent_service
uv run python main.py
```

`main.py` 中的 `uvicorn.run` 明确绑定 `host="0.0.0.0"`、`port=8000`，并在非 Windows 平台启用 reload。因此 API 地址是 `http://127.0.0.1:8000`（或本机局域网地址的 8000 端口）。启动日志应出现 `Uvicorn running on http://0.0.0.0:8000`。

另一个终端检查服务和用户头：

```bash
curl -sS -H 'X-User-ID: local-linux' http://127.0.0.1:8000/health
```

响应应为 JSON，含 `status`、`version`、`components`；`status` 为 `ok` 时，组件不应有 `not_ready`。`/health` 也需要 `X-User-ID`，这和前端 setup 页的行为一致。

## 4. 启动前端（端口 5173）

终端 B：

```bash
cd /path/to/agentscope/examples/web_ui
pnpm --filter frontend dev
```

Vite 默认在 `http://localhost:5173` 提供前端（若端口被占用，Vite 会在日志中选择另一个端口）。首次打开会进入 setup 页。填写：

1. **Server URL**：`http://127.0.0.1:8000`（不要填写 Vite 的 5173）。
2. **Username**：例如 `local-linux`。这是前端发送的 `X-User-ID`，用于隔离该用户的 Agent、凭据和会话。
3. 点击检查/继续。前端会先请求 `GET /health`，健康 JSON 校验通过后才保存这两个值到浏览器 `localStorage`。

`examples/web_ui/backend` 目前只是一个 Express `/api/health` 占位服务（默认 3000）；Vite 的 `/api` 代理也指向 3000。它不是 AgentScope API，不能替代第 3 节的 Python 服务。因此不要把 `pnpm dev` 启动的占位后端当成完整服务；需要完整 UI 时只启动 `pnpm --filter frontend dev`，并把 setup 的 Server URL 指向 8000。

## 5. 在浏览器录入模型 endpoint、key、model

凭据和模型配置走 UI，不使用本文未核验的环境变量：

1. 左侧打开 **Credential（凭据）**（路由 `/credential`），点击新增凭据。
2. 类型选择 **OpenAI API**。该类型由 `src/agentscope/credential/_openai.py` 定义，字段为 `api_key`、可选 `organization`、可选 `base_url`；兼容服务把自己的 API endpoint 填到 `base_url`，通常填服务要求的 base URL（不要擅自追加路径）。
3. 填入 key 并保存。key 只输入到自己的本地服务；不要截图、复制到 issue 或提交到 Git。
4. 回到 **Chat（聊天）**。如果尚无 Agent，点击 Agent 选择器旁的 **+** 新建 Agent；创建对话后，在会话的模型选择/参数入口选择刚保存的 OpenAI 凭据和模型名。前端保存的会话配置形如 `{type, credential_id, model, parameters}`；模型名必须是你的 endpoint 实际支持的名称。
5. 如需验证 endpoint，先让一个普通 Agent 的会话发送一条短消息；成功后再配置 TeamPipeline。不要把不存在或未验证的模型名写进文档配置。

本次验收使用的模型名为 `gpt-6-luna`。该名称只适用于用户已确认的兼容服务；不同服务的模型列表可能不同，仍应以服务返回的 `/v1/models` 为准。

如果需要一个可注册免费层且直接兼容 OpenAI 协议的验收服务，Groq 官方文档给出的 base URL 是 `https://api.groq.com/openai/v1`，可用模型示例为 `openai/gpt-oss-20b`。Groq API key 需要在其控制台由使用者自行创建；不要使用网上公开的共享 key，也不要把 key 写入本仓库。

## 6. 配置 TeamPipeline 成员会话

先为每个成员准备独立、可用的 Agent 和会话模型配置：

1. 在 Chat 页用 **+** 创建至少两个普通 Agent。
2. 为每个 Agent 新建会话，并在该会话的模型入口选择可用的 credential/model；发送一条短消息确认单 Agent 可运行。
3. 新建或编辑主 Agent，展开 **TeamPipeline / 子 Agent**。勾选 **Enabled**，在搜索列表中勾选一个或多个已有成员。UI 会排除主 Agent 自身；启用时至少选择一个成员，然后保存。
4. 主会话中发送任务。成员会以各自 Agent 的资源和最近会话配置执行；在 UI 中可沿成员回复 ID 查看成员过程。成员的 TeamPipeline 配置不会递归展开。

服务保存的配置字段是：

```json
{
  "enabled": true,
  "member_agent_ids": ["agent-a", "agent-b"]
}
```

会话更新使用 `PATCH /sessions/{session_id}?agent_id=...`，配置变更若碰到正在运行的会话会得到 409；等待本轮结束后再改模型或工作区。成员的工具审批可跨 HTTP 续接，但进程崩溃或无粘性多 Pod 接管不在本分支保证范围内。

## 7. 验证清单

只做本地/静态验证时可运行：

```bash
cd /path/to/agentscope
uv run python -m compileall -q src examples/agent_service/main.py
python -m pytest -q tests/team_pipeline_config_test.py tests/pipeline_team_test.py
git diff --check

cd examples/web_ui
pnpm --filter frontend build
pnpm --filter frontend lint
```

人工浏览器验收（需要你自己的真实模型 endpoint/key/model）至少覆盖：setup 页 `/health` 检查、普通 Agent 单轮消息、两个成员都能运行、主 Agent 的 TeamPipeline 成员勾选与保存、成员回复展示，以及成员审批后刷新页面仍能继续。本文没有声称已经用真实模型在浏览器完成验收；没有 key 或 endpoint 时，应把模型流标为未测。

## 8. 日志、停止与持久化

- 前后端分别在各自终端前台输出日志；建议用 `tmux`/`systemd`/容器日志收集来保留日志，不要把 API key 打进日志。
- 前台停止：在对应终端按 `Ctrl-C`。先停前端，再停 Python 服务；Redis 是否停止取决于是否还要保留其他本地服务。
- 示例服务使用 RedisStorage（`localhost:6379`），因此 Agent、凭据和会话等服务数据由 Redis 持久化策略决定；示例没有在代码中配置 RDB/AOF，也没有把运行中的 TeamPipeline 调度对象持久化为可崩溃恢复队列。`LocalWorkspaceManager` 的工作区位于 `examples/agent_service/workspaces`，应按需备份并限制权限。
- 只在单进程/单 Pod 使用正在执行的 TeamPipeline，或确保会话粘性；不要把当前内存中的 TeamPipeline 任务当成跨进程可恢复状态。

## 9. 安全与远程访问

- 默认只在本机访问：把浏览器和服务都留在 `127.0.0.1`，不要把 8000/5173 暴露到公网。
- 必须从另一台电脑访问时，优先 SSH 本地转发：

  ```bash
  ssh -N -L 8000:127.0.0.1:8000 -L 5173:127.0.0.1:5173 user@linux-host
  ```

  然后在本地浏览器打开 `http://127.0.0.1:5173`，setup 的 Server URL 仍填 `http://127.0.0.1:8000`。按需限制 SSH 用户、密钥和防火墙；不要直接开放 Redis 6379。
- 若要局域网/公网部署，需另行加 TLS、认证、CORS/反向代理和密钥管理；本示例的 CORS 是 `allow_origins=["*"]`，不应直接作为公网生产配置。

## 参考与已知边界

- [Agent Service 示例 README](../examples/agent_service/README.md)
- [Web UI 前端 README](../examples/web_ui/frontend/README.md)
- [官方 Agent Service 文档](https://docs.agentscope.io/latest/en/deploy/agent-service)
- [TeamPipeline 子 Agent 设计与边界](./team-pipeline-subagent.md)

已核对：`main.py` 的 8000 端口/Redis localhost:6379、前端 setup 页的 Server URL + Username + `/health` 流程、OpenAI credential schema、TeamPipeline 配置字段、Vite 默认开发入口与 3000 占位 backend。未核对：真实第三方模型调用、浏览器端真实 key 的验收、生产 TLS/反向代理、多进程/多 Pod 故障接管。
