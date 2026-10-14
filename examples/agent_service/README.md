# Agent Service

Agent service is a FastAPI-based, multi-tenant and multi-session service built with AgentScope 2.0.

This example demonstrates

- how to set up the agent service with SQLite storage for a single-process POC,
- how to switch to Redis storage for multi-process or production deployment, and
- how to launch the service and its companion Web UI

Details about the agent service please refer to the [tutorial](https://docs.agentscope.io/latest/en/deploy/agent-service).

## Prerequisites

- Python ≥ 3.11
- Node.js ≥ 20 with `npx`
- `aiosqlite` when using the default SQLite POC storage
- [optional] Gaode/AMap API key in `AMAP_API_KEY` (for the `amap` MCP)

## Quickstart

The default POC mode uses a local SQLite file and does not require Redis or
PostgreSQL:

```bash
uv pip install -e ".[full,storage-sql]"
uv pip install aiosqlite
```

Start the service with SQLite storage:

```bash
cd examples/agent_service
python main.py
```

For the official multi-process setup, switch to Redis storage and start Redis:

```bash
# macOS (Homebrew)
brew install redis
brew services start redis

# Linux (systemd)
sudo apt install redis-server
sudo systemctl start redis-server

# Docker (cross-platform)
docker run --rm -p 6379:6379 redis:7
```

```bash
AGENTSCOPE_POC_MINIMAL=0 \
AGENTSCOPE_STORAGE=redis \
AGENTSCOPE_MESSAGE_BUS=redis \
python main.py
```

The SQLite mode is intended for a single worker POC. It keeps service records
in `agentscope-poc.db`, while the in-memory message bus is reset on restart.
For multi-worker or multi-Pod operation, use both Redis storage and the Redis
message bus as shown above.

POC mode also disables optional external MCP/skill hubs, channel workers, the
scheduler, knowledge-base/Qdrant setup, and the index worker so the service
starts without network-side dependencies. Set `AGENTSCOPE_POC_MINIMAL=0` to
restore those integrations.

Launch the Web UI in a separate terminal to experience a chat-style interface:

```bash
cd examples/web_ui/

pnpm install
# or npm install

# Run in dev mode
pnpm dev
```

After that, you can set the API endpoint `http://localhost:8000` in the Web UI and start experiencing the agent service.

<img src="https://gw.alicdn.com/imgextra/i2/O1CN01Phmg1G1brIVC8WXyU_!!6000000003518-2-tps-2938-1736.png" alt="Web UI Screenshot" width="100%">

## What Next

- You can customize the service in `main.py` by adding your own MCPs, middlewares, or workspace manager implementations.

- Experience the agent service, including
    - human-in-the-loop interactions & permission system
<img src="https://gw.alicdn.com/imgextra/i1/O1CN01vGGiBw20agWwpzmjy_!!6000000006866-2-tps-2934-1732.png" alt="Permission System" width="100%">

    - schedule tasks
<img src="https://gw.alicdn.com/imgextra/i1/O1CN01Xi3Qw71E2haKKu4z0_!!6000000000294-2-tps-2932-1738.png" alt="Schedule Tasks" width="100%">

    - and more! (stay tuned for future updates)
