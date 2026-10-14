# -*- coding: utf-8 -*-
"""The example script to start the agent service."""
import os
import sys

import uvicorn
from fastapi.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware

from agentscope.app import create_app, SubAgentTemplate
from agentscope.app.channel import (
    DingTalkChannel,
    DiscordChannel,
    FeishuChannel,
)
from agentscope.app.hub import ClawSkillHub, GitHubMCPHub
from agentscope.app.message_bus import InMemoryMessageBus
from agentscope.app.workspace_manager import LocalWorkspaceManager
from agentscope.mcp import MCPClient, StdioMCPConfig, HttpMCPConfig
from agentscope.middleware import AgenticMemoryMiddleware, MiddlewareBase
from agentscope.permission import PermissionContext, PermissionMode
from agentscope.workspace import WorkspaceBase

POC_MODE = os.getenv("AGENTSCOPE_POC_MINIMAL", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}

default_mcps = [] if POC_MODE else [
    MCPClient(
        name="playwright",
        mcp_config=StdioMCPConfig(
            command="npx",
            args=["@playwright/mcp@latest"],
        ),
        is_stateful=True,
    ),
]

if os.getenv("AMAP_API_KEY") and not POC_MODE:
    default_mcps.append(
        MCPClient(
            name="amap",
            mcp_config=HttpMCPConfig(
                url=f"https://mcp.amap.com/mcp?key="
                f"{os.environ['AMAP_API_KEY']}",
            ),
            is_stateful=False,
        ),
    )

def build_storage():
    """Build the persistence backend for this example service.

    SQLite is the default for the single-process POC so Redis is not required.
    Set ``AGENTSCOPE_STORAGE=redis`` for the official multi-process setup.
    """
    backend = os.getenv("AGENTSCOPE_STORAGE", "sqlite").strip().lower()
    if backend == "sqlite":
        from agentscope.app.storage import AsyncSQLAlchemyStorage

        return AsyncSQLAlchemyStorage(
            url=os.getenv(
                "AGENTSCOPE_SQLITE_URL",
                "sqlite+aiosqlite:///./agentscope-poc.db",
            ),
            create_tables=True,
        )
    if backend == "redis":
        from agentscope.app.storage import RedisStorage

        return RedisStorage(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
        )
    raise ValueError(
        "AGENTSCOPE_STORAGE must be either 'sqlite' (default) or 'redis'",
    )


storage = build_storage()


def build_message_bus():
    """Build the in-process or Redis-backed service message bus."""
    backend = os.getenv("AGENTSCOPE_MESSAGE_BUS", "inmemory").strip().lower()
    if backend == "inmemory":
        return InMemoryMessageBus()
    if backend == "redis":
        from agentscope.app.message_bus import RedisMessageBus

        return RedisMessageBus(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
        )
    raise ValueError(
        "AGENTSCOPE_MESSAGE_BUS must be either 'inmemory' (default) or 'redis'",
    )


message_bus = build_message_bus()

if POC_MODE:
    vector_store = None
else:
    from agentscope.app.rag.knowledge_base_manager import CollectionPerKbManager
    from agentscope.rag import ApproxTokenChunker, QdrantStore

    vector_store = QdrantStore(location=":memory:")


async def longterm_memory_factory(
    user_id: str,
    agent_id: str,
    session_id: str,
    workspace: WorkspaceBase,
) -> list[MiddlewareBase]:
    """Attach Markdown-file long-term memory, stored under the session's
    workspace so it is reachable through whichever backend is bound."""
    del user_id, agent_id, session_id
    return [
        AgenticMemoryMiddleware(
            workdir=workspace.workdir,
            backend=workspace.get_backend(),
        ),
    ]


app = create_app(
    storage=storage,
    message_bus=message_bus,
    # -- To use a Redis-backed message bus instead (recommended for
    # -- multi-process / production deployments), uncomment the lines
    # -- below and replace the InMemoryMessageBus() above:
    #
    # from agentscope.app.message_bus import RedisMessageBus
    # message_bus=RedisMessageBus(
    #     host="localhost",
    #     port=6379,
    # ),
    workspace_manager=LocalWorkspaceManager(
        basedir=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "workspaces",
        ),
        # The default MCP servers that will be added into the workspace
        default_mcps=default_mcps,
    ),
    # Knowledge base feature — backed by an in-memory Qdrant store. The
    # CollectionPerKbManager allocates one collection per knowledge base,
    # so any embedding dimension is allowed.
    knowledge_base_manager=(
        None
        if POC_MODE
        else CollectionPerKbManager(
            storage=storage,
            vector_store=vector_store,
        )
    ),
    # Chunker classes users can pick from when creating a knowledge base;
    # the chosen type and parameters are pinned on the knowledge base.
    knowledge_chunkers=[] if POC_MODE else [ApproxTokenChunker],
    # Resource hubs the UI browses under /hub. Neither needs credentials
    # of its own — an individual MCP card declares whatever key it wants
    # from the user in its ``inputs_schema``. Passing a ClawHub token
    # only raises the rate limit.
    mcp_hubs=[] if POC_MODE else [GitHubMCPHub()],
    skill_hubs=[]
    if POC_MODE
    else [ClawSkillHub(api_token=os.getenv("CLAWHUB_API_TOKEN"))],
    # Customize your own subagent templates
    custom_subagent_templates=[
        SubAgentTemplate(
            type="explorer",
            description=(
                "Read-only agents specialized in exploration tasks. It can "
                "read files but cannot modify, create, or delete them. Use "
                "this agent type when you need to investigate the codebase, "
                "understand its structure, or gather information from files "
                "to support planning—without making any changes."
            ),
            system_prompt_template="""You are {member_name}, an explorer \
agent in team '{team_name}' led by {leader_name}.

Team purpose: {team_description}

Your role: {member_description}

## Responsibilities
- Complete the exploration tasks assigned by the team leader.
- You are read-only: you may inspect files and the codebase, but you must \
never modify, create, or delete anything.

## Reporting
- Always report the task result back to {leader_name} using the TeamSay \
tool, whether the task succeeds or fails.
- Keep your private reasoning private; only share conclusions and findings \
that the leader needs.

Note: `TeamSay` is your ONLY channel to communicate with {leader_name} and \
the other team members. Any other output you produce is invisible to them, \
so anything you want them to see MUST be sent through `TeamSay`.""",
            permission_context=PermissionContext(
                # Read-only
                mode=PermissionMode.EXPLORE,
            ),
        ),
    ],
    # Long-term memory. The default PER_AGENT workspace isolation makes
    # the memory survive across sessions of the same agent.
    extra_agent_middlewares=longterm_memory_factory,
    extra_middlewares=[
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        ),
    ],
    channels=[]
    if POC_MODE
    else [DingTalkChannel, DiscordChannel, FeishuChannel],
    enable_index_worker=not POC_MODE,
    enable_channel_worker=not POC_MODE,
    enable_scheduler=not POC_MODE,
)


if __name__ == "__main__":
    # Start the service
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        # Hot reload forces a SelectorEventLoop on Windows, which cannot
        # spawn the subprocesses that the builtin tools rely on
        reload=sys.platform != "win32",
    )
