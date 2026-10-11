"""Initialize the agent module."""
from ._a2a_agent import A2AAgent
from ._agent import Agent
from ._config import ContextConfig, InjectionConfig, ModelConfig, ReActConfig
from ._realtime import RealtimeAgent, TurnAggregator, TurnMetrics

__all__ = [
    "A2AAgent",
    "Agent",
    "ContextConfig",
    "InjectionConfig",
    "ModelConfig",
    "ReActConfig",
    "RealtimeAgent",
    "SubagentExecutionContext",
    "SubagentManager",
    "SubagentSession",
    "TurnAggregator",
    "TurnMetrics",
]

from ._execution_context import SubagentExecutionContext
from ._subagent import SubagentManager, SubagentSession
