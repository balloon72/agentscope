"""Static team pipeline configuration contract tests."""
import sys
from pathlib import Path

import pytest

# Keep this focused test runnable from a source checkout without installation.
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agentscope.app.storage import AgentData, TeamPipelineConfig  # noqa: E402
from agentscope.agent import ContextConfig, ReActConfig  # noqa: E402


def test_team_pipeline_config_defaults_are_disabled() -> None:
    data = AgentData(
        name="leader",
        context_config=ContextConfig(),
        react_config=ReActConfig(),
    )
    assert data.team_pipeline_config == TeamPipelineConfig()


def test_team_pipeline_config_rejects_empty_enabled_selection() -> None:
    with pytest.raises(ValueError, match="member_agent_ids"):
        TeamPipelineConfig(enabled=True)


def test_team_pipeline_config_rejects_duplicate_ids() -> None:
    with pytest.raises(ValueError, match="duplicates"):
        TeamPipelineConfig(member_agent_ids=["a", "a"])
