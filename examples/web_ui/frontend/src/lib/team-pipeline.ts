import type { AgentView, TeamPipelineConfig } from '../api/types';

/** Fresh values for old records and dialogs; never mutate an API response. */
export function copyTeamPipelineConfig(config?: TeamPipelineConfig): TeamPipelineConfig {
	return {
		enabled: config?.enabled ?? false,
		member_agent_ids: [...new Set(config?.member_agent_ids ?? [])],
	};
}

/** A parent may select any visible agent except itself. */
export function teamPipelineCandidates(agents: AgentView[], currentAgentId?: string): AgentView[] {
	return agents.filter((agent) => agent.id !== currentAgentId);
}

export function toggleTeamPipelineMember(
	memberIds: string[],
	agentId: string,
	selected: boolean,
): string[] {
	return selected
		? [...new Set([...memberIds, agentId])]
		: memberIds.filter((id) => id !== agentId);
}
