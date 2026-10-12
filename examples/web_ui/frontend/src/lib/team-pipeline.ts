import type { AgentView, TeamPipelineConfig } from '../api/types';

/** Fresh values for old records and dialogs; never mutate an API response. */
export function copyTeamPipelineConfig(config?: TeamPipelineConfig): TeamPipelineConfig {
	return {
		enabled: config?.enabled ?? false,
		member_agent_ids: [...new Set(config?.member_agent_ids ?? [])],
	};
}

/**
 * Static TeamPipeline members must be owned by the current user. Shared
 * read-only agents are visible in the general picker, but the backend rejects
 * them because their model/workspace session is owner-scoped. Existing
 * non-editable ids are surfaced as unavailable selections by the form.
 */
export function teamPipelineCandidates(agents: AgentView[], currentAgentId?: string): AgentView[] {
	return agents.filter((agent) => agent.id !== currentAgentId && agent.editable);
}

/** Names are display labels; ids disambiguate duplicate agent names. */
export function duplicateTeamPipelineNames(agents: AgentView[]): string[] {
	const names = new Map<string, { label: string; count: number }>();
	for (const agent of agents) {
		const label = agent.data.name.trim();
		const key = label.toLocaleLowerCase();
		const current = names.get(key);
		if (current) current.count += 1;
		else names.set(key, { label, count: 1 });
	}
	return [...names.values()].filter((entry) => entry.count > 1).map((entry) => entry.label);
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
