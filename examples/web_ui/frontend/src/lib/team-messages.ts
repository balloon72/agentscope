import type { Msg } from '@agentscope-ai/agentscope/message';

import type { MessageAuthor } from '@/components/chat/ASMessageBubble';

/** Rows used to render a TeamPipeline conversation. */
export type TeamMessageEntry =
	| { kind: 'message'; message: Msg }
	| { kind: 'leader'; message: Msg; members: Msg[] }
	| { kind: 'orphan-members'; members: Msg[] };

/**
 * Group TeamPipeline participant messages under their most recent leader.
 *
 * This deliberately uses participant names rather than array positions. The
 * stream may interleave several member replies, and history can be refreshed
 * while one of those replies is still running. A member reply that arrives
 * before its leader (a partial history page) is held until the next leader;
 * if there is no leader at all it is rendered as a non-assistant activity row
 * instead of reintroducing a peer assistant bubble.
 */
export function groupTeamMessages(
	msgs: Msg[],
	authors?: ReadonlyMap<string, MessageAuthor>,
): TeamMessageEntry[] {
	const memberNames = new Set(
		[...(authors?.entries() ?? [])]
			.filter(([, author]) => author.role === 'member')
			.map(([name]) => name),
	);
	if (memberNames.size === 0) {
		return msgs.map((message) => ({ kind: 'message', message }));
	}

	const entries: TeamMessageEntry[] = [];
	let activeLeader: Extract<TeamMessageEntry, { kind: 'leader' }> | null = null;
	let pendingMembers: Msg[] = [];

	for (const message of msgs) {
		if (message.role === 'assistant' && memberNames.has(message.name)) {
			if (activeLeader) activeLeader.members.push(message);
			else pendingMembers.push(message);
			continue;
		}

		if (message.role === 'assistant') {
			const leader: Extract<TeamMessageEntry, { kind: 'leader' }> = {
				kind: 'leader',
				message,
				members: pendingMembers,
			};
			entries.push(leader);
			activeLeader = leader;
			pendingMembers = [];
			continue;
		}

		if (pendingMembers.length > 0) {
			entries.push({ kind: 'orphan-members', members: pendingMembers });
			pendingMembers = [];
		}
		entries.push({ kind: 'message', message });
		// A new user/system turn starts a new leader group.
		activeLeader = null;
	}

	if (pendingMembers.length > 0) {
		entries.push({ kind: 'orphan-members', members: pendingMembers });
	}
	return entries;
}

