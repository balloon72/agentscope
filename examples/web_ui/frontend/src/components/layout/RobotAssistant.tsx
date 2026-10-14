import {
	RobotAssistant as AssistantWidget,
	type RobotAssistantCopy,
} from '@agentscope-ai/assistant-widget';
import { useMemo } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';

import { client } from '@/api/client';
import { useTranslation } from '@/i18n/useI18n';

const CHAT_PATH_RE = /^\/chat\/([^/]+)(?:\/([^/]+))?/;

/**
 * Application adapter for the reusable assistant widget.
 *
 * Routing and translations stay owned by this host. The widget receives the
 * existing API client, so it does not create credentials or duplicate auth.
 */
export function RobotAssistant() {
	const { t } = useTranslation();
	const navigate = useNavigate();
	const location = useLocation();
	const [, routeAgentId, routeSessionId] = location.pathname.match(CHAT_PATH_RE) ?? [];

	const copy = useMemo<RobotAssistantCopy>(
		() => ({
			open: t('robot.open'),
			title: t('robot.title'),
			welcomeTitle: t('robot.welcomeTitle'),
			welcomeDescription: (name) => t('robot.welcomeDescription', { name }),
			welcomeNoAgent: t('robot.welcomeNoAgent'),
			activeSession: (name) => t('robot.activeSession', { name }),
			handoff: t('robot.handoff'),
			openChat: t('robot.openChat'),
			recentSessions: t('robot.recentSessions'),
			today: t('chat.session.today'),
			earlier: t('chat.session.earlier'),
			sessionsError: t('robot.sessionsError'),
			noSessions: t('robot.noSessions'),
			loading: t('common.loading'),
		}),
		[t],
	);

	return (
		<AssistantWidget
			apiClient={client}
			route={{ agentId: routeAgentId, sessionId: routeSessionId }}
			copy={copy}
			onNavigateToChat={(agentId, sessionId) => {
				if (agentId) {
					navigate(sessionId ? `/chat/${agentId}/${sessionId}` : `/chat/${agentId}`);
				} else {
					navigate('/chat');
				}
			}}
		/>
	);
}
