import { Bubble, Conversations, Welcome } from '@ant-design/x';
import { Button, Drawer, Empty, FloatButton, Spin, Typography } from 'antd';
import { isToday } from 'date-fns';
import { Bot, MessageSquareText, X } from 'lucide-react';
import { useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';

import { useAgents } from '@/hooks/useAgents';
import { useSessions } from '@/hooks/useSessions';
import { useTranslation } from '@/i18n/useI18n';

const CHAT_PATH_RE = /^\/chat\/([^/]+)(?:\/([^/]+))?/;
const MAX_SESSIONS = 8;

/**
 * A small global entry point for the assistant.
 *
 * The drawer is intentionally a navigation surface rather than a second chat
 * client. Session rows are backed by the same useSessions query used by
 * ChatPage, and selecting one hands the user back to ChatPage so streaming,
 * HITL cards, team grouping, and all other chat state have a single owner.
 */
export function RobotAssistant() {
	const { t } = useTranslation();
	const navigate = useNavigate();
	const location = useLocation();
	const [open, setOpen] = useState(false);
	const { agents, loading: agentsLoading } = useAgents();

	const [, routeAgentId, routeSessionId] = location.pathname.match(CHAT_PATH_RE) ?? [];
	const selectedAgentId = routeAgentId ?? agents[0]?.id ?? null;
	const {
		sessions,
		loading: sessionsLoading,
		error: sessionsError,
	} = useSessions(open ? selectedAgentId : null);

	const activeAgent = agents.find((agent) => agent.id === selectedAgentId) ?? null;
	const activeSession = sessions.find((view) => view.session.id === routeSessionId) ?? null;

	const conversationItems = useMemo(
		() =>
			sessions.slice(0, MAX_SESSIONS).map((view) => ({
				key: view.session.id,
				label: view.session.config.name || view.session.id,
				group: isToday(new Date(view.session.created_at))
					? t('chat.session.today')
					: t('chat.session.earlier'),
				icon: <MessageSquareText aria-hidden="true" className="size-3.5" />,
			})),
		[sessions, t],
	);

	const openChat = (sessionId?: string) => {
		setOpen(false);
		if (selectedAgentId) {
			navigate(
				sessionId ? `/chat/${selectedAgentId}/${sessionId}` : `/chat/${selectedAgentId}`,
			);
		} else {
			navigate('/chat');
		}
	};

	return (
		<>
			<FloatButton
				className="robot-assistant-float-button"
				icon={<Bot aria-hidden="true" />}
				type="primary"
				shape="circle"
				aria-label={t('robot.open')}
				tooltip={t('robot.open')}
				onClick={() => setOpen(true)}
			/>
			<Drawer
				open={open}
				placement="right"
				width={360}
				onClose={() => setOpen(false)}
				closeIcon={<X aria-hidden="true" className="size-4" />}
				title={
					<div className="flex items-center gap-2">
						<span className="flex size-7 items-center justify-center rounded-full bg-primary text-primary-foreground">
							<Bot aria-hidden="true" className="size-4" />
						</span>
						<span>{t('robot.title')}</span>
					</div>
				}
				styles={{ body: { padding: 16 } }}
			>
				<div className="flex h-full min-h-0 flex-col gap-4">
					<Welcome
						variant="borderless"
						icon={<Bot aria-hidden="true" className="size-6" />}
						title={t('robot.welcomeTitle')}
						description={
							activeAgent
								? t('robot.welcomeDescription', { name: activeAgent.data.name })
								: t('robot.welcomeNoAgent')
						}
					/>

					<div className="rounded-xl border border-border/60 bg-muted/30 p-3">
						<Bubble
							role="assistant"
							content={
								activeSession
									? t('robot.activeSession', {
											name:
												activeSession.session.config.name ||
												activeSession.session.id,
										})
									: t('robot.handoff')
							}
						/>
						<Button type="primary" block onClick={() => openChat(routeSessionId)}>
							{t('robot.openChat')}
						</Button>
					</div>

					<div className="flex min-h-0 flex-1 flex-col gap-2">
						<div className="flex items-center justify-between">
							<Typography.Text strong>{t('robot.recentSessions')}</Typography.Text>
							<Typography.Text type="secondary" className="text-xs">
								{sessions.length}
							</Typography.Text>
						</div>
						{agentsLoading || sessionsLoading ? (
							<div className="flex flex-1 items-center justify-center py-8">
								<Spin size="small" aria-label={t('common.loading')} />
							</div>
						) : sessionsError ? (
							<Typography.Text type="danger" className="py-2 text-sm">
								{t('robot.sessionsError')}
							</Typography.Text>
						) : conversationItems.length > 0 ? (
							<div className="min-h-0 flex-1 overflow-y-auto">
								<Conversations
									items={conversationItems}
									groupable
									activeKey={routeSessionId}
									onActiveChange={(sessionId) => openChat(String(sessionId))}
								/>
							</div>
						) : (
							<Empty
								image={Empty.PRESENTED_IMAGE_SIMPLE}
								description={t('robot.noSessions')}
							/>
						)}
					</div>
				</div>
			</Drawer>
		</>
	);
}
