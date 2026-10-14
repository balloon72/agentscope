import { Bubble, Conversations, Welcome } from '@ant-design/x';
import { Button, Drawer, Empty, FloatButton, Spin, Typography } from 'antd';
import { isToday } from 'date-fns';
import { Bot, MessageSquareText, X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

/** The small read-only surface of the host API client used by this widget. */
export interface RobotAssistantApiClient {
	get<T>(path: string, params?: Record<string, string>): Promise<T>;
}

export interface RobotAssistantAgent {
	id: string;
	name: string;
}

export interface RobotAssistantSession {
	id: string;
	name: string;
	createdAt: string;
}

export interface RobotAssistantDataSource {
	listAgents(): Promise<RobotAssistantAgent[]>;
	listSessions(agentId: string): Promise<RobotAssistantSession[]>;
}

interface AgentListResponse {
	agents: Array<{ id: string; data?: { name?: string } }>;
}

interface SessionListResponse {
	sessions: Array<{
		session: {
			id: string;
			created_at: string;
			config?: { name?: string };
		};
	}>;
}

/**
 * Adapts the host's already-configured API client to the widget contract.
 * Authentication, base URL and user headers stay owned by the host client.
 */
export function createRobotAssistantDataSource(
	apiClient: RobotAssistantApiClient,
): RobotAssistantDataSource {
	return {
		async listAgents() {
			const response = await apiClient.get<AgentListResponse>('/agent/');
			return response.agents.map((agent) => ({
				id: agent.id,
				name: agent.data?.name || agent.id,
			}));
		},
		async listSessions(agentId: string) {
			const response = await apiClient.get<SessionListResponse>('/sessions/', {
				agent_id: agentId,
			});
			return response.sessions.map(({ session }) => ({
				id: session.id,
				name: session.config?.name || session.id,
				createdAt: session.created_at,
			}));
		},
	};
}

export interface RobotAssistantRoute {
	agentId?: string | null;
	sessionId?: string | null;
}

export interface RobotAssistantCopy {
	open: string;
	title: string;
	welcomeTitle: string;
	welcomeDescription: (agentName: string) => string;
	welcomeNoAgent: string;
	activeSession: (sessionName: string) => string;
	handoff: string;
	openChat: string;
	recentSessions: string;
	sessionsError: string;
	noSessions: string;
	loading: string;
}

export interface RobotAssistantProps {
	/** A host client; the widget never creates credentials or performs login. */
	apiClient: RobotAssistantApiClient;
	/** Called when the user wants to continue in the host's full chat page. */
	onNavigateToChat: (agentId?: string, sessionId?: string) => void;
	route?: RobotAssistantRoute;
	copy?: Partial<RobotAssistantCopy>;
	maxSessions?: number;
}

const DEFAULT_COPY: RobotAssistantCopy = {
	open: 'Open assistant',
	title: 'Assistant',
	welcomeTitle: 'Need a hand?',
	welcomeDescription: (agentName) => `Pick up your conversation with ${agentName}.`,
	welcomeNoAgent: 'Choose an agent to start a conversation.',
	activeSession: (sessionName) => `Your current session is “${sessionName}”.`,
	handoff: 'Open the full chat to send messages, review replies, and handle confirmations.',
	openChat: 'Open full chat',
	recentSessions: 'Recent sessions',
	sessionsError: 'Sessions could not be loaded.',
	noSessions: 'No sessions yet.',
	loading: 'Loading…',
};

const DEFAULT_MAX_SESSIONS = 8;

/**
 * A compact navigation surface for an AgentScope host application.
 *
 * It intentionally does not send messages or open an SSE stream. The host
 * remains the owner of chat state, HITL cards and navigation semantics.
 */
export function RobotAssistant({
	apiClient,
	onNavigateToChat,
	route = {},
	copy: copyOverrides,
	maxSessions = DEFAULT_MAX_SESSIONS,
}: RobotAssistantProps) {
	const copy = { ...DEFAULT_COPY, ...copyOverrides };
	const [open, setOpen] = useState(false);
	const dataSource = useMemo(() => createRobotAssistantDataSource(apiClient), [apiClient]);
	const [agents, setAgents] = useState<RobotAssistantAgent[]>([]);
	const [agentsLoading, setAgentsLoading] = useState(false);
	const [agentsError, setAgentsError] = useState<Error | null>(null);
	const [sessions, setSessions] = useState<RobotAssistantSession[]>([]);
	const [sessionsLoading, setSessionsLoading] = useState(false);
	const [sessionsError, setSessionsError] = useState<Error | null>(null);

	useEffect(() => {
		let active = true;
		setAgentsLoading(true);
		setAgentsError(null);
		void dataSource
			.listAgents()
			.then((nextAgents) => {
				if (!active) return;
				setAgents(nextAgents);
			})
			.catch((error: unknown) => {
				if (active)
					setAgentsError(error instanceof Error ? error : new Error(String(error)));
			})
			.finally(() => {
				if (active) setAgentsLoading(false);
			});
		return () => {
			active = false;
		};
	}, [dataSource]);

	const selectedAgentId = route.agentId ?? agents[0]?.id ?? null;
	const activeAgent = agents.find((agent) => agent.id === selectedAgentId) ?? null;
	const activeSession = sessions.find((session) => session.id === route.sessionId) ?? null;

	useEffect(() => {
		if (!open || !selectedAgentId) {
			setSessions([]);
			setSessionsError(null);
			setSessionsLoading(false);
			return;
		}
		let active = true;
		setSessionsLoading(true);
		setSessionsError(null);
		void dataSource
			.listSessions(selectedAgentId)
			.then((nextSessions) => {
				if (!active) return;
				setSessions(nextSessions);
			})
			.catch((error: unknown) => {
				if (active)
					setSessionsError(error instanceof Error ? error : new Error(String(error)));
			})
			.finally(() => {
				if (active) setSessionsLoading(false);
			});
		return () => {
			active = false;
		};
	}, [dataSource, open, selectedAgentId]);

	const conversationItems = useMemo(
		() =>
			sessions.slice(0, maxSessions).map((session) => ({
				key: session.id,
				label: session.name,
				group: isToday(new Date(session.createdAt)) ? 'Today' : 'Earlier',
				icon: <MessageSquareText aria-hidden="true" className="as-assistant-icon as-assistant-icon-sm" />,
			})),
		[maxSessions, sessions],
	);

	const openChat = (sessionId?: string) => {
		setOpen(false);
		onNavigateToChat(selectedAgentId ?? undefined, sessionId);
	};

	return (
		<>
			<FloatButton
				className="robot-assistant-float-button"
				icon={<Bot aria-hidden="true" />}
				type="primary"
				shape="circle"
				aria-label={copy.open}
				tooltip={copy.open}
				onClick={() => setOpen(true)}
			/>
			<Drawer
				open={open}
				placement="right"
				width={360}
				onClose={() => setOpen(false)}
				closeIcon={<X aria-hidden="true" className="as-assistant-icon as-assistant-icon-md" />}
				title={
					<div className="as-assistant-header">
						<span className="as-assistant-avatar">
							<Bot aria-hidden="true" className="as-assistant-icon as-assistant-icon-md" />
						</span>
						<span>{copy.title}</span>
					</div>
				}
				styles={{ body: { padding: 16 } }}
			>
				<div className="as-assistant-stack">
					<Welcome
						variant="borderless"
						icon={<Bot aria-hidden="true" className="as-assistant-icon as-assistant-icon-lg" />}
						title={copy.welcomeTitle}
						description={
							activeAgent
								? copy.welcomeDescription(activeAgent.name)
								: copy.welcomeNoAgent
						}
					/>

					<div className="as-assistant-handoff">
						<Bubble
							role="assistant"
							content={
								activeSession
									? copy.activeSession(activeSession.name)
									: copy.handoff
							}
						/>
						<Button
							type="primary"
							block
							onClick={() => openChat(route.sessionId ?? undefined)}
						>
							{copy.openChat}
						</Button>
					</div>

					<div className="as-assistant-stack">
						<div className="as-assistant-session-heading">
							<Typography.Text strong>{copy.recentSessions}</Typography.Text>
							<Typography.Text
								type="secondary"
								className="as-assistant-session-count"
							>
								{sessions.length}
							</Typography.Text>
						</div>
						{agentsLoading || sessionsLoading ? (
							<div className="as-assistant-loading">
								<Spin size="small" aria-label={copy.loading} />
							</div>
						) : agentsError || sessionsError ? (
							<Typography.Text type="danger" className="as-assistant-error">
								{copy.sessionsError}
							</Typography.Text>
						) : conversationItems.length > 0 ? (
							<div className="as-assistant-session-list">
								<Conversations
									items={conversationItems}
									groupable
									activeKey={route.sessionId ?? undefined}
									onActiveChange={(sessionId) => openChat(String(sessionId))}
								/>
							</div>
						) : (
							<Empty
								image={Empty.PRESENTED_IMAGE_SIMPLE}
								description={copy.noSessions}
							/>
						)}
					</div>
				</div>
			</Drawer>
		</>
	);
}
