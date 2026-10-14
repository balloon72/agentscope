import { Bubble, Conversations, Sender, ThoughtChain } from '@ant-design/x';
import {
	Alert,
	Avatar,
	Badge,
	Button,
	Card,
	Collapse,
	Space,
	Tag,
	Tooltip,
	type CollapseProps,
} from 'antd';
import {
	ArrowRight,
	Bot,
	CheckCircle2,
	ChevronRight,
	CircleDot,
	Code2,
	FileCheck2,
	GitBranch,
	Lightbulb,
	PlayCircle,
	RefreshCw,
	RotateCcw,
	Search,
	Send,
	Sparkles,
	UsersRound,
} from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';

import { setAppMode } from '@/lib/app-mode';

interface DemoChatPageProps {
	onUseApi: () => void;
}

type DemoMessage = {
	id: string;
	role: 'user' | 'assistant';
	content: string;
	streaming?: boolean;
};

type DemoMember = {
	id: string;
	name: string;
	role: string;
	status: 'done' | 'running';
	task: string;
	output: string;
	icon: React.ReactNode;
};

const INITIAL_MESSAGES: DemoMessage[] = [
	{
		id: 'user-1',
		role: 'user',
		content: '帮我调研 AgentScope 的多智能体协作方式，并给出落地建议。',
	},
	{
		id: 'assistant-1',
		role: 'assistant',
		content:
			'收到。我会把任务拆给研究、工程和评审子 Agent，并在最后汇总一份可执行的建议。',
	},
];

const INITIAL_MEMBERS: DemoMember[] = [
	{
		id: 'researcher',
		name: 'Researcher',
		role: '资料检索',
		status: 'done',
		task: '梳理 AgentScope 与 TeamPipeline 的核心概念',
		output:
			'AgentScope 提供事件流、工具调用和会话状态管理；TeamPipeline 适合让主 Agent 将独立子任务并行委派，再统一收敛结果。',
		icon: <Search className="size-4" />,
	},
	{
		id: 'builder',
		name: 'Builder',
		role: '方案设计',
		status: 'done',
		task: '设计一个可渐进上线的实现路径',
		output:
			'建议先用静态演示验证交互，再接入 Service SSE。生产环境保留中断、HITL 和失败重试，子 Agent 输出默认折叠。',
		icon: <Code2 className="size-4" />,
	},
	{
		id: 'reviewer',
		name: 'Reviewer',
		role: '质量评审',
		status: 'done',
		task: '检查结论风险与验收清单',
		output:
			'验收重点：主 Agent / 子 Agent 身份清晰、TeamPipeline 状态可见、输出可折叠、总结完整，并能一键重置演示。',
		icon: <FileCheck2 className="size-4" />,
	},
];

function memberItems(members: DemoMember[]): CollapseProps['items'] {
	return members.map((member) => ({
		key: member.id,
		label: (
			<div className="flex min-w-0 items-center gap-3">
				<span className="flex size-7 shrink-0 items-center justify-center rounded-lg bg-blue-50 text-blue-600">
					{member.icon}
				</span>
				<span className="min-w-0 flex-1">
					<span className="block truncate font-medium text-slate-800">{member.name}</span>
					<span className="block truncate text-xs text-slate-500">{member.role} · {member.task}</span>
				</span>
				<Tag color={member.status === 'done' ? 'success' : 'processing'} className="mr-0">
					{member.status === 'done' ? '已完成' : '运行中'}
				</Tag>
			</div>
		),
		children: (
			<div className="rounded-lg bg-slate-50 px-3 py-2.5 text-sm leading-6 text-slate-600">
				{member.output}
			</div>
		),
	}));
}

/** A self-contained, deterministic conversation for online UI acceptance. */
export function DemoChatPage({ onUseApi }: DemoChatPageProps) {
	const [messages, setMessages] = useState<DemoMessage[]>(() => INITIAL_MESSAGES.map((m) => ({ ...m })));
	const [members, setMembers] = useState<DemoMember[]>(() => INITIAL_MEMBERS.map((m) => ({ ...m })));
	const [input, setInput] = useState('');
	const [activeConversation, setActiveConversation] = useState('pipeline-demo');
	const [replying, setReplying] = useState(false);
	const [runVersion, setRunVersion] = useState(1);
	const replyTimer = useRef<number | null>(null);

	useEffect(() => () => {
		if (replyTimer.current !== null) window.clearTimeout(replyTimer.current);
	}, []);

	const resetDemo = () => {
		if (replyTimer.current !== null) {
			window.clearTimeout(replyTimer.current);
			replyTimer.current = null;
		}
		setMessages(INITIAL_MESSAGES.map((m) => ({ ...m })));
		setMembers(INITIAL_MEMBERS.map((m) => ({ ...m })));
		setInput('');
		setReplying(false);
		setRunVersion((version) => version + 1);
	};

	const sendMessage = (value: string) => {
		const content = value.trim();
		if (!content || replying) return;
		setMessages((current) => [
			...current,
			{ id: `user-${Date.now()}`, role: 'user', content },
		]);
		setInput('');
		setReplying(true);
		setMembers((current) => current.map((member) => ({ ...member, status: 'running' })));
		replyTimer.current = window.setTimeout(() => {
			replyTimer.current = null;
			setMessages((current) => [
				...current,
				{
					id: `assistant-${Date.now()}`,
					role: 'assistant',
					content: '演示回复已生成。主 Agent 已收到子 Agent 结果，可以继续追问或重置演示。',
				},
			]);
			setMembers((current) => current.map((member) => ({ ...member, status: 'done' })));
			setReplying(false);
		}, 650);
	};

	const conversations = useMemo(
		() => [
			{ key: 'pipeline-demo', label: 'TeamPipeline 研究演示', group: '今天' },
			{ key: 'hitl-demo', label: '工具调用与确认卡', group: '今天' },
			{ key: 'stream-demo', label: '流式回复体验', group: '更早' },
		],
		[],
	);

	return (
		<div className="flex h-full min-h-0 w-full flex-col overflow-hidden bg-[#f5f7fb] text-slate-900" data-demo-mode="true">
			<header className="flex shrink-0 items-center justify-between border-b border-slate-200/80 bg-white/90 px-5 py-3 backdrop-blur">
				<div className="flex items-center gap-3">
					<div className="flex size-9 items-center justify-center rounded-xl bg-[#1457d9] text-white shadow-sm">
						<Sparkles className="size-5" />
					</div>
					<div>
						<div className="flex items-center gap-2">
							<h1 className="text-[15px] font-semibold tracking-tight">AgentScope Studio</h1>
							<Tag color="gold" className="mr-0 text-[11px] font-medium">演示模式</Tag>
						</div>
						<p className="mt-0.5 text-xs text-slate-500">Ant Design X × AgentScope 交互验收</p>
					</div>
				</div>
				<Space size="small">
					<Tooltip title="恢复初始的静态对话和子 Agent 状态">
						<Button icon={<RotateCcw className="size-3.5" />} onClick={resetDemo} data-testid="reset-demo">
							重置演示
						</Button>
					</Tooltip>
					<Button type="primary" ghost onClick={() => { setAppMode('api'); onUseApi(); }} data-testid="switch-api">
						切换真实 API
						<ArrowRight className="ml-1 size-3.5" />
					</Button>
				</Space>
			</header>

			<div className="flex min-h-0 flex-1 gap-3 p-3">
				<aside className="hidden w-[238px] shrink-0 flex-col rounded-2xl border border-slate-200/80 bg-white p-3 shadow-[0_8px_28px_-24px_rgba(15,23,42,0.45)] md:flex">
					<div className="mb-3 flex items-center justify-between px-1">
						<span className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-400">演示会话</span>
						<Button type="text" size="small" icon={<RefreshCw className="size-3.5" />} onClick={resetDemo} aria-label="重置演示" />
					</div>
					<Conversations
						items={conversations}
						activeKey={activeConversation}
						onActiveChange={(key) => setActiveConversation(String(key))}
						groupable={{ collapsible: false }}
						style={{ flex: 1 }}
					/>
					<div className="mt-auto rounded-xl bg-blue-50/80 p-3 text-xs leading-5 text-blue-700">
						<div className="mb-1 flex items-center gap-1.5 font-medium"><PlayCircle className="size-3.5" />无需后端即可体验</div>
						静态数据会模拟主 Agent、TeamPipeline 和子 Agent 回复。
					</div>
				</aside>

				<main className="flex min-w-0 flex-1 flex-col overflow-hidden rounded-2xl border border-slate-200/80 bg-white shadow-[0_8px_28px_-24px_rgba(15,23,42,0.45)]">
					<div className="flex shrink-0 items-center justify-between border-b border-slate-100 px-5 py-3">
						<div className="flex items-center gap-3">
							<Avatar size={36} icon={<Bot className="size-5" />} className="bg-blue-100 text-blue-700" />
							<div>
								<div className="flex items-center gap-2"><span className="font-semibold">Research Orchestrator</span><Badge status="success" text="在线" /></div>
								<div className="mt-0.5 text-xs text-slate-500">主 Agent · TeamPipeline</div>
							</div>
						</div>
						<Tag icon={<UsersRound className="size-3" />} color="blue">3 个子 Agent</Tag>
					</div>

					<div className="min-h-0 flex-1 overflow-y-auto px-4 py-5 sm:px-8">
						<div className="mx-auto flex max-w-3xl flex-col gap-4">
							{messages.map((message) => (
								<div key={message.id} data-message-role={message.role}>
									<Bubble
										content={message.content}
										placement={message.role === 'user' ? 'end' : 'start'}
										variant={message.role === 'user' ? 'filled' : 'borderless'}
										streaming={Boolean(message.streaming)}
										avatar={message.role === 'user' ? <Avatar size={28}>U</Avatar> : <Avatar size={28} icon={<Bot className="size-4" />} className="bg-blue-100 text-blue-700" />}
									/>
								</div>
							))}

							<Card size="small" className="border-blue-100 bg-blue-50/50" styles={{ body: { padding: 14 } }}>
								<div className="mb-2 flex items-center justify-between">
									<div className="flex items-center gap-2 text-sm font-semibold text-slate-800"><GitBranch className="size-4 text-blue-600" />TeamPipeline 执行轨迹</div>
									<Tag color="processing" className="mr-0">{replying ? '运行中' : '已完成'}</Tag>
								</div>
								<div className="mb-3 flex items-center gap-2 text-xs text-slate-500"><CircleDot className="size-3.5 text-emerald-500" />主 Agent 已拆分任务并收集结果</div>
								<Collapse items={memberItems(members)} ghost expandIcon={({ isActive }) => <ChevronRight className={`size-4 text-slate-400 transition-transform ${isActive ? 'rotate-90' : ''}`} />} />
							</Card>

							<div className="rounded-xl border border-emerald-100 bg-emerald-50/60 p-4" data-final-summary="true">
								<div className="mb-2 flex items-center gap-2 text-sm font-semibold text-emerald-800"><CheckCircle2 className="size-4" />最终总结</div>
								<p className="text-sm leading-6 text-emerald-900/80">推荐采用“主 Agent 规划 → TeamPipeline 并行执行 → 主 Agent 汇总”的路径。先用当前演示模式验收信息层级，再接入真实 API，即可平滑保留 SSE 流式回复、子 Agent 折叠输出和 HITL 确认流程。</p>
							</div>

							<ThoughtChain
								items={[
									{ key: 'plan', title: '主 Agent 规划', description: '拆分为检索、设计、评审三个子任务', status: 'success', collapsible: true },
									{ key: 'parallel', title: 'TeamPipeline 并行执行', description: '3 个子 Agent 返回结果', status: 'success', collapsible: true },
									{ key: 'summary', title: '主 Agent 汇总', description: '生成最终可执行建议', status: replying ? 'loading' : 'success' },
								]}
								line="dashed"
							/>
						</div>
					</div>

					<div className="shrink-0 border-t border-slate-100 bg-white px-4 pb-4 pt-3 sm:px-8">
						<Alert type="info" showIcon message="这是纯前端演示，发送内容不会调用 LLM。" className="mb-3 border-blue-100 bg-blue-50/60 text-xs" />
						<Sender
							value={input}
							onChange={setInput}
							onSubmit={sendMessage}
							loading={replying}
							placeholder="输入消息，体验演示回复…"
							prefix={<Lightbulb className="size-4 text-amber-500" />}
							suffix={<Send className="size-4 text-slate-400" />}
						/>
						<div className="mt-2 flex items-center justify-between px-1 text-[11px] text-slate-400"><span>回车发送 · 可随时重置演示</span><span>演示运行 #{runVersion}</span></div>
					</div>
				</main>
			</div>
		</div>
	);
}
