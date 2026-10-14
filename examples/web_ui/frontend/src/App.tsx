import { XProvider } from '@ant-design/x';
import { QueryClientProvider } from '@tanstack/react-query';
import { Onborda, OnbordaProvider } from 'onborda';
import { useEffect, useMemo, useState } from 'react';
import { createBrowserRouter, Navigate, RouterProvider, useNavigate } from 'react-router-dom';
import { Toaster } from 'sonner';

import { MCPHubPage } from './pages/mcp';
import { SkillHubPage } from './pages/skill';
import { RouteError } from '@/components/error/RouteError';
import { AppLayout } from '@/components/layout/AppLayout';
import { buildChatTour } from '@/components/tour/chatTourSteps';
import { TourCard } from '@/components/tour/TourCard';
import { UploadProvider } from '@/context/UploadContext';
import { useTranslation } from '@/i18n/useI18n';
import { queryClient } from '@/lib/query-client';
import {
	APP_MODE_CHANGE_EVENT,
	getAppMode,
	type AppMode,
	setAppMode,
} from '@/lib/app-mode';
import { DemoChatPage } from '@/pages/demo/DemoChatPage';
import { ChannelPage } from '@/pages/channel';
import { ChatPage } from '@/pages/chat';
import { CredentialPage } from '@/pages/credential';
import { KnowledgePage } from '@/pages/knowledge';
import { SchedulePage } from '@/pages/schedule';
import { SetupPage } from '@/pages/setup';

function SetupPageRoute() {
	const navigate = useNavigate();
	return (
		<>
			<div className="h-screen">
				<SetupPage onComplete={() => navigate('/')} />
			</div>
			<Toaster richColors position="top-right" />
		</>
	);
}

const router = createBrowserRouter([
	{
		element: <AppLayout />,
		errorElement: <RouteError />,
		children: [
			{
				// Content-level boundary: a crash in a page replaces only
				// the Outlet area, so AppLayout (the icon rail / nav) stays
				// usable. The parent route keeps its own errorElement as a
				// last-resort catch-all for AppLayout/AppSidebar crashes.
				errorElement: <RouteError />,
				children: [
					{ path: '/', element: <Navigate to="/chat" replace /> },
					{
						path: '/chat/:agentId?/:sessionId?/:memberId?',
						element: <ChatPage />,
					},
					{ path: '/schedule', element: <SchedulePage /> },
					{ path: '/channel', element: <ChannelPage /> },
					{ path: '/credential', element: <CredentialPage /> },
					{ path: '/mcp', element: <MCPHubPage /> },
					{ path: '/mcp/:hubId', element: <MCPHubPage /> },
					{ path: '/skill', element: <SkillHubPage /> },
					{ path: '/skill/:hubId', element: <SkillHubPage /> },
					{ path: '/knowledge', element: <KnowledgePage /> },
					{ path: '/knowledge/:kbId', element: <KnowledgePage /> },
				],
			},
		],
	},
	{ path: '/setup', element: <SetupPageRoute />, errorElement: <RouteError /> },
]);

function App() {
	const { t } = useTranslation();
	const [mode, setMode] = useState<AppMode>(getAppMode);
	const [setupComplete, setSetupComplete] = useState(() => !!localStorage.getItem('server_url'));
	const tours = useMemo(() => [buildChatTour(t)], [t]);

	// Mode changes are intentionally evented so the settings page and the
	// standalone demo can switch transports without a full page reload.
	useEffect(() => {
		const onModeChange = (event: Event) => {
			const next = (event as CustomEvent<AppMode>).detail;
			if (next === 'demo' || next === 'api') setMode(next);
		};
		window.addEventListener(APP_MODE_CHANGE_EVENT, onModeChange);
		return () => window.removeEventListener(APP_MODE_CHANGE_EVENT, onModeChange);
	}, []);

	if (mode === 'demo') {
		return (
			<XProvider theme={{ cssVar: {}, hashed: false }}>
				<DemoChatPage
					onUseApi={() => {
						setAppMode('api');
						setSetupComplete(!!localStorage.getItem('server_url'));
					}}
				/>
			</XProvider>
		);
	}

	if (!setupComplete) {
		return (
			<SetupPage
				onComplete={() => setSetupComplete(true)}
				onUseDemo={() => setAppMode('demo')}
			/>
		);
	}

	return (
		<QueryClientProvider client={queryClient}>
			<OnbordaProvider>
				<Onborda
					steps={tours}
					cardComponent={TourCard}
					shadowOpacity="0.6"
					cardTransition={{ type: 'spring', duration: 0.4 }}
				>
					<UploadProvider>
						<XProvider theme={{ cssVar: {}, hashed: false }}>
							<RouterProvider router={router} />
						</XProvider>
					</UploadProvider>
					<Toaster richColors position="top-right" />
				</Onborda>
			</OnbordaProvider>
		</QueryClientProvider>
	);
}

export default App;
