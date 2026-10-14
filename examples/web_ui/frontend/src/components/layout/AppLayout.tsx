import { Outlet } from 'react-router-dom';

import { AppSidebar } from '@/components/layout/AppSidebar';
import { RobotAssistant } from '@/components/layout/RobotAssistant';
import { SidebarInset, SidebarProvider } from '@/components/ui/sidebar';

export function AppLayout() {
	return (
		<div className="h-screen flex">
			<SidebarProvider>
				<AppSidebar />
				<SidebarInset className="flex-1 overflow-hidden bg-canvas">
					<Outlet />
				</SidebarInset>
				<RobotAssistant />
			</SidebarProvider>
		</div>
	);
}
