/** Frontend transport mode. Demo keeps the UI useful without a running service. */
export type AppMode = 'demo' | 'api';

export const APP_MODE_STORAGE_KEY = 'agentscope_frontend_mode';
export const APP_MODE_CHANGE_EVENT = 'agentscope:mode-change';

/**
 * Fresh installs intentionally start in demo mode so the UI can be reviewed
 * without an AgentScope service or an LLM. Existing API users keep their
 * explicit choice from localStorage.
 */
export function getAppMode(): AppMode {
	if (typeof window === 'undefined') return 'demo';
	return window.localStorage.getItem(APP_MODE_STORAGE_KEY) === 'api' ? 'api' : 'demo';
}

export function setAppMode(mode: AppMode): void {
	if (typeof window === 'undefined') return;
	window.localStorage.setItem(APP_MODE_STORAGE_KEY, mode);
	window.dispatchEvent(new CustomEvent(APP_MODE_CHANGE_EVENT, { detail: mode }));
}
