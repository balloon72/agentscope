import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(
	new URL('../src/components/layout/RobotAssistant.tsx', import.meta.url),
	'utf8',
);
const layoutSource = readFileSync(
	new URL('../src/components/layout/AppLayout.tsx', import.meta.url),
	'utf8',
);
const widgetSource = readFileSync(
	new URL('../../packages/assistant-widget/src/RobotAssistant.tsx', import.meta.url),
	'utf8',
);
const en = JSON.parse(
	readFileSync(new URL('../src/i18n/locales/en.json', import.meta.url), 'utf8'),
);
const zh = JSON.parse(
	readFileSync(new URL('../src/i18n/locales/zh.json', import.meta.url), 'utf8'),
);

test('mounts a global robot entry point from the app layout', () => {
	assert.match(layoutSource, /<RobotAssistant \/>/);
	assert.match(source, /from '@agentscope-ai\/assistant-widget'/);
	assert.match(source, /<AssistantWidget/);
});

test('keeps the reusable package responsible for the Ant Design X surface', () => {
	assert.match(widgetSource, /from '@ant-design\/x'/);
	assert.match(widgetSource, /from 'antd'/);
	assert.match(widgetSource, /<FloatButton[\s\S]*icon=\{<Bot/);
	assert.match(widgetSource, /<Drawer/);
	assert.match(widgetSource, /<Conversations/);
});

test('passes the host API and routes session selection back through ChatPage', () => {
	assert.match(source, /apiClient=\{client\}/);
	assert.match(source, /route=\{\{ agentId: routeAgentId, sessionId: routeSessionId \}\}/);
	assert.match(source, /onNavigateToChat=\{/);
	assert.match(source, /navigate\(sessionId \? `\/chat\/\$\{agentId\}\/\$\{sessionId\}`/);
	assert.doesNotMatch(
		source,
		/chatApi|useMessages|streamEvents|localStorage|X-User-ID|Authorization/,
	);
});

test('keeps credentials in the host API client instead of the widget package', () => {
	assert.match(widgetSource, /apiClient: RobotAssistantApiClient/);
	assert.match(widgetSource, /apiClient\.get/);
	assert.doesNotMatch(widgetSource, /localStorage|X-User-ID|Authorization/);
});

test('keeps assistant copy available in both locale files', () => {
	for (const locale of [en, zh]) {
		assert.ok(locale.robot);
		for (const key of ['open', 'title', 'openChat', 'recentSessions', 'noSessions']) {
			assert.equal(typeof locale.robot[key], 'string');
		}
	}
});
