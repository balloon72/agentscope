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
const en = JSON.parse(
	readFileSync(new URL('../src/i18n/locales/en.json', import.meta.url), 'utf8'),
);
const zh = JSON.parse(
	readFileSync(new URL('../src/i18n/locales/zh.json', import.meta.url), 'utf8'),
);

test('mounts a global robot entry point from the app layout', () => {
	assert.match(layoutSource, /<RobotAssistant \/>/);
	assert.match(source, /<FloatButton[\s\S]*icon=\{<Bot/);
	assert.match(source, /aria-label=\{t\('robot\.open'\)\}/);
});

test('uses Ant Design X and AntD for the compact assistant surface', () => {
	assert.match(source, /from '@ant-design\/x'/);
	assert.match(source, /from 'antd'/);
	assert.match(source, /<Conversations/);
	assert.match(source, /<Drawer/);
});

test('routes session selection back through ChatPage instead of creating a second chat client', () => {
	assert.match(source, /useSessions\(\s*open \? selectedAgentId : null,?\s*\)/);
	assert.match(
		source,
		/navigate\(\s*sessionId \? `\/chat\/\$\{selectedAgentId\}\/\$\{sessionId\}`/,
	);
	assert.doesNotMatch(source, /chatApi|useMessages|streamEvents/);
});

test('keeps assistant copy available in both locale files', () => {
	for (const locale of [en, zh]) {
		assert.ok(locale.robot);
		for (const key of ['open', 'title', 'openChat', 'recentSessions', 'noSessions']) {
			assert.equal(typeof locale.robot[key], 'string');
		}
	}
});
