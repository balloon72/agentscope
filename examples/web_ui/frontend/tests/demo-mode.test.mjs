import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const appMode = readFileSync(
	new URL('../src/lib/app-mode.ts', import.meta.url),
	'utf8',
);
const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8');
const demo = readFileSync(
	new URL('../src/pages/demo/DemoChatPage.tsx', import.meta.url),
	'utf8',
);

test('starts in demo mode and keeps an explicit API switch', () => {
	assert.match(appMode, /localStorage\.getItem\(APP_MODE_STORAGE_KEY\) === 'api'/);
	assert.match(app, /if \(mode === 'demo'\)/);
	assert.match(app, /<DemoChatPage/);
	assert.match(demo, /data-testid="switch-api"/);
});

test('demo shows pipeline members as collapsible output and can reset', () => {
	assert.match(demo, /<Collapse items=\{memberItems\(members\)\}/);
	assert.match(demo, /data-testid="reset-demo"/);
	assert.match(demo, /TeamPipeline 执行轨迹/);
	assert.match(demo, /data-final-summary="true"/);
});

test('reset cancels a pending deterministic reply', () => {
	assert.match(demo, /clearTimeout\(replyTimer\.current\)/);
	assert.match(demo, /replyTimer\.current = window\.setTimeout/);
});
