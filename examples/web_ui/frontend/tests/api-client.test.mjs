import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(new URL('../src/api/client.ts', import.meta.url), 'utf8');
const apiIndex = readFileSync(new URL('../src/api/index.ts', import.meta.url), 'utf8');
const robotSource = readFileSync(
	new URL('../src/components/layout/RobotAssistant.tsx', import.meta.url),
	'utf8',
);

test('exposes a credential-aware apiClient/request adapter entry point', () => {
	assert.match(source, /export interface RequestOptions/);
	assert.match(source, /export async function request</);
	assert.match(source, /export const apiClient = client/);
	assert.match(apiIndex, /export \{ apiClient, client, request \} from '\.\/client'/);
});

test('request adapter composes service URL and query params', () => {
	assert.match(source, /new URL\(path, baseUrl \?\? getBaseUrl\(\)\)/);
	assert.match(
		source,
		/Object\.entries\(params\)\.forEach\(\(\[k, v\]\) => url\.searchParams\.set\(k, v\)\)/,
	);
});

test('request adapter injects the user credential through one transport boundary', () => {
	assert.match(source, /'X-User-ID': userId \?\? getUserId\(\)/);
	assert.match(source, /headers: buildHeaders\(body !== undefined, userId\)/);
	assert.doesNotMatch(robotSource, /localStorage|X-User-ID|Authorization/);
});

test('normalizes non-2xx responses into ApiError with backend detail', () => {
	assert.match(source, /const error = new ApiError\(res\.status, detail\)/);
	assert.match(source, /if \(!silent\) toast\.error\(detail\)/);
	assert.match(source, /async function extractErrorDetail/);
});
