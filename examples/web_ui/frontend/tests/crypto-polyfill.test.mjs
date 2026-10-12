import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

const source = readFileSync(new URL('../src/crypto-polyfill.ts', import.meta.url), 'utf8');

test('HTTP context generates distinct RFC 4122 v4 message IDs', () => {
	const crypto = { getRandomValues: webcrypto.getRandomValues.bind(webcrypto) };
	runInNewContext(source, { crypto });
	const ids = Array.from({ length: 100 }, () => crypto.randomUUID());
	for (const id of ids) {
		assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
	}
	assert.equal(new Set(ids).size, ids.length);
});

test('secure context preserves the native implementation', () => {
	const native = () => 'native';
	const crypto = { randomUUID: native };
	runInNewContext(source, { crypto });
	assert.equal(crypto.randomUUID, native);
});

test('entrypoint installs the fallback before loading the app', () => {
	const entry = readFileSync(new URL('../src/main.tsx', import.meta.url), 'utf8');
	assert.ok(entry.indexOf("import './crypto-polyfill'") < entry.indexOf('import App from'));
});
