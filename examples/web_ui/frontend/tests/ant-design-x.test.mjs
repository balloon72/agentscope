import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const packageJson = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const appSource = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8');
const messageSource = readFileSync(
	new URL('../src/components/chat/ASMessageBubble.tsx', import.meta.url),
	'utf8',
);

test('pins the Ant Design X v2 dependency pair', () => {
	assert.match(packageJson.dependencies['@ant-design/x'], /^\^2\.9\.0$/);
	assert.match(packageJson.dependencies.antd, /^\^6\./);
});

test('wraps the application in XProvider and uses streaming X bubbles', () => {
	assert.match(appSource, /import \{ XProvider \} from '@ant-design\/x'/);
	assert.match(appSource, /<XProvider theme=\{\{ cssVar: \{\}, hashed: false \}\}>/);
	assert.match(messageSource, /import \{ Bubble as XBubble \} from '@ant-design\/x'/);
	assert.match(messageSource, /streaming=\{isRunning\}/);
});
