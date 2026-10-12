import assert from 'node:assert/strict';
import test from 'node:test';

import { groupTeamMessages } from '../src/lib/team-messages.ts';

const msg = (id, name, role = 'assistant') => ({
	id,
	name,
	role,
	content: [],
	metadata: {},
	created_at: `2026-01-01T00:00:0${id}.000Z`,
	finished_at: `2026-01-01T00:00:0${id}.500Z`,
});

const authors = new Map([
	['leader', { role: 'leader', name: 'leader' }],
	['researcher', { role: 'member', name: 'researcher' }],
	['builder', { role: 'member', name: 'builder' }],
]);

test('groups interleaved member replies under the active leader', () => {
	const messages = [
		msg(1, 'user', 'user'),
		msg(2, 'leader'),
		msg(3, 'researcher'),
		msg(4, 'builder'),
		msg(5, 'researcher'),
	];
	const rows = groupTeamMessages(messages, authors);

	assert.deepEqual(
		rows.map((row) => row.kind),
		['message', 'leader'],
	);
	assert.deepEqual(
		rows[1].members.map((member) => member.name),
		['researcher', 'builder', 'researcher'],
	);
});

test('starts a new member section for each subsequent leader turn', () => {
	const rows = groupTeamMessages(
		[
			msg(1, 'leader'),
			msg(2, 'researcher'),
			msg(3, 'user', 'user'),
			msg(4, 'leader'),
			msg(5, 'builder'),
		],
		authors,
	);

	assert.deepEqual(
		rows.map((row) => row.kind),
		['leader', 'message', 'leader'],
	);
	assert.deepEqual(
		rows[0].members.map((member) => member.name),
		['researcher'],
	);
	assert.deepEqual(
		rows[2].members.map((member) => member.name),
		['builder'],
	);
});

test('holds a partial member history until its leader is available', () => {
	const rows = groupTeamMessages([msg(1, 'researcher'), msg(2, 'leader')], authors);

	assert.equal(rows.length, 1);
	assert.equal(rows[0].kind, 'leader');
	assert.deepEqual(
		rows[0].members.map((member) => member.name),
		['researcher'],
	);
});

test('keeps ordinary conversations unchanged without member metadata', () => {
	const rows = groupTeamMessages([msg(1, 'assistant'), msg(2, 'user', 'user')]);

	assert.deepEqual(
		rows.map((row) => row.kind),
		['message', 'message'],
	);
});
