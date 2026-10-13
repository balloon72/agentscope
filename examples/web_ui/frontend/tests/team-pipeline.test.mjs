import assert from 'node:assert/strict';
import test from 'node:test';

import {
	enabledTeamPipelineDuplicateNames,
	duplicateTeamPipelineNames,
	teamPipelineMemberIdsByName,
} from '../src/lib/team-pipeline.ts';

const agent = (id, name) => ({ id, data: { name } });

test('matches backend exact-name duplicate semantics', () => {
	const agents = [agent('a', 'Worker'), agent('b', 'worker'), agent('c', 'Worker')];

	assert.deepEqual(duplicateTeamPipelineNames(agents), ['Worker']);
	assert.deepEqual(
		enabledTeamPipelineDuplicateNames(
			{ enabled: false, member_agent_ids: ['a', 'b', 'c'] },
			agents,
		),
		[],
	);
});

test('maps configured member names to stable agent ids', () => {
	const agents = [agent('a', 'Worker'), agent('b', 'worker')];
	const ids = teamPipelineMemberIdsByName(agents, ['b', 'a']);

	assert.equal(ids.get('Worker'), 'a');
	assert.equal(ids.get('worker'), 'b');
});
