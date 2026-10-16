import assert from 'node:assert/strict';
import test from 'node:test';

import { ReplyFinishedReason, EventType } from '@agentscope-ai/agentscope/event';
import { appendEvent, AssistantMsg } from '@agentscope-ai/agentscope/message';

test('appendEvent preserves structured terminal reply errors', () => {
	const reply = AssistantMsg({ id: 'reply-error', name: 'assistant', content: [] });
	const error = {
		type: 'setup',
		message: "The session could not be prepared — check the agent's configuration.",
	};

	appendEvent(reply, {
		id: 'event-error',
		created_at: '2026-01-01T00:00:01.000Z',
		type: EventType.REPLY_END,
		session_id: 'session-1',
		reply_id: reply.id,
		finished_reason: ReplyFinishedReason.ERROR,
		error,
	});

	assert.equal(reply.finished_at, '2026-01-01T00:00:01.000Z');
	assert.equal(reply.finished_reason, ReplyFinishedReason.ERROR);
	assert.deepEqual(reply.error, error);
});
