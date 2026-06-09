import { expect } from 'vitest';
import type { ConfirmEvent } from '../../src/domain/types.js';
import type { MessageQueue } from '../../src/mq/message-queue.js';

function sampleEvent(id: string): ConfirmEvent {
  return { reservationId: id, eventId: 'evt-1', userId: 'u1', reservedAt: '2026-06-09T00:00:00Z' };
}

async function waitFor(cond: () => boolean, timeoutMs = 30_000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('timeout waiting for condition');
    await new Promise((r) => setTimeout(r, 100));
  }
}

/** publish→subscribe 라운드트립: 발행한 이벤트가 핸들러로 전달된다. */
export async function assertRoundTrip(mq: MessageQueue): Promise<void> {
  const received: string[] = [];
  await mq.subscribe(async (e) => { received.push(e.reservationId); });
  await mq.publish(sampleEvent('rt-1'));
  await waitFor(() => received.includes('rt-1'));
  expect(received).toContain('rt-1');
}

/** 핸들러가 처음 실패해도 재시도로 결국 성공 처리된다(at-least-once). */
export async function assertRetry(mq: MessageQueue): Promise<void> {
  let attempts = 0;
  await mq.subscribe(async () => {
    attempts++;
    if (attempts < 2) throw new Error('inject fail');
  });
  await mq.publish(sampleEvent('retry-1'));
  await waitFor(() => attempts >= 2);
  expect(attempts).toBeGreaterThanOrEqual(2);
}
