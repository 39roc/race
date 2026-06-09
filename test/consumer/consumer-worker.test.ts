import { describe, it, expect } from 'vitest';
import type { ConfirmEvent } from '../../src/domain/types.js';
import type { ConfirmHandler, MessageQueue } from '../../src/mq/message-queue.js';
import { ConsumerWorker } from '../../src/consumer/consumer-worker.js';
import { MetricsStore } from '../../src/metrics/metrics-store.js';

class FakeMq implements MessageQueue {
  private handler?: ConfirmHandler;
  async publish(): Promise<void> {}
  async subscribe(h: ConfirmHandler): Promise<void> { this.handler = h; }
  async close(): Promise<void> {}
  async deliver(e: ConfirmEvent): Promise<void> { await this.handler!(e); }
}

function evt(id: string): ConfirmEvent {
  return { reservationId: id, eventId: 'e', userId: 'u', reservedAt: '2026-06-09T00:00:00Z' };
}

describe('ConsumerWorker', () => {
  it('정상 이벤트를 처리하고 메트릭에 기록한다', async () => {
    const mq = new FakeMq(); const metrics = new MetricsStore();
    const worker = new ConsumerWorker(mq, metrics, 0);
    await worker.start();
    await mq.deliver(evt('a'));
    expect(metrics.snapshot().processed).toBe(1);
  });

  it('같은 reservationId 재전달은 중복으로 처리한다(멱등)', async () => {
    const mq = new FakeMq(); const metrics = new MetricsStore();
    const worker = new ConsumerWorker(mq, metrics, 0);
    await worker.start();
    await mq.deliver(evt('dup'));
    await mq.deliver(evt('dup'));
    const s = metrics.snapshot();
    expect(s.processed).toBe(1);
    expect(s.duplicates).toBe(1);
  });

  it('FAIL_RATE=1 이면 예외를 던진다(재시도 유발)', async () => {
    const mq = new FakeMq(); const metrics = new MetricsStore();
    const worker = new ConsumerWorker(mq, metrics, 1);
    await worker.start();
    await expect(mq.deliver(evt('f'))).rejects.toThrow();
  });
});
