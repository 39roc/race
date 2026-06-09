import { describe, it, expect } from 'vitest';
import type { ConfirmEvent, ReserveResult } from '../../src/domain/types.js';
import type { ConcurrencyStrategy } from '../../src/strategies/strategy.js';
import type { ConfirmHandler, MessageQueue } from '../../src/mq/message-queue.js';
import { ReservationService } from '../../src/domain/reservation-service.js';

class StubStrategy implements ConcurrencyStrategy {
  constructor(private readonly result: ReserveResult) {}
  async decrementStock(): Promise<ReserveResult> { return this.result; }
}
class SpyMq implements MessageQueue {
  published: ConfirmEvent[] = [];
  async publish(e: ConfirmEvent): Promise<void> { this.published.push(e); }
  async subscribe(_h: ConfirmHandler): Promise<void> {}
  async close(): Promise<void> {}
}

describe('ReservationService', () => {
  it('예약 성공 시 확정 이벤트를 발행한다', async () => {
    const mq = new SpyMq();
    const svc = new ReservationService(
      new StubStrategy({ status: 'RESERVED', reservationId: 'r1' }), mq,
    );
    const res = await svc.reserve('evt-1', 'u1');
    expect(res.status).toBe('RESERVED');
    expect(mq.published).toHaveLength(1);
    expect(mq.published[0].reservationId).toBe('r1');
  });

  it('품절이면 이벤트를 발행하지 않는다', async () => {
    const mq = new SpyMq();
    const svc = new ReservationService(new StubStrategy({ status: 'SOLD_OUT' }), mq);
    const res = await svc.reserve('evt-1', 'u1');
    expect(res.status).toBe('SOLD_OUT');
    expect(mq.published).toHaveLength(0);
  });
});
