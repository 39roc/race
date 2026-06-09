import type { ReserveResult } from './types.js';
import type { ConcurrencyStrategy } from '../strategies/strategy.js';
import type { MessageQueue } from '../mq/message-queue.js';

export class ReservationService {
  constructor(
    private readonly strategy: ConcurrencyStrategy,
    private readonly mq: MessageQueue,
    private readonly now: () => string = () => new Date().toISOString(),
  ) {}

  async reserve(eventId: string, userId: string): Promise<ReserveResult> {
    const result = await this.strategy.decrementStock(eventId, userId);
    if (result.status === 'RESERVED') {
      await this.mq.publish({
        reservationId: result.reservationId,
        eventId,
        userId,
        reservedAt: this.now(),
      });
    }
    return result;
  }
}
