import type { ConfirmEvent } from '../domain/types.js';
import type { MessageQueue } from '../mq/message-queue.js';
import type { MetricsStore } from '../metrics/metrics-store.js';

export class ConsumerWorker {
  private readonly seen = new Set<string>();

  constructor(
    private readonly mq: MessageQueue,
    private readonly metrics: MetricsStore,
    private readonly failRate: number,
    private readonly rng: () => number = Math.random,
  ) {}

  async start(): Promise<void> {
    await this.mq.subscribe(async (event) => this.handle(event));
  }

  private async handle(event: ConfirmEvent): Promise<void> {
    if (this.seen.has(event.reservationId)) {
      this.metrics.recordDuplicate();
      return; // 멱등: 이미 처리됨
    }

    if (this.rng() < this.failRate) {
      this.metrics.recordRetry();
      throw new Error(`injected failure for ${event.reservationId}`);
    }

    const start = Date.parse(event.reservedAt);
    const latency = Math.max(0, Date.now() - start);
    this.seen.add(event.reservationId);
    this.metrics.recordProcessed(latency);
  }
}
