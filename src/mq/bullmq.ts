import { Queue, Worker, type RedisOptions } from 'bullmq';
import type { ConfirmEvent } from '../domain/types.js';
import type { ConfirmHandler, MessageQueue } from './message-queue.js';

export class BullMq implements MessageQueue {
  private readonly queue: Queue;
  private worker?: Worker;
  private readonly queueName: string;

  constructor(private readonly connection: RedisOptions, name = 'confirm') {
    this.queueName = `confirm-${name}`;
    this.queue = new Queue(this.queueName, { connection });
  }

  async publish(event: ConfirmEvent): Promise<void> {
    await this.queue.add('confirm', event, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 200 },
      removeOnComplete: false,
      removeOnFail: false, // failed 상태로 남겨 DLQ 역할
    });
  }

  async subscribe(handler: ConfirmHandler): Promise<void> {
    this.worker = new Worker(
      this.queueName,
      async (job) => { await handler(job.data as ConfirmEvent); },
      { connection: this.connection, concurrency: 16 },
    );
    await this.worker.waitUntilReady();
  }

  async close(): Promise<void> {
    await this.worker?.close();
    await this.queue.close();
  }
}
