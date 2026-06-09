import { Queue, Worker, type RedisOptions } from 'bullmq';
import type { ConfirmEvent } from '../domain/types.js';
import type { ConfirmHandler, MessageQueue } from './message-queue.js';

export class BullMq implements MessageQueue {
  private readonly queue: Queue;
  private worker?: Worker;
  private readonly queueName: string;

  constructor(
    private readonly connection: RedisOptions,
    name = 'confirm',
    private readonly onDeadLetter?: (event: ConfirmEvent) => void,
  ) {
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
    // Redis 연결 오류 시 'error' 이벤트 미처리로 프로세스가 죽지 않도록 가드.
    this.worker.on('error', () => {});
    this.worker.on('failed', (job) => {
      // 마지막 시도까지 실패해 더 이상 재시도가 없을 때만 DLQ로 간주
      if (job && job.attemptsMade >= (job.opts.attempts ?? 1)) {
        this.onDeadLetter?.(job.data as ConfirmEvent);
      }
    });
    await this.worker.waitUntilReady();
  }

  async close(): Promise<void> {
    // 큐를 먼저 닫아 신규 publish를 막고, 워커가 진행 중 작업을 마치게 한다.
    await this.queue.close();
    await this.worker?.close();
  }
}
