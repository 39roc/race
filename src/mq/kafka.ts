import { Kafka, type Consumer, type Producer } from 'kafkajs';
import type { ConfirmEvent } from '../domain/types.js';
import type { ConfirmHandler, MessageQueue } from './message-queue.js';

const MAX_ATTEMPTS = 3;

export class KafkaMq implements MessageQueue {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private consumer?: Consumer;
  private readonly topic: string;
  private readonly dlqTopic: string;
  private readonly groupId: string;
  private readonly attempts = new Map<string, number>();

  constructor(
    brokers: string[],
    suffix = 'main',
    private readonly onDeadLetter?: (event: ConfirmEvent) => void,
  ) {
    this.kafka = new Kafka({ clientId: 'ticket', brokers });
    this.producer = this.kafka.producer();
    this.topic = `confirm-${suffix}`;
    this.dlqTopic = `confirm-${suffix}-dlq`;
    this.groupId = `confirm-group-${suffix}`;
  }

  async publish(event: ConfirmEvent): Promise<void> {
    await this.producer.connect();
    await this.producer.send({
      topic: this.topic,
      messages: [{ key: event.reservationId, value: JSON.stringify(event) }],
    });
  }

  async subscribe(handler: ConfirmHandler): Promise<void> {
    this.consumer = this.kafka.consumer({ groupId: this.groupId });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });
    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value!.toString()) as ConfirmEvent;
        const key = event.reservationId;
        const n = (this.attempts.get(key) ?? 0) + 1;
        this.attempts.set(key, n);
        try {
          await handler(event);
          this.attempts.delete(key);
        } catch (e) {
          if (n >= MAX_ATTEMPTS) {
            // DLQ로 이동 후 커밋 진행(무한 재시도 방지)
            await this.producer.send({
              topic: this.dlqTopic,
              messages: [{ key, value: message.value }],
            });
            this.attempts.delete(key);
            this.onDeadLetter?.(event);
          } else {
            throw e; // 커밋 안 됨 → kafkajs가 같은 메시지 재처리
          }
        }
      },
    });
  }

  async close(): Promise<void> {
    await this.consumer?.disconnect();
    await this.producer.disconnect();
  }
}
