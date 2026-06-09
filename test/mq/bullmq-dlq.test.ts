import { afterAll, beforeAll, describe, it, expect } from 'vitest';
import type { StartedRedisContainer } from '@testcontainers/redis';
import type Redis from 'ioredis';
import { startRedis } from '../helpers/containers.js';
import { BullMq } from '../../src/mq/bullmq.js';
import type { ConfirmEvent } from '../../src/domain/types.js';

describe('BullMq DLQ', () => {
  let container: StartedRedisContainer; let redis: Redis;
  beforeAll(async () => { ({ container, redis } = await startRedis()); });
  afterAll(async () => { redis.disconnect(); await container.stop(); });

  it('재시도 소진 시 onDeadLetter 콜백이 호출된다', async () => {
    const dead: ConfirmEvent[] = [];
    const mq = new BullMq(redis.options, 'dlq', (e) => { dead.push(e); });
    await mq.subscribe(async () => { throw new Error('always fail'); });
    await mq.publish({ reservationId: 'd1', eventId: 'e', userId: 'u', reservedAt: '2026-06-09T00:00:00Z' });
    const start = Date.now();
    while (dead.length === 0) {
      if (Date.now() - start > 30_000) throw new Error('timeout waiting for DLQ');
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(dead).toHaveLength(1);
    expect(dead[0].reservationId).toBe('d1');
    await mq.close();
  });
});
