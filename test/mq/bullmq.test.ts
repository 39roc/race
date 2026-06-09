import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedRedisContainer } from '@testcontainers/redis';
import type Redis from 'ioredis';
import { startRedis } from '../helpers/containers.js';
import { assertRoundTrip, assertRetry } from './contract.js';
import { BullMq } from '../../src/mq/bullmq.js';

describe('BullMq', () => {
  let container: StartedRedisContainer; let redis: Redis;
  beforeAll(async () => { ({ container, redis } = await startRedis()); });
  afterAll(async () => { redis.disconnect(); await container.stop(); });

  it('publish→subscribe 라운드트립', async () => {
    const mq = new BullMq(redis.options, 'rt');
    await assertRoundTrip(mq);
    await mq.close();
  });

  it('핸들러 실패 시 재시도', async () => {
    const mq = new BullMq(redis.options, 'retry');
    await assertRetry(mq);
    await mq.close();
  });
});
