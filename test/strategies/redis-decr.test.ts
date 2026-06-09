import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { StartedRedisContainer } from '@testcontainers/redis';
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import { startPg, startRedis } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { RedisDecrStrategy } from '../../src/strategies/redis-decr.js';

describe('RedisDecrStrategy', () => {
  let pgC: StartedPostgreSqlContainer; let pool: Pool;
  let rC: StartedRedisContainer; let redis: Redis;
  beforeAll(async () => {
    ({ container: pgC, pool } = await startPg());
    ({ container: rC, redis } = await startRedis());
  });
  afterAll(async () => {
    await pool.end(); await pgC.stop(); redis.disconnect(); await rC.stop();
  });

  it('오버셀 없이 정확히 재고만큼만 예약된다', async () => {
    const strategy = new RedisDecrStrategy(pool, redis);
    await redis.set('stock:evt-redis', '100'); // 재고를 Redis에도 시드
    await assertConcurrencyContract({
      pool, strategy, eventId: 'evt-redis', stock: 100, concurrency: 1000, allowOversell: false,
    });
  });
});
