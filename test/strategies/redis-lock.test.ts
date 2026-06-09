import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { StartedRedisContainer } from '@testcontainers/redis';
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import { startPg, startRedis } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { RedisLockStrategy } from '../../src/strategies/redis-lock.js';

describe('RedisLockStrategy', () => {
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
    await assertConcurrencyContract({
      pool, strategy: new RedisLockStrategy(pool, redis),
      eventId: 'evt-lock', stock: 100, concurrency: 300, allowOversell: false,
    });
  });
});
