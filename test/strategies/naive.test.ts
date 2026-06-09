import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Pool } from 'pg';
import { startPg } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { NaiveStrategy } from '../../src/strategies/naive.js';

describe('NaiveStrategy', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;

  beforeAll(async () => {
    ({ container, pool } = await startPg());
  });
  afterAll(async () => {
    await pool.end();
    await container.stop();
  });

  it('동시 요청 시 오버셀이 발생한다(버그 문서화)', async () => {
    await assertConcurrencyContract({
      pool,
      strategy: new NaiveStrategy(pool),
      eventId: 'evt-naive',
      stock: 50,
      concurrency: 500,
      allowOversell: true,
    });
  });
});
