import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Pool } from 'pg';
import { startPg } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { DbPessimisticStrategy } from '../../src/strategies/db-pessimistic.js';

describe('DbPessimisticStrategy', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  beforeAll(async () => { ({ container, pool } = await startPg()); });
  afterAll(async () => { await pool.end(); await container.stop(); });

  it('오버셀 없이 정확히 재고만큼만 예약된다', async () => {
    await assertConcurrencyContract({
      pool, strategy: new DbPessimisticStrategy(pool),
      eventId: 'evt-pess', stock: 100, concurrency: 1000, allowOversell: false,
    });
  });
});
