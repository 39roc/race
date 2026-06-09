import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { Pool } from 'pg';
import Redis from 'ioredis';
import { migrate } from '../../src/infra/pg.js';

export async function startPg(): Promise<{ container: StartedPostgreSqlContainer; pool: Pool }> {
  const container = await new PostgreSqlContainer('postgres:16').start();
  const pool = new Pool({ connectionString: container.getConnectionUri(), max: 50 });
  await migrate(pool);
  return { container, pool };
}

export async function startRedis(): Promise<{ container: StartedRedisContainer; redis: Redis }> {
  const container = await new RedisContainer('redis:7').start();
  const redis = new Redis(container.getConnectionUrl(), { maxRetriesPerRequest: null });
  return { container, redis };
}
