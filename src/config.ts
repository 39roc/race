import type { Pool } from 'pg';
import type Redis from 'ioredis';
import type { ConcurrencyStrategy } from './strategies/strategy.js';
import type { MessageQueue } from './mq/message-queue.js';
import type { ConfirmEvent } from './domain/types.js';
import { NaiveStrategy } from './strategies/naive.js';
import { DbAtomicStrategy } from './strategies/db-atomic.js';
import { DbPessimisticStrategy } from './strategies/db-pessimistic.js';
import { DbOptimisticStrategy } from './strategies/db-optimistic.js';
import { RedisDecrStrategy } from './strategies/redis-decr.js';
import { RedisLockStrategy } from './strategies/redis-lock.js';
import { BullMq } from './mq/bullmq.js';
import { KafkaMq } from './mq/kafka.js';

export function makeStrategy(name: string, pool: Pool, redis: Redis): ConcurrencyStrategy {
  switch (name) {
    case 'naive': return new NaiveStrategy(pool);
    case 'db-atomic': return new DbAtomicStrategy(pool);
    case 'db-pessimistic': return new DbPessimisticStrategy(pool);
    case 'db-optimistic': return new DbOptimisticStrategy(pool);
    case 'redis-decr': return new RedisDecrStrategy(pool, redis);
    case 'redis-lock': return new RedisLockStrategy(pool, redis);
    default: throw new Error(`unknown STRATEGY: ${name}`);
  }
}

export function makeMq(name: string, redis: Redis, onDeadLetter?: (event: ConfirmEvent) => void): MessageQueue {
  switch (name) {
    case 'bullmq': return new BullMq(redis.options, 'main', onDeadLetter);
    case 'kafka': return new KafkaMq((process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','), 'main', onDeadLetter);
    default: throw new Error(`unknown MQ: ${name}`);
  }
}
