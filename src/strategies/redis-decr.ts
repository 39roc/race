import type { Pool } from 'pg';
import type Redis from 'ioredis';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

// 재고가 0보다 크면 차감하고 1, 아니면 -1 반환 (원자적)
const DECR_IF_POSITIVE = `
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then return -1 end
redis.call('DECR', KEYS[1])
return 1`;

export class RedisDecrStrategy implements ConcurrencyStrategy {
  constructor(private readonly pool: Pool, private readonly redis: Redis) {}

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    const ok = (await this.redis.eval(DECR_IF_POSITIVE, 1, `stock:${eventId}`)) as number;
    if (ok !== 1) return { status: 'SOLD_OUT' };

    const ins = await this.pool.query(
      'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
      [eventId, userId],
    );
    return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
  }
}
