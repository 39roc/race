import type { Pool } from 'pg';
import type Redis from 'ioredis';
import Redlock from 'redlock';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

export class RedisLockStrategy implements ConcurrencyStrategy {
  private readonly redlock: Redlock;

  constructor(private readonly pool: Pool, redis: Redis) {
    // 경합이 높으므로 재시도 횟수를 넉넉히, TTL로 데드락 방지
    this.redlock = new Redlock([redis], { retryCount: 200, retryDelay: 50, retryJitter: 30 });
  }

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    const lock = await this.redlock.acquire([`lock:event:${eventId}`], 3000);
    try {
      const read = await this.pool.query('SELECT stock FROM events WHERE id = $1', [eventId]);
      const stock = read.rows[0]?.stock as number | undefined;
      if (stock === undefined || stock <= 0) return { status: 'SOLD_OUT' };

      await this.pool.query('UPDATE events SET stock = stock - 1 WHERE id = $1', [eventId]);
      const ins = await this.pool.query(
        'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
        [eventId, userId],
      );
      return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
    } finally {
      await lock.release().catch(() => {});
    }
  }
}
