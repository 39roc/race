import type { Pool } from 'pg';
import type Redis from 'ioredis';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

// 소유 토큰이 일치할 때만 해제(다른 소유자의 락을 실수로 지우지 않도록 원자적으로).
const RELEASE = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end`;

/**
 * Redis 분산락(SET key token NX PX ttl) 기반 전략.
 * 락을 잡은 동안에만 Postgres 재고를 읽고 차감하므로 직렬화되어 오버셀이 없다.
 * TTL로 소유자가 죽어도 데드락이 풀리고, 해제는 토큰 검사로 안전하게 한다.
 */
export class RedisLockStrategy implements ConcurrencyStrategy {
  constructor(
    private readonly pool: Pool,
    private readonly redis: Redis,
    private readonly ttlMs = 3000,
    private readonly retryCount = 500,
    private readonly retryDelayMs = 50,
  ) {}

  private async acquire(key: string, token: string): Promise<boolean> {
    for (let i = 0; i < this.retryCount; i++) {
      const ok = await this.redis.set(key, token, 'PX', this.ttlMs, 'NX');
      if (ok === 'OK') return true;
      // 지터를 섞어 thundering herd를 완화
      await new Promise((r) => setTimeout(r, this.retryDelayMs + Math.floor(Math.random() * 30)));
    }
    return false;
  }

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    const key = `lock:event:${eventId}`;
    const token = `${userId}-${Date.now()}-${Math.random()}`;
    if (!(await this.acquire(key, token))) {
      throw new Error(`lock acquire timeout for ${eventId}`);
    }
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
      await this.redis.eval(RELEASE, 1, key, token).catch(() => {});
    }
  }
}
