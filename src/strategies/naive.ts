import type { Pool } from 'pg';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

export class NaiveStrategy implements ConcurrencyStrategy {
  constructor(private readonly pool: Pool) {}

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    // 읽기와 쓰기 사이에 await 경계가 있어 동시 요청이 같은 재고를 읽고 모두 통과한다.
    const read = await this.pool.query('SELECT stock FROM events WHERE id = $1', [eventId]);
    const stock = read.rows[0]?.stock as number | undefined;
    if (stock === undefined || stock <= 0) return { status: 'SOLD_OUT' };

    await this.pool.query('UPDATE events SET stock = $1 WHERE id = $2', [stock - 1, eventId]);
    const ins = await this.pool.query(
      'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
      [eventId, userId],
    );
    return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
  }
}
