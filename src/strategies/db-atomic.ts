import type { Pool } from 'pg';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

export class DbAtomicStrategy implements ConcurrencyStrategy {
  constructor(private readonly pool: Pool) {}

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    const upd = await this.pool.query(
      'UPDATE events SET stock = stock - 1 WHERE id = $1 AND stock > 0 RETURNING stock',
      [eventId],
    );
    if (upd.rowCount === 0) return { status: 'SOLD_OUT' };

    const ins = await this.pool.query(
      'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
      [eventId, userId],
    );
    return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
  }
}
