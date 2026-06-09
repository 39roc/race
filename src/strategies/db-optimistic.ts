import type { Pool } from 'pg';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

export class DbOptimisticStrategy implements ConcurrencyStrategy {
  constructor(private readonly pool: Pool, private readonly maxRetries = 100) {}

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      const read = await this.pool.query(
        'SELECT stock, version FROM events WHERE id = $1',
        [eventId],
      );
      const row = read.rows[0] as { stock: number; version: number } | undefined;
      if (!row || row.stock <= 0) return { status: 'SOLD_OUT' };

      const upd = await this.pool.query(
        'UPDATE events SET stock = stock - 1, version = version + 1 WHERE id = $1 AND version = $2',
        [eventId, row.version],
      );
      if (upd.rowCount === 1) {
        const ins = await this.pool.query(
          'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
          [eventId, userId],
        );
        return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
      }
      // version 충돌 → 재시도
    }
    return { status: 'SOLD_OUT' }; // 재시도 소진
  }
}
