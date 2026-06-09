import type { Pool, PoolClient } from 'pg';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

export class DbPessimisticStrategy implements ConcurrencyStrategy {
  constructor(private readonly pool: Pool) {}

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    const client: PoolClient = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const read = await client.query(
        'SELECT stock FROM events WHERE id = $1 FOR UPDATE',
        [eventId],
      );
      const stock = read.rows[0]?.stock as number | undefined;
      if (stock === undefined || stock <= 0) {
        await client.query('ROLLBACK');
        return { status: 'SOLD_OUT' };
      }
      await client.query('UPDATE events SET stock = stock - 1 WHERE id = $1', [eventId]);
      const ins = await client.query(
        'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
        [eventId, userId],
      );
      await client.query('COMMIT');
      return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }
}
