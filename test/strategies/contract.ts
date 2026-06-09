import { expect } from 'vitest';
import type { Pool } from 'pg';
import { seedEvent } from '../../src/infra/pg.js';
import type { ConcurrencyStrategy } from '../../src/strategies/strategy.js';

/**
 * 재고 STOCK개에 CONCURRENCY개의 동시 예약 요청을 던지고
 * 성공 수와 실제 reservations 행 수를 검증한다.
 * allowOversell=false 면 성공 수 == 재고 (오버셀 0) 를 단언.
 * allowOversell=true  면 오버셀(성공 수 > 재고)이 발생함을 단언(naive 버그 문서화).
 */
export async function assertConcurrencyContract(opts: {
  pool: Pool;
  strategy: ConcurrencyStrategy;
  eventId: string;
  stock: number;
  concurrency: number;
  allowOversell: boolean;
}): Promise<void> {
  const { pool, strategy, eventId, stock, concurrency, allowOversell } = opts;
  await seedEvent(pool, eventId, stock);

  const results = await Promise.all(
    Array.from({ length: concurrency }, (_, i) =>
      strategy.decrementStock(eventId, `user-${i}`),
    ),
  );

  const reserved = results.filter((r) => r.status === 'RESERVED').length;
  const rows = await pool.query('SELECT count(*)::int AS c FROM reservations WHERE event_id = $1', [eventId]);
  const persisted = rows.rows[0].c as number;

  // 성공 응답 수와 실제 적재 행 수는 항상 일치해야 한다.
  expect(reserved).toBe(persisted);

  if (allowOversell) {
    // naive: 오버셀 발생 증명. 이 단언은 "확률적"이다 — 레이스가 안 잡히면 reserved == stock이
    // 되어 실패할 수 있다(전략이 고쳐진 게 아니라 동시성이 낮았던 것). stock 대비 concurrency를
    // 충분히 크게 줘서(예: 50 vs 500) 거의 항상 재현되게 한다. 간헐 실패 시 concurrency를 올린다.
    expect(reserved).toBeGreaterThan(stock);
  } else {
    expect(reserved).toBe(stock); // 락 전략: 정확히 재고만큼만
  }
}
