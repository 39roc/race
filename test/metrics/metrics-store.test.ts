import { describe, it, expect } from 'vitest';
import { MetricsStore } from '../../src/metrics/metrics-store.js';

describe('MetricsStore', () => {
  it('처리/중복/DLQ 카운트와 지연 퍼센타일을 집계한다', () => {
    const m = new MetricsStore();
    m.recordProcessed(10);
    m.recordProcessed(20);
    m.recordProcessed(30);
    m.recordDuplicate();
    m.recordDlq();
    m.recordRetry();
    m.recordRetry();

    const s = m.snapshot();
    expect(s.processed).toBe(3);
    expect(s.duplicates).toBe(1);
    expect(s.dlq).toBe(1);
    expect(s.retries).toBe(2);
    expect(s.latency.p50).toBe(20);
    expect(s.latency.max).toBe(30);
  });

  it('빈 상태에서도 안전하게 0을 반환한다', () => {
    const s = new MetricsStore().snapshot();
    expect(s.processed).toBe(0);
    expect(s.latency.p50).toBe(0);
  });
});
