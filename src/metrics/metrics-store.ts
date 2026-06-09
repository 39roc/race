export interface MetricsSnapshot {
  processed: number;
  duplicates: number;
  dlq: number;
  retries: number;
  latency: { p50: number; p95: number; p99: number; max: number };
}

export class MetricsStore {
  private latencies: number[] = [];
  private processed = 0;
  private duplicates = 0;
  private dlq = 0;
  private retries = 0;

  recordProcessed(latencyMs: number): void {
    this.processed++;
    this.latencies.push(latencyMs);
  }
  recordDuplicate(): void { this.duplicates++; }
  recordDlq(): void { this.dlq++; }
  recordRetry(): void { this.retries++; }

  private percentile(sorted: number[], p: number): number {
    if (sorted.length === 0) return 0;
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[idx];
  }

  snapshot(): MetricsSnapshot {
    const sorted = [...this.latencies].sort((a, b) => a - b);
    return {
      processed: this.processed,
      duplicates: this.duplicates,
      dlq: this.dlq,
      retries: this.retries,
      latency: {
        p50: this.percentile(sorted, 50),
        p95: this.percentile(sorted, 95),
        p99: this.percentile(sorted, 99),
        max: sorted.length ? sorted[sorted.length - 1] : 0,
      },
    };
  }
}
