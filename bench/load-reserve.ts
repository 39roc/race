import { request } from 'undici';

const BASE = process.env.BASE ?? 'http://localhost:3000';
const STOCK = Number(process.env.STOCK ?? 100);
const N = Number(process.env.N ?? 1000);
const EVENT = process.env.EVENT ?? `bench-${STOCK}-${N}`;

async function post(path: string, body: unknown): Promise<{ status: number; ms: number }> {
  const start = performance.now();
  const res = await request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  await res.body.text();
  return { status: res.statusCode, ms: performance.now() - start };
}

function pct(sorted: number[], p: number): number {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

async function main(): Promise<void> {
  await post('/seed', { eventId: EVENT, stock: STOCK });

  const t0 = performance.now();
  const results = await Promise.all(
    Array.from({ length: N }, (_, i) => post('/reserve', { eventId: EVENT, userId: `u${i}` })),
  );
  const wall = performance.now() - t0;

  const reserved = results.filter((r) => r.status === 201).length;
  const soldOut = results.filter((r) => r.status === 409).length;
  const lat = results.map((r) => r.ms).sort((a, b) => a - b);
  const oversell = Math.max(0, reserved - STOCK);

  console.log(JSON.stringify({
    strategy: process.env.STRATEGY ?? '(server-side)',
    stock: STOCK, requests: N,
    reserved, soldOut, oversell,
    invariantOk: oversell === 0,
    throughputRps: Math.round((N / wall) * 1000),
    latencyMs: { p50: Math.round(pct(lat, 50)), p95: Math.round(pct(lat, 95)), p99: Math.round(pct(lat, 99)) },
  }, null, 2));
}

main().catch((e) => { console.error(e); process.exit(1); });
