import { request, Agent, setGlobalDispatcher } from 'undici';

const BASE = process.env.BASE ?? 'http://localhost:3000';
const STOCK = Number(process.env.STOCK ?? 100);
const N = Number(process.env.N ?? 1000);
// 동시에 열어두는 최대 연결 수. N개 요청을 이 풀로 다중화한다.
const CONNECTIONS = Number(process.env.CONNECTIONS ?? 200);
const EVENT = process.env.EVENT ?? `bench-${STOCK}-${N}`;

// 실제 부하 테스트처럼 keep-alive 연결 풀로 다중화한다.
// (N개 raw 동시 연결을 한꺼번에 열면 클라이언트 측 EPIPE/ECONNRESET가 발생한다.)
setGlobalDispatcher(
  new Agent({ connections: CONNECTIONS, pipelining: 1, keepAliveTimeout: 10_000, keepAliveMaxTimeout: 10_000 }),
);

async function post(path: string, body: unknown): Promise<{ status: number; ms: number }> {
  const start = performance.now();
  try {
    const res = await request(`${BASE}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    await res.body.text();
    return { status: res.statusCode, ms: performance.now() - start };
  } catch {
    // 클라이언트 측 연결 오류(소켓 끊김 등)도 실패로 집계(전체 부하가 중단되지 않게).
    return { status: 0, ms: performance.now() - start };
  }
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
  // 201(예약)·409(품절)이 아닌 모든 응답(서버 500, 락 획득 타임아웃, 연결 오류 등).
  const failed = results.filter((r) => r.status !== 201 && r.status !== 409).length;
  const lat = results.map((r) => r.ms).sort((a, b) => a - b);
  const oversell = Math.max(0, reserved - STOCK);

  console.log(JSON.stringify({
    strategy: process.env.STRATEGY ?? '(server-side)',
    stock: STOCK, requests: N,
    reserved, soldOut, failed, oversell,
    invariantOk: oversell === 0,
    throughputRps: Math.round((N / wall) * 1000),
    latencyMs: { p50: Math.round(pct(lat, 50)), p95: Math.round(pct(lat, 95)), p99: Math.round(pct(lat, 99)) },
  }, null, 2));
}

main().catch((e) => { console.error(e); process.exit(1); });
