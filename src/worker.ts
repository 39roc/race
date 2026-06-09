import { makeRedis } from './infra/redis.js';
import { makeMq } from './config.js';
import { ConsumerWorker } from './consumer/consumer-worker.js';
import { MetricsStore } from './metrics/metrics-store.js';

async function main(): Promise<void> {
  const redis = makeRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
  const mq = makeMq(process.env.MQ ?? 'bullmq', redis);
  const metrics = new MetricsStore();
  const failRate = Number(process.env.FAIL_RATE ?? 0);

  const worker = new ConsumerWorker(mq, metrics, failRate);
  await worker.start();
  console.log(`[worker] mq=${process.env.MQ ?? 'bullmq'} failRate=${failRate} started`);

  // 5초마다 메트릭 출력
  setInterval(() => console.log('[metrics]', JSON.stringify(metrics.snapshot())), 5000);
}

main().catch((e) => { console.error(e); process.exit(1); });
