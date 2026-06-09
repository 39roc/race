import express from 'express';
import { makePool, migrate, seedEvent } from './infra/pg.js';
import { makeRedis } from './infra/redis.js';
import { makeStrategy, makeMq } from './config.js';
import { ReservationService } from './domain/reservation-service.js';

async function main(): Promise<void> {
  const pool = makePool(process.env.PG_URL ?? 'postgres://ticket:ticket@localhost:5433/ticket');
  const redis = makeRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
  await migrate(pool);

  const strategyName = process.env.STRATEGY ?? 'db-atomic';
  const mq = makeMq(process.env.MQ ?? 'bullmq', redis);
  const service = new ReservationService(makeStrategy(strategyName, pool, redis), mq);

  const app = express();
  app.use(express.json());

  // 실험용 시드 엔드포인트: 재고 초기화 (Postgres + Redis 동기화)
  app.post('/seed', async (req, res) => {
    const { eventId, stock } = req.body as { eventId: string; stock: number };
    await seedEvent(pool, eventId, stock);
    await redis.set(`stock:${eventId}`, String(stock));
    res.json({ ok: true, eventId, stock });
  });

  app.post('/reserve', async (req, res) => {
    const { eventId, userId } = req.body as { eventId: string; userId: string };
    try {
      const result = await service.reserve(eventId, userId);
      res.status(result.status === 'RESERVED' ? 201 : 409).json(result);
    } catch (e) {
      // 예약 처리 중 예외(예: 락 획득 타임아웃)가 프로세스를 죽이지 않도록 500으로 응답.
      res.status(500).json({ status: 'ERROR', message: (e as Error).message });
    }
  });

  const port = Number(process.env.PORT ?? 3000);
  app.listen(port, () => console.log(`[server] strategy=${strategyName} mq=${process.env.MQ ?? 'bullmq'} :${port}`));
}

main().catch((e) => { console.error(e); process.exit(1); });
