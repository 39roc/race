# 선착순 티켓 동시성 & Kafka vs BullMQ — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 선착순 티켓 예약 앱을 TDD로 구현해 6가지 동시성 제어 전략을 비교하고, 후속 발권 처리를 Kafka와 BullMQ로 각각 구현해 전달 보장·재시도·처리량을 비교한다.

**Architecture:** 단일 Express 앱에 두 개의 독립 교체 축(`ConcurrencyStrategy`, `MessageQueue`)을 두고 env로 조립한다. `ReservationService`는 인터페이스에만 의존하고, 컨슈머 워커가 확정 이벤트를 소비해 발권을 시뮬레이션하며 메트릭을 수집한다.

**Tech Stack:** Node.js 20 + TypeScript, Express, PostgreSQL(pg), Redis(ioredis + redlock), Kafka(kafkajs), BullMQ, Vitest + Testcontainers, Docker Compose.

---

## File Structure

```
ticket/
├── docker-compose.yml
├── package.json
├── tsconfig.json
├── vitest.config.ts
├── .env.example
├── db/migrations/001_init.sql
├── src/
│   ├── domain/types.ts                 # ReserveResult, ConfirmEvent
│   ├── domain/reservation-service.ts   # ReservationService
│   ├── infra/pg.ts                      # pg Pool 헬퍼 + 마이그레이션 실행
│   ├── infra/redis.ts                   # ioredis 클라이언트 헬퍼
│   ├── strategies/strategy.ts           # ConcurrencyStrategy 인터페이스
│   ├── strategies/naive.ts
│   ├── strategies/db-atomic.ts
│   ├── strategies/db-pessimistic.ts
│   ├── strategies/db-optimistic.ts
│   ├── strategies/redis-decr.ts
│   ├── strategies/redis-lock.ts
│   ├── mq/message-queue.ts              # MessageQueue 인터페이스
│   ├── mq/bullmq.ts
│   ├── mq/kafka.ts
│   ├── consumer/consumer-worker.ts      # 발권 + 실패주입 + 멱등 + DLQ
│   ├── metrics/metrics-store.ts
│   ├── config.ts                        # env factory
│   ├── server.ts                        # Express /reserve
│   └── worker.ts                        # 컨슈머 엔트리포인트
├── test/
│   ├── helpers/containers.ts            # Testcontainers 부트스트랩
│   ├── strategies/contract.ts           # 공용 동시성 계약 슈트
│   ├── strategies/*.test.ts
│   ├── mq/contract.ts                   # 공용 MQ 계약 슈트
│   ├── mq/*.test.ts
│   └── domain/reservation-service.test.ts
└── bench/
    ├── load-reserve.ts                  # 동시 예약 부하 + 불변식 검증
    └── report.ts                        # 비교표 출력
```

---

## Task 1: 프로젝트 스캐폴드

**Files:**
- Create: `package.json`, `tsconfig.json`, `vitest.config.ts`, `.env.example`

- [ ] **Step 1: package.json 작성**

```json
{
  "name": "ticket",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "test": "vitest run",
    "test:watch": "vitest",
    "build": "tsc -p tsconfig.json",
    "server": "tsx src/server.ts",
    "worker": "tsx src/worker.ts",
    "bench:reserve": "tsx bench/load-reserve.ts"
  },
  "dependencies": {
    "bullmq": "^5.12.0",
    "express": "^4.19.2",
    "ioredis": "^5.4.1",
    "kafkajs": "^2.2.4",
    "pg": "^8.12.0",
    "redlock": "^5.0.0-beta.2",
    "undici": "^6.19.0"
  },
  "devDependencies": {
    "@testcontainers/kafka": "^10.13.0",
    "@testcontainers/postgresql": "^10.13.0",
    "@testcontainers/redis": "^10.13.0",
    "@types/express": "^4.17.21",
    "@types/node": "^20.14.0",
    "@types/pg": "^8.11.6",
    "testcontainers": "^10.13.0",
    "tsx": "^4.16.0",
    "typescript": "^5.5.0",
    "vitest": "^2.0.0"
  }
}
```

- [ ] **Step 2: tsconfig.json 작성**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "dist",
    "types": ["node"]
  },
  "include": ["src", "test", "bench"]
}
```

- [ ] **Step 3: vitest.config.ts 작성**

테스트는 실제 컨테이너를 띄우므로 타임아웃을 넉넉히 둔다.

```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    testTimeout: 120_000,
    hookTimeout: 120_000,
    fileParallelism: false,
  },
});
```

- [ ] **Step 4: .env.example 작성**

```bash
# 동시성 전략: naive | db-atomic | db-pessimistic | db-optimistic | redis-decr | redis-lock
STRATEGY=db-atomic
# 메시지 큐: kafka | bullmq
MQ=bullmq
# 컨슈머 실패 주입 확률 (0~1)
FAIL_RATE=0
PORT=3000
PG_URL=postgres://ticket:ticket@localhost:5433/ticket
REDIS_URL=redis://localhost:6379
KAFKA_BROKERS=localhost:9092
```

- [ ] **Step 5: 의존성 설치**

Run: `npm install`
Expected: `node_modules/` 생성, 에러 없음.

- [ ] **Step 6: Commit**

```bash
git add package.json package-lock.json tsconfig.json vitest.config.ts .env.example
git commit -m "chore: 프로젝트 스캐폴드 (ts, vitest, deps)"
```

---

## Task 2: Docker Compose 인프라

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: docker-compose.yml 작성 (KRaft 모드 Kafka)**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ticket
      POSTGRES_PASSWORD: ticket
      POSTGRES_DB: ticket
    ports: ["5433:5432"]   # 호스트 5432 충돌 회피
  redis:
    image: redis:7
    ports: ["6379:6379"]
  kafka:
    image: apache/kafka:3.7.0   # bitnami/kafka는 Docker Hub 배포 중단됨 → 공식 이미지 사용
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # 듀얼 리스너: 호스트(localhost:9092)와 컨테이너 내부(kafka:19092) 모두 접속 가능
      KAFKA_LISTENERS: PLAINTEXT://:19092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports: ["8080:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:19092
    depends_on: [kafka]
```

- [ ] **Step 2: 인프라 기동 확인**

Run: `docker compose up -d && docker compose ps`
Expected: postgres/redis/kafka/kafka-ui 모두 `running`. (로컬 개발용. 테스트는 Testcontainers로 독립 기동하므로 이 compose는 수동 실험·bench용.)

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: docker-compose 인프라 (postgres, redis, kafka KRaft, kafka-ui)"
```

---

## Task 3: 도메인 타입 + DB 스키마 + 인프라 헬퍼

**Files:**
- Create: `src/domain/types.ts`, `db/migrations/001_init.sql`, `src/infra/pg.ts`, `src/infra/redis.ts`

- [ ] **Step 1: 도메인 타입 작성**

`src/domain/types.ts`:

```ts
export type ReserveResult =
  | { status: 'RESERVED'; reservationId: string }
  | { status: 'SOLD_OUT' };

export interface ConfirmEvent {
  reservationId: string;
  eventId: string;
  userId: string;
  reservedAt: string; // ISO 8601
}
```

- [ ] **Step 2: 마이그레이션 SQL 작성**

`db/migrations/001_init.sql`:

```sql
CREATE TABLE IF NOT EXISTS events (
  id      TEXT PRIMARY KEY,
  name    TEXT NOT NULL,
  stock   INTEGER NOT NULL CHECK (stock >= 0),
  version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reservations (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id   TEXT NOT NULL REFERENCES events(id),
  user_id    TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- [ ] **Step 3: pg 헬퍼 작성**

`src/infra/pg.ts`:

```ts
import { Pool } from 'pg';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

export function makePool(url: string): Pool {
  return new Pool({ connectionString: url, max: 50 });
}

export async function migrate(pool: Pool): Promise<void> {
  const sql = readFileSync(
    resolve(__dirname, '../../db/migrations/001_init.sql'),
    'utf8',
  );
  await pool.query(sql);
}

export async function seedEvent(
  pool: Pool,
  id: string,
  stock: number,
): Promise<void> {
  await pool.query(
    `INSERT INTO events (id, name, stock, version) VALUES ($1, $1, $2, 0)
     ON CONFLICT (id) DO UPDATE SET stock = $2, version = 0`,
    [id, stock],
  );
}
```

- [ ] **Step 4: redis 헬퍼 작성**

`src/infra/redis.ts`:

```ts
import Redis from 'ioredis';

export function makeRedis(url: string): Redis {
  return new Redis(url, { maxRetriesPerRequest: null });
}
```

- [ ] **Step 5: Commit**

```bash
git add src/domain/types.ts db/migrations/001_init.sql src/infra/pg.ts src/infra/redis.ts
git commit -m "feat: 도메인 타입, DB 스키마, pg/redis 인프라 헬퍼"
```

---

## Task 4: ConcurrencyStrategy 인터페이스 + 공용 계약 슈트 + naive 전략

naive는 read-modify-write로 오버셀이 **발생함**을 증명한다. 나머지 전략은 동일 슈트로 오버셀이 **없음**을 증명한다.

**Files:**
- Create: `src/strategies/strategy.ts`, `test/helpers/containers.ts`, `test/strategies/contract.ts`, `src/strategies/naive.ts`, `test/strategies/naive.test.ts`

- [ ] **Step 1: 인터페이스 작성**

`src/strategies/strategy.ts`:

```ts
import type { ReserveResult } from '../domain/types.js';

export interface ConcurrencyStrategy {
  decrementStock(eventId: string, userId: string): Promise<ReserveResult>;
}
```

- [ ] **Step 2: Testcontainers 헬퍼 작성**

`test/helpers/containers.ts`:

```ts
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { Pool } from 'pg';
import Redis from 'ioredis';
import { migrate } from '../../src/infra/pg.js';

export async function startPg(): Promise<{ container: StartedPostgreSqlContainer; pool: Pool }> {
  const container = await new PostgreSqlContainer('postgres:16').start();
  const pool = new Pool({ connectionString: container.getConnectionUri(), max: 50 });
  await migrate(pool);
  return { container, pool };
}

export async function startRedis(): Promise<{ container: StartedRedisContainer; redis: Redis }> {
  const container = await new RedisContainer('redis:7').start();
  const redis = new Redis(container.getConnectionUrl(), { maxRetriesPerRequest: null });
  return { container, redis };
}
```

- [ ] **Step 3: 공용 계약 슈트 작성 (이것이 모든 전략 테스트의 핵심)**

`test/strategies/contract.ts`:

```ts
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
    expect(reserved).toBeGreaterThan(stock); // naive: 오버셀 발생 증명
  } else {
    expect(reserved).toBe(stock); // 락 전략: 정확히 재고만큼만
  }
}
```

- [ ] **Step 4: naive 테스트 작성 (failing first)**

`test/strategies/naive.test.ts`:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Pool } from 'pg';
import { startPg } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { NaiveStrategy } from '../../src/strategies/naive.js';

describe('NaiveStrategy', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;

  beforeAll(async () => {
    ({ container, pool } = await startPg());
  });
  afterAll(async () => {
    await pool.end();
    await container.stop();
  });

  it('동시 요청 시 오버셀이 발생한다(버그 문서화)', async () => {
    await assertConcurrencyContract({
      pool,
      strategy: new NaiveStrategy(pool),
      eventId: 'evt-naive',
      stock: 50,
      concurrency: 500,
      allowOversell: true,
    });
  });
});
```

- [ ] **Step 5: 테스트 실패 확인**

Run: `npx vitest run test/strategies/naive.test.ts`
Expected: FAIL — `NaiveStrategy`가 아직 없어 import 에러.

- [ ] **Step 6: naive 구현 (read-modify-write로 의도적 레이스)**

`src/strategies/naive.ts`:

```ts
import type { Pool } from 'pg';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

export class NaiveStrategy implements ConcurrencyStrategy {
  constructor(private readonly pool: Pool) {}

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    // 읽기와 쓰기 사이에 await 경계가 있어 동시 요청이 같은 재고를 읽고 모두 통과한다.
    const read = await this.pool.query('SELECT stock FROM events WHERE id = $1', [eventId]);
    const stock = read.rows[0]?.stock as number | undefined;
    if (stock === undefined || stock <= 0) return { status: 'SOLD_OUT' };

    await this.pool.query('UPDATE events SET stock = $1 WHERE id = $2', [stock - 1, eventId]);
    const ins = await this.pool.query(
      'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
      [eventId, userId],
    );
    return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
  }
}
```

- [ ] **Step 7: 테스트 통과 확인 (오버셀이 실제로 발생)**

Run: `npx vitest run test/strategies/naive.test.ts`
Expected: PASS — 성공 수 > 50 (오버셀 발생). 드물게 오버셀이 안 나면 `concurrency`를 1000으로 올린다.

- [ ] **Step 8: Commit**

```bash
git add src/strategies/strategy.ts test/helpers/containers.ts test/strategies/contract.ts src/strategies/naive.ts test/strategies/naive.test.ts
git commit -m "feat: ConcurrencyStrategy 인터페이스 + 계약 슈트 + naive(오버셀 재현)"
```

---

## Task 5: db-atomic 전략

`UPDATE ... WHERE stock > 0`의 원자성으로 오버셀을 막는다.

**Files:**
- Create: `src/strategies/db-atomic.ts`, `test/strategies/db-atomic.test.ts`

- [ ] **Step 1: 테스트 작성**

`test/strategies/db-atomic.test.ts`:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Pool } from 'pg';
import { startPg } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { DbAtomicStrategy } from '../../src/strategies/db-atomic.js';

describe('DbAtomicStrategy', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  beforeAll(async () => { ({ container, pool } = await startPg()); });
  afterAll(async () => { await pool.end(); await container.stop(); });

  it('오버셀 없이 정확히 재고만큼만 예약된다', async () => {
    await assertConcurrencyContract({
      pool, strategy: new DbAtomicStrategy(pool),
      eventId: 'evt-atomic', stock: 100, concurrency: 1000, allowOversell: false,
    });
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/strategies/db-atomic.test.ts`
Expected: FAIL — `DbAtomicStrategy` 없음.

- [ ] **Step 3: 구현**

`src/strategies/db-atomic.ts`:

```ts
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
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/strategies/db-atomic.test.ts`
Expected: PASS — 성공 수 == 100.

- [ ] **Step 5: Commit**

```bash
git add src/strategies/db-atomic.ts test/strategies/db-atomic.test.ts
git commit -m "feat: db-atomic 전략 (원자적 조건부 UPDATE)"
```

---

## Task 6: db-pessimistic 전략

`SELECT ... FOR UPDATE`로 행 잠금 후 차감.

**Files:**
- Create: `src/strategies/db-pessimistic.ts`, `test/strategies/db-pessimistic.test.ts`

- [ ] **Step 1: 테스트 작성**

`test/strategies/db-pessimistic.test.ts` — Task 5 테스트와 동일 구조, 다음만 교체:
- import: `import { DbPessimisticStrategy } from '../../src/strategies/db-pessimistic.js';`
- describe 이름: `'DbPessimisticStrategy'`
- strategy: `new DbPessimisticStrategy(pool)`
- eventId: `'evt-pess'`
- (stock 100, concurrency 1000, allowOversell false 동일)

전체 코드:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Pool } from 'pg';
import { startPg } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { DbPessimisticStrategy } from '../../src/strategies/db-pessimistic.js';

describe('DbPessimisticStrategy', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  beforeAll(async () => { ({ container, pool } = await startPg()); });
  afterAll(async () => { await pool.end(); await container.stop(); });

  it('오버셀 없이 정확히 재고만큼만 예약된다', async () => {
    await assertConcurrencyContract({
      pool, strategy: new DbPessimisticStrategy(pool),
      eventId: 'evt-pess', stock: 100, concurrency: 1000, allowOversell: false,
    });
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/strategies/db-pessimistic.test.ts`
Expected: FAIL — 구현 없음.

- [ ] **Step 3: 구현 (트랜잭션 + 행 잠금)**

`src/strategies/db-pessimistic.ts`:

```ts
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
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/strategies/db-pessimistic.test.ts`
Expected: PASS — 성공 수 == 100.

- [ ] **Step 5: Commit**

```bash
git add src/strategies/db-pessimistic.ts test/strategies/db-pessimistic.test.ts
git commit -m "feat: db-pessimistic 전략 (SELECT FOR UPDATE)"
```

---

## Task 7: db-optimistic 전략

version 기반 CAS, 충돌 시 제한 횟수 재시도.

**Files:**
- Create: `src/strategies/db-optimistic.ts`, `test/strategies/db-optimistic.test.ts`

- [ ] **Step 1: 테스트 작성**

`test/strategies/db-optimistic.test.ts`:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Pool } from 'pg';
import { startPg } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { DbOptimisticStrategy } from '../../src/strategies/db-optimistic.js';

describe('DbOptimisticStrategy', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  beforeAll(async () => { ({ container, pool } = await startPg()); });
  afterAll(async () => { await pool.end(); await container.stop(); });

  it('오버셀 없이 정확히 재고만큼만 예약된다', async () => {
    await assertConcurrencyContract({
      pool, strategy: new DbOptimisticStrategy(pool, 100),
      eventId: 'evt-opt', stock: 100, concurrency: 1000, allowOversell: false,
    });
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/strategies/db-optimistic.test.ts`
Expected: FAIL — 구현 없음.

- [ ] **Step 3: 구현 (version CAS + 재시도)**

`src/strategies/db-optimistic.ts`:

```ts
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
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/strategies/db-optimistic.test.ts`
Expected: PASS — 성공 수 == 100. (높은 경합에서 재시도가 많아 maxRetries가 충분해야 함.)

- [ ] **Step 5: Commit**

```bash
git add src/strategies/db-optimistic.ts test/strategies/db-optimistic.test.ts
git commit -m "feat: db-optimistic 전략 (version CAS + 재시도)"
```

---

## Task 8: redis-decr 전략

Lua 스크립트로 경계 검사 + 원자적 차감. 예약은 Postgres에 적재(검증 일관성 유지).

**Files:**
- Create: `src/strategies/redis-decr.ts`, `test/strategies/redis-decr.test.ts`

- [ ] **Step 1: 테스트 작성 (pg + redis 둘 다 필요)**

`test/strategies/redis-decr.test.ts`:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { StartedRedisContainer } from '@testcontainers/redis';
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import { startPg, startRedis } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { RedisDecrStrategy } from '../../src/strategies/redis-decr.js';

describe('RedisDecrStrategy', () => {
  let pgC: StartedPostgreSqlContainer; let pool: Pool;
  let rC: StartedRedisContainer; let redis: Redis;
  beforeAll(async () => {
    ({ container: pgC, pool } = await startPg());
    ({ container: rC, redis } = await startRedis());
  });
  afterAll(async () => {
    await pool.end(); await pgC.stop(); redis.disconnect(); await rC.stop();
  });

  it('오버셀 없이 정확히 재고만큼만 예약된다', async () => {
    const strategy = new RedisDecrStrategy(pool, redis);
    await redis.set('stock:evt-redis', '100'); // 재고를 Redis에도 시드
    await assertConcurrencyContract({
      pool, strategy, eventId: 'evt-redis', stock: 100, concurrency: 1000, allowOversell: false,
    });
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/strategies/redis-decr.test.ts`
Expected: FAIL — 구현 없음.

- [ ] **Step 3: 구현 (Lua로 경계 검사)**

`src/strategies/redis-decr.ts`:

```ts
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

// 재고가 0보다 크면 차감하고 1, 아니면 -1 반환 (원자적)
const DECR_IF_POSITIVE = `
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then return -1 end
redis.call('DECR', KEYS[1])
return 1`;

export class RedisDecrStrategy implements ConcurrencyStrategy {
  constructor(private readonly pool: Pool, private readonly redis: Redis) {}

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    const ok = (await this.redis.eval(DECR_IF_POSITIVE, 1, `stock:${eventId}`)) as number;
    if (ok !== 1) return { status: 'SOLD_OUT' };

    const ins = await this.pool.query(
      'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
      [eventId, userId],
    );
    return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
  }
}
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/strategies/redis-decr.test.ts`
Expected: PASS — 성공 수 == 100.

- [ ] **Step 5: Commit**

```bash
git add src/strategies/redis-decr.ts test/strategies/redis-decr.test.ts
git commit -m "feat: redis-decr 전략 (Lua 원자적 경계 차감)"
```

---

## Task 9: redis-lock 전략

Redlock 분산락 획득 후 Postgres 차감. lock TTL 필수.

**Files:**
- Create: `src/strategies/redis-lock.ts`, `test/strategies/redis-lock.test.ts`

- [ ] **Step 1: 테스트 작성**

`test/strategies/redis-lock.test.ts`:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { StartedRedisContainer } from '@testcontainers/redis';
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import { startPg, startRedis } from '../helpers/containers.js';
import { assertConcurrencyContract } from './contract.js';
import { RedisLockStrategy } from '../../src/strategies/redis-lock.js';

describe('RedisLockStrategy', () => {
  let pgC: StartedPostgreSqlContainer; let pool: Pool;
  let rC: StartedRedisContainer; let redis: Redis;
  beforeAll(async () => {
    ({ container: pgC, pool } = await startPg());
    ({ container: rC, redis } = await startRedis());
  });
  afterAll(async () => {
    await pool.end(); await pgC.stop(); redis.disconnect(); await rC.stop();
  });

  it('오버셀 없이 정확히 재고만큼만 예약된다', async () => {
    await assertConcurrencyContract({
      pool, strategy: new RedisLockStrategy(pool, redis),
      eventId: 'evt-lock', stock: 100, concurrency: 300, allowOversell: false,
    });
  });
});
```

> 주의: 분산락은 직렬화되어 느리므로 concurrency를 300으로 둔다(테스트 시간 단축).

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/strategies/redis-lock.test.ts`
Expected: FAIL — 구현 없음.

- [ ] **Step 3: 구현 (Redlock)**

`src/strategies/redis-lock.ts`:

```ts
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import Redlock from 'redlock';
import type { ReserveResult } from '../domain/types.js';
import type { ConcurrencyStrategy } from './strategy.js';

export class RedisLockStrategy implements ConcurrencyStrategy {
  private readonly redlock: Redlock;

  constructor(private readonly pool: Pool, redis: Redis) {
    // 경합이 높으므로 재시도 횟수를 넉넉히, TTL로 데드락 방지
    this.redlock = new Redlock([redis], { retryCount: 200, retryDelay: 50, retryJitter: 30 });
  }

  async decrementStock(eventId: string, userId: string): Promise<ReserveResult> {
    const lock = await this.redlock.acquire([`lock:event:${eventId}`], 3000);
    try {
      const read = await this.pool.query('SELECT stock FROM events WHERE id = $1', [eventId]);
      const stock = read.rows[0]?.stock as number | undefined;
      if (stock === undefined || stock <= 0) return { status: 'SOLD_OUT' };

      await this.pool.query('UPDATE events SET stock = stock - 1 WHERE id = $1', [eventId]);
      const ins = await this.pool.query(
        'INSERT INTO reservations (event_id, user_id) VALUES ($1, $2) RETURNING id',
        [eventId, userId],
      );
      return { status: 'RESERVED', reservationId: ins.rows[0].id as string };
    } finally {
      await lock.release().catch(() => {});
    }
  }
}
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/strategies/redis-lock.test.ts`
Expected: PASS — 성공 수 == 100.

- [ ] **Step 5: Commit**

```bash
git add src/strategies/redis-lock.ts test/strategies/redis-lock.test.ts
git commit -m "feat: redis-lock 전략 (Redlock 분산락)"
```

---

## Task 10: MetricsStore

컨슈머/벤치 결과 집계. 인메모리 + 퍼센타일 계산.

**Files:**
- Create: `src/metrics/metrics-store.ts`, `test/metrics/metrics-store.test.ts`

- [ ] **Step 1: 테스트 작성**

`test/metrics/metrics-store.test.ts`:

```ts
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
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/metrics/metrics-store.test.ts`
Expected: FAIL — `MetricsStore` 없음.

- [ ] **Step 3: 구현**

`src/metrics/metrics-store.ts`:

```ts
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
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/metrics/metrics-store.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/metrics/metrics-store.ts test/metrics/metrics-store.test.ts
git commit -m "feat: MetricsStore (카운트 + 지연 퍼센타일)"
```

---

## Task 11: MessageQueue 인터페이스 + 공용 계약 슈트 + BullMQ 구현

**Files:**
- Create: `src/mq/message-queue.ts`, `test/mq/contract.ts`, `src/mq/bullmq.ts`, `test/mq/bullmq.test.ts`

- [ ] **Step 1: 인터페이스 작성**

`src/mq/message-queue.ts`:

```ts
import type { ConfirmEvent } from '../domain/types.js';

export type ConfirmHandler = (event: ConfirmEvent) => Promise<void>;

export interface MessageQueue {
  publish(event: ConfirmEvent): Promise<void>;
  /** handler가 throw하면 재시도, 한도 초과 시 DLQ로 이동해야 한다. */
  subscribe(handler: ConfirmHandler): Promise<void>;
  close(): Promise<void>;
}
```

- [ ] **Step 2: 공용 MQ 계약 슈트 작성**

`test/mq/contract.ts`:

```ts
import { expect } from 'vitest';
import type { ConfirmEvent } from '../../src/domain/types.js';
import type { MessageQueue } from '../../src/mq/message-queue.js';

function sampleEvent(id: string): ConfirmEvent {
  return { reservationId: id, eventId: 'evt-1', userId: 'u1', reservedAt: '2026-06-09T00:00:00Z' };
}

async function waitFor(cond: () => boolean, timeoutMs = 30_000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('timeout waiting for condition');
    await new Promise((r) => setTimeout(r, 100));
  }
}

/** publish→subscribe 라운드트립: 발행한 이벤트가 핸들러로 전달된다. */
export async function assertRoundTrip(mq: MessageQueue): Promise<void> {
  const received: string[] = [];
  await mq.subscribe(async (e) => { received.push(e.reservationId); });
  await mq.publish(sampleEvent('rt-1'));
  await waitFor(() => received.includes('rt-1'));
  expect(received).toContain('rt-1');
}

/** 핸들러가 처음 실패해도 재시도로 결국 성공 처리된다(at-least-once). */
export async function assertRetry(mq: MessageQueue): Promise<void> {
  let attempts = 0;
  await mq.subscribe(async () => {
    attempts++;
    if (attempts < 2) throw new Error('inject fail');
  });
  await mq.publish(sampleEvent('retry-1'));
  await waitFor(() => attempts >= 2);
  expect(attempts).toBeGreaterThanOrEqual(2);
}
```

- [ ] **Step 3: BullMQ 테스트 작성**

`test/mq/bullmq.test.ts`:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import type { StartedRedisContainer } from '@testcontainers/redis';
import type Redis from 'ioredis';
import { startRedis } from '../helpers/containers.js';
import { assertRoundTrip, assertRetry } from './contract.js';
import { BullMq } from '../../src/mq/bullmq.js';

describe('BullMq', () => {
  let container: StartedRedisContainer; let redis: Redis;
  beforeAll(async () => { ({ container, redis } = await startRedis()); });
  afterAll(async () => { redis.disconnect(); await container.stop(); });

  it('publish→subscribe 라운드트립', async () => {
    const mq = new BullMq(redis.options, 'rt');
    await assertRoundTrip(mq);
    await mq.close();
  });

  it('핸들러 실패 시 재시도', async () => {
    const mq = new BullMq(redis.options, 'retry');
    await assertRetry(mq);
    await mq.close();
  });
});
```

- [ ] **Step 4: 실패 확인**

Run: `npx vitest run test/mq/bullmq.test.ts`
Expected: FAIL — `BullMq` 없음.

- [ ] **Step 5: BullMQ 구현 (attempts/backoff 재시도, failed=DLQ)**

`src/mq/bullmq.ts`:

```ts
import { Queue, Worker, type RedisOptions } from 'bullmq';
import type { ConfirmEvent } from '../domain/types.js';
import type { ConfirmHandler, MessageQueue } from './message-queue.js';

export class BullMq implements MessageQueue {
  private readonly queue: Queue;
  private worker?: Worker;
  private readonly queueName: string;

  constructor(private readonly connection: RedisOptions, name = 'confirm') {
    this.queueName = `confirm-${name}`;
    this.queue = new Queue(this.queueName, { connection });
  }

  async publish(event: ConfirmEvent): Promise<void> {
    await this.queue.add('confirm', event, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 200 },
      removeOnComplete: false,
      removeOnFail: false, // failed 상태로 남겨 DLQ 역할
    });
  }

  async subscribe(handler: ConfirmHandler): Promise<void> {
    this.worker = new Worker(
      this.queueName,
      async (job) => { await handler(job.data as ConfirmEvent); },
      { connection: this.connection, concurrency: 16 },
    );
    await this.worker.waitUntilReady();
  }

  async close(): Promise<void> {
    await this.worker?.close();
    await this.queue.close();
  }
}
```

- [ ] **Step 6: 통과 확인**

Run: `npx vitest run test/mq/bullmq.test.ts`
Expected: PASS — 라운드트립·재시도 모두 통과.

- [ ] **Step 7: Commit**

```bash
git add src/mq/message-queue.ts test/mq/contract.ts src/mq/bullmq.ts test/mq/bullmq.test.ts
git commit -m "feat: MessageQueue 인터페이스 + 계약 슈트 + BullMQ 구현"
```

---

## Task 12: Kafka 구현

kafkajs 컨슈머. 수동 커밋 + 실패 시 재시도, 한도 초과 시 dead-letter 토픽으로 발행(DLQ).

**Files:**
- Create: `src/mq/kafka.ts`, `test/mq/kafka.test.ts`

- [ ] **Step 1: Kafka 테스트 작성 (동일 계약 슈트 재사용)**

`test/mq/kafka.test.ts`:

```ts
import { afterAll, beforeAll, describe, it } from 'vitest';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { assertRoundTrip, assertRetry } from './contract.js';
import { KafkaMq } from '../../src/mq/kafka.js';

describe('KafkaMq', () => {
  let container: StartedKafkaContainer;
  let brokers: string[];
  beforeAll(async () => {
    container = await new KafkaContainer('confluentinc/cp-kafka:7.5.0').withExposedPorts(9093).start();
    brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`];
  });
  afterAll(async () => { await container.stop(); });

  it('publish→subscribe 라운드트립', async () => {
    const mq = new KafkaMq(brokers, 'rt');
    await assertRoundTrip(mq);
    await mq.close();
  });

  it('핸들러 실패 시 재시도', async () => {
    const mq = new KafkaMq(brokers, 'retry');
    await assertRetry(mq);
    await mq.close();
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/mq/kafka.test.ts`
Expected: FAIL — `KafkaMq` 없음.

- [ ] **Step 3: Kafka 구현**

`src/mq/kafka.ts`:

```ts
import { Kafka, type Consumer, type Producer } from 'kafkajs';
import type { ConfirmEvent } from '../domain/types.js';
import type { ConfirmHandler, MessageQueue } from './message-queue.js';

const MAX_ATTEMPTS = 3;

export class KafkaMq implements MessageQueue {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private consumer?: Consumer;
  private readonly topic: string;
  private readonly dlqTopic: string;
  private readonly groupId: string;
  private readonly attempts = new Map<string, number>();

  constructor(brokers: string[], suffix = 'main') {
    this.kafka = new Kafka({ clientId: 'ticket', brokers });
    this.producer = this.kafka.producer();
    this.topic = `confirm-${suffix}`;
    this.dlqTopic = `confirm-${suffix}-dlq`;
    this.groupId = `confirm-group-${suffix}`;
  }

  async publish(event: ConfirmEvent): Promise<void> {
    await this.producer.connect();
    await this.producer.send({
      topic: this.topic,
      messages: [{ key: event.reservationId, value: JSON.stringify(event) }],
    });
  }

  async subscribe(handler: ConfirmHandler): Promise<void> {
    this.consumer = this.kafka.consumer({ groupId: this.groupId });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });
    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value!.toString()) as ConfirmEvent;
        const key = event.reservationId;
        const n = (this.attempts.get(key) ?? 0) + 1;
        this.attempts.set(key, n);
        try {
          await handler(event);
          this.attempts.delete(key);
        } catch (e) {
          if (n >= MAX_ATTEMPTS) {
            // DLQ로 이동 후 커밋 진행(무한 재시도 방지)
            await this.producer.send({
              topic: this.dlqTopic,
              messages: [{ key, value: message.value }],
            });
            this.attempts.delete(key);
          } else {
            throw e; // 커밋 안 됨 → kafkajs가 같은 메시지 재처리
          }
        }
      },
    });
  }

  async close(): Promise<void> {
    await this.consumer?.disconnect();
    await this.producer.disconnect();
  }
}
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/mq/kafka.test.ts`
Expected: PASS. (Kafka 컨테이너 기동에 시간이 걸리므로 첫 실행은 느릴 수 있음.)

- [ ] **Step 5: Commit**

```bash
git add src/mq/kafka.ts test/mq/kafka.test.ts
git commit -m "feat: Kafka 구현 (수동 재시도 + dead-letter 토픽)"
```

---

## Task 13: ConsumerWorker (발권 + 실패주입 + 멱등 + 메트릭)

MQ에 구독해 발권을 시뮬레이션. `FAIL_RATE`로 실패 주입, `reservationId`로 멱등 처리, 결과를 MetricsStore에 기록.

**Files:**
- Create: `src/consumer/consumer-worker.ts`, `test/consumer/consumer-worker.test.ts`

- [ ] **Step 1: 테스트 작성 (인메모리 fake MQ로 단위 테스트)**

`test/consumer/consumer-worker.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import type { ConfirmEvent } from '../../src/domain/types.js';
import type { ConfirmHandler, MessageQueue } from '../../src/mq/message-queue.js';
import { ConsumerWorker } from '../../src/consumer/consumer-worker.js';
import { MetricsStore } from '../../src/metrics/metrics-store.js';

class FakeMq implements MessageQueue {
  private handler?: ConfirmHandler;
  async publish(): Promise<void> {}
  async subscribe(h: ConfirmHandler): Promise<void> { this.handler = h; }
  async close(): Promise<void> {}
  async deliver(e: ConfirmEvent): Promise<void> { await this.handler!(e); }
}

function evt(id: string): ConfirmEvent {
  return { reservationId: id, eventId: 'e', userId: 'u', reservedAt: '2026-06-09T00:00:00Z' };
}

describe('ConsumerWorker', () => {
  it('정상 이벤트를 처리하고 메트릭에 기록한다', async () => {
    const mq = new FakeMq(); const metrics = new MetricsStore();
    const worker = new ConsumerWorker(mq, metrics, 0);
    await worker.start();
    await mq.deliver(evt('a'));
    expect(metrics.snapshot().processed).toBe(1);
  });

  it('같은 reservationId 재전달은 중복으로 처리한다(멱등)', async () => {
    const mq = new FakeMq(); const metrics = new MetricsStore();
    const worker = new ConsumerWorker(mq, metrics, 0);
    await worker.start();
    await mq.deliver(evt('dup'));
    await mq.deliver(evt('dup'));
    const s = metrics.snapshot();
    expect(s.processed).toBe(1);
    expect(s.duplicates).toBe(1);
  });

  it('FAIL_RATE=1 이면 예외를 던진다(재시도 유발)', async () => {
    const mq = new FakeMq(); const metrics = new MetricsStore();
    const worker = new ConsumerWorker(mq, metrics, 1);
    await worker.start();
    await expect(mq.deliver(evt('f'))).rejects.toThrow();
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/consumer/consumer-worker.test.ts`
Expected: FAIL — `ConsumerWorker` 없음.

- [ ] **Step 3: 구현**

`src/consumer/consumer-worker.ts`:

```ts
import type { ConfirmEvent } from '../domain/types.js';
import type { MessageQueue } from '../mq/message-queue.js';
import type { MetricsStore } from '../metrics/metrics-store.js';

export class ConsumerWorker {
  private readonly seen = new Set<string>();

  constructor(
    private readonly mq: MessageQueue,
    private readonly metrics: MetricsStore,
    private readonly failRate: number,
    private readonly rng: () => number = Math.random,
  ) {}

  async start(): Promise<void> {
    await this.mq.subscribe(async (event) => this.handle(event));
  }

  private async handle(event: ConfirmEvent): Promise<void> {
    if (this.seen.has(event.reservationId)) {
      this.metrics.recordDuplicate();
      return; // 멱등: 이미 처리됨
    }

    if (this.rng() < this.failRate) {
      this.metrics.recordRetry();
      throw new Error(`injected failure for ${event.reservationId}`);
    }

    const start = Date.parse(event.reservedAt);
    const latency = Math.max(0, Date.now() - start);
    this.seen.add(event.reservationId);
    this.metrics.recordProcessed(latency);
  }
}
```

> 멱등 처리는 성공 시에만 `seen`에 추가하므로, 실패→재시도 후 성공이 정상 동작한다. DLQ 카운트는 MQ 구현이 dead-letter로 보낼 때 별도 컨슈머에서 `recordDlq()`를 호출하거나, 통합 단계에서 dead-letter 토픽/failed 잡을 폴링해 집계한다(Task 16 bench에서 측정).

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/consumer/consumer-worker.test.ts`
Expected: PASS — 3개 모두 통과.

- [ ] **Step 5: Commit**

```bash
git add src/consumer/consumer-worker.ts test/consumer/consumer-worker.test.ts
git commit -m "feat: ConsumerWorker (발권 + 실패주입 + 멱등 + 메트릭)"
```

---

## Task 14: ReservationService + config factory

`ReservationService`는 전략·MQ 주입을 받아 예약→발행을 조율. `config`는 env로 구현체를 조립.

**Files:**
- Create: `src/domain/reservation-service.ts`, `test/domain/reservation-service.test.ts`, `src/config.ts`

- [ ] **Step 1: ReservationService 테스트 작성**

`test/domain/reservation-service.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import type { ConfirmEvent, ReserveResult } from '../../src/domain/types.js';
import type { ConcurrencyStrategy } from '../../src/strategies/strategy.js';
import type { ConfirmHandler, MessageQueue } from '../../src/mq/message-queue.js';
import { ReservationService } from '../../src/domain/reservation-service.js';

class StubStrategy implements ConcurrencyStrategy {
  constructor(private readonly result: ReserveResult) {}
  async decrementStock(): Promise<ReserveResult> { return this.result; }
}
class SpyMq implements MessageQueue {
  published: ConfirmEvent[] = [];
  async publish(e: ConfirmEvent): Promise<void> { this.published.push(e); }
  async subscribe(_h: ConfirmHandler): Promise<void> {}
  async close(): Promise<void> {}
}

describe('ReservationService', () => {
  it('예약 성공 시 확정 이벤트를 발행한다', async () => {
    const mq = new SpyMq();
    const svc = new ReservationService(
      new StubStrategy({ status: 'RESERVED', reservationId: 'r1' }), mq,
    );
    const res = await svc.reserve('evt-1', 'u1');
    expect(res.status).toBe('RESERVED');
    expect(mq.published).toHaveLength(1);
    expect(mq.published[0].reservationId).toBe('r1');
  });

  it('품절이면 이벤트를 발행하지 않는다', async () => {
    const mq = new SpyMq();
    const svc = new ReservationService(new StubStrategy({ status: 'SOLD_OUT' }), mq);
    const res = await svc.reserve('evt-1', 'u1');
    expect(res.status).toBe('SOLD_OUT');
    expect(mq.published).toHaveLength(0);
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npx vitest run test/domain/reservation-service.test.ts`
Expected: FAIL — `ReservationService` 없음.

- [ ] **Step 3: ReservationService 구현**

`src/domain/reservation-service.ts`:

```ts
import type { ReserveResult } from './types.js';
import type { ConcurrencyStrategy } from '../strategies/strategy.js';
import type { MessageQueue } from '../mq/message-queue.js';

export class ReservationService {
  constructor(
    private readonly strategy: ConcurrencyStrategy,
    private readonly mq: MessageQueue,
    private readonly now: () => string = () => new Date().toISOString(),
  ) {}

  async reserve(eventId: string, userId: string): Promise<ReserveResult> {
    const result = await this.strategy.decrementStock(eventId, userId);
    if (result.status === 'RESERVED') {
      await this.mq.publish({
        reservationId: result.reservationId,
        eventId,
        userId,
        reservedAt: this.now(),
      });
    }
    return result;
  }
}
```

- [ ] **Step 4: 통과 확인**

Run: `npx vitest run test/domain/reservation-service.test.ts`
Expected: PASS.

- [ ] **Step 5: config factory 구현 (테스트 없이 조립 코드)**

`src/config.ts`:

```ts
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import type { ConcurrencyStrategy } from './strategies/strategy.js';
import type { MessageQueue } from './mq/message-queue.js';
import { NaiveStrategy } from './strategies/naive.js';
import { DbAtomicStrategy } from './strategies/db-atomic.js';
import { DbPessimisticStrategy } from './strategies/db-pessimistic.js';
import { DbOptimisticStrategy } from './strategies/db-optimistic.js';
import { RedisDecrStrategy } from './strategies/redis-decr.js';
import { RedisLockStrategy } from './strategies/redis-lock.js';
import { BullMq } from './mq/bullmq.js';
import { KafkaMq } from './mq/kafka.js';

export function makeStrategy(name: string, pool: Pool, redis: Redis): ConcurrencyStrategy {
  switch (name) {
    case 'naive': return new NaiveStrategy(pool);
    case 'db-atomic': return new DbAtomicStrategy(pool);
    case 'db-pessimistic': return new DbPessimisticStrategy(pool);
    case 'db-optimistic': return new DbOptimisticStrategy(pool);
    case 'redis-decr': return new RedisDecrStrategy(pool, redis);
    case 'redis-lock': return new RedisLockStrategy(pool, redis);
    default: throw new Error(`unknown STRATEGY: ${name}`);
  }
}

export function makeMq(name: string, redis: Redis): MessageQueue {
  switch (name) {
    case 'bullmq': return new BullMq(redis.options, 'main');
    case 'kafka': return new KafkaMq((process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','), 'main');
    default: throw new Error(`unknown MQ: ${name}`);
  }
}
```

- [ ] **Step 6: 타입 체크**

Run: `npx tsc --noEmit -p tsconfig.json`
Expected: 에러 없음.

- [ ] **Step 7: Commit**

```bash
git add src/domain/reservation-service.ts test/domain/reservation-service.test.ts src/config.ts
git commit -m "feat: ReservationService + env 기반 config factory"
```

---

## Task 15: Express 서버 + 워커 엔트리포인트

**Files:**
- Create: `src/server.ts`, `src/worker.ts`

- [ ] **Step 1: server.ts 작성**

`src/server.ts`:

```ts
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
    const result = await service.reserve(eventId, userId);
    res.status(result.status === 'RESERVED' ? 201 : 409).json(result);
  });

  const port = Number(process.env.PORT ?? 3000);
  app.listen(port, () => console.log(`[server] strategy=${strategyName} mq=${process.env.MQ ?? 'bullmq'} :${port}`));
}

main().catch((e) => { console.error(e); process.exit(1); });
```

- [ ] **Step 2: worker.ts 작성**

`src/worker.ts`:

```ts
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
```

- [ ] **Step 3: 수동 스모크 테스트 (인프라 기동 상태)**

Run:
```bash
docker compose up -d
STRATEGY=db-atomic MQ=bullmq npx tsx src/server.ts &
MQ=bullmq npx tsx src/worker.ts &
sleep 3
curl -s -XPOST localhost:3000/seed -H 'content-type: application/json' -d '{"eventId":"e1","stock":2}'
curl -s -XPOST localhost:3000/reserve -H 'content-type: application/json' -d '{"eventId":"e1","userId":"u1"}'
```
Expected: `/seed` → `{"ok":true,...}`, `/reserve` → `{"status":"RESERVED",...}`, worker 로그에 `[metrics]` processed 증가.
정리: `kill %1 %2`.

- [ ] **Step 4: Commit**

```bash
git add src/server.ts src/worker.ts
git commit -m "feat: Express /reserve 서버 + 컨슈머 워커 엔트리포인트"
```

---

## Task 16: 벤치마크 — 동시 예약 부하 + 불변식 검증 + 비교표

**Files:**
- Create: `bench/load-reserve.ts`, `bench/report.ts`, `docs/RESULTS.md`

- [ ] **Step 1: load-reserve.ts 작성**

`bench/load-reserve.ts` — 지정 재고를 시드하고 N개 동시 요청을 쏜 뒤 불변식과 지연을 측정.

```ts
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
```

- [ ] **Step 2: 전략별 벤치 실행 절차 문서화 + report 스켈레톤**

`bench/report.ts` — 전략 목록을 돌며 서버를 띄우는 건 수동이 단순하므로, 이 스크립트는 사용법을 출력하는 가이드 역할.

```ts
const STRATEGIES = ['naive', 'db-atomic', 'db-pessimistic', 'db-optimistic', 'redis-decr', 'redis-lock'];

console.log('# 동시성 전략 벤치마크 실행 가이드\n');
console.log('각 전략마다 서버를 해당 STRATEGY로 띄운 뒤 bench:reserve를 실행하고 결과를 docs/RESULTS.md에 모은다.\n');
for (const s of STRATEGIES) {
  console.log(`## ${s}`);
  console.log('```bash');
  console.log(`STRATEGY=${s} MQ=bullmq npx tsx src/server.ts &`);
  console.log(`STOCK=100 N=1000 EVENT=bench-${s} npx tsx bench/load-reserve.ts`);
  console.log('kill %1');
  console.log('```\n');
}
```

- [ ] **Step 3: RESULTS.md 템플릿 작성**

`docs/RESULTS.md`:

```markdown
# 실험 결과

## 동시성 전략 비교 (재고 100, 동시 요청 1000)

| 전략 | reserved | oversell | invariantOk | throughput(rps) | p50 | p95 | p99 |
|------|----------|----------|-------------|-----------------|-----|-----|-----|
| naive | (채움) | (>0 예상) | false | | | | |
| db-atomic | 100 | 0 | true | | | | |
| db-pessimistic | 100 | 0 | true | | | | |
| db-optimistic | 100 | 0 | true | | | | |
| redis-decr | 100 | 0 | true | | | | |
| redis-lock | 100 | 0 | true | | | | |

> naive는 오버셀 발생(버그), 나머지는 불변식 통과. 처리량은 redis-decr > db-atomic > 락 계열 순으로 예상.

## Kafka vs BullMQ 비교

| 항목 | Kafka | BullMQ |
|------|-------|--------|
| 모델 | 로그 기반 pull, 파티션 오프셋 | Redis 기반 잡 큐, push |
| 전달 보장 | at-least-once (수동 커밋) | at-least-once (attempts) |
| 재시도 | 커밋 보류로 같은 메시지 재처리 | attempts + backoff |
| DLQ | dead-letter 토픽 | failed 상태/전용 큐 |
| 처리량(N건) | (채움) | (채움) |
| 지연 p95 | (채움) | (채움) |

> FAIL_RATE>0 으로 재시도/DLQ/중복 카운트를 worker [metrics] 로그에서 수집해 채운다.
```

- [ ] **Step 4: 벤치 1회 실행 확인 (db-atomic 예시)**

Run:
```bash
docker compose up -d
STRATEGY=db-atomic MQ=bullmq npx tsx src/server.ts &
sleep 3
STOCK=100 N=1000 EVENT=bench-atomic npx tsx bench/load-reserve.ts
kill %1
```
Expected: `reserved: 100, oversell: 0, invariantOk: true` + 처리량/지연 수치 출력.

- [ ] **Step 5: Commit**

```bash
git add bench/load-reserve.ts bench/report.ts docs/RESULTS.md
git commit -m "feat: 동시 예약 부하 벤치 + 불변식 검증 + 결과 템플릿"
```

---

## Task 17: 전체 테스트 + 문서 마무리

**Files:**
- Create: `README.md`

- [ ] **Step 1: 전체 테스트 실행**

Run: `npx vitest run`
Expected: 모든 전략/MQ/도메인/메트릭 테스트 PASS. (Kafka 통합 테스트로 수 분 소요 가능.)

- [ ] **Step 2: README 작성 (실행/실험 가이드)**

`README.md`에 다음을 포함:
- 목적(동시성 학습 + Kafka vs BullMQ 비교)
- `docker compose up -d` → `npm run server` / `npm run worker` 실행법
- env 스위치(`STRATEGY`, `MQ`, `FAIL_RATE`) 표
- 벤치 실행법(`npx tsx bench/report.ts`로 가이드 출력 → 전략별 실행)
- 결과는 `docs/RESULTS.md` 참고, 설계는 `docs/superpowers/specs/2026-06-09-ticket-concurrency-mq-design.md` 참고

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README 실행/실험 가이드"
```

---

## Self-Review 확인 사항 (작성자 체크 완료)

- **Spec 커버리지**: 6개 동시성 전략(Task 4–9), Kafka/BullMQ(Task 11–12), 컨슈머 실패주입·멱등·DLQ(Task 13), 메트릭(Task 10), 벤치·불변식(Task 16) — spec 9개 섹션 모두 태스크로 매핑됨.
- **타입 일관성**: `ReserveResult`/`ConfirmEvent`(Task 3), `ConcurrencyStrategy.decrementStock`/`MessageQueue.publish|subscribe|close` 시그니처가 모든 구현·테스트에서 동일하게 사용됨.
- **Placeholder 없음**: 모든 코드 스텝에 실제 코드 포함. RESULTS.md의 "(채움)"은 의도된 측정 결과 칸(런타임 산출물)으로 코드 placeholder 아님.
- **주의 사항**: 통합 테스트는 Docker 필요. naive 오버셀은 확률적이므로 미발생 시 concurrency 상향.
