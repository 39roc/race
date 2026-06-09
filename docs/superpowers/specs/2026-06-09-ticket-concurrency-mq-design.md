# 선착순 티켓 시스템 — 동시성 학습 & Kafka vs BullMQ 비교 설계

- 작성일: 2026-06-09
- 상태: 승인 대기 (사용자 검토 전)

## 1. 목적

학습용 프로젝트로서 두 가지를 다룬다.

1. **동시성 학습** — 선착순 티켓 재고 차감을 여러 동시성 제어 방식으로 구현·비교하며 레이스 컨디션의 발생과 방지를 체감한다.
2. **MQ 비교** — 예약 성공 후 후속 처리(발권)를 Kafka와 BullMQ로 각각 구현해, 전달 보장/재시도/실패 처리와 처리량/지연을 비교한다.

개발 방식은 **TDD(Red → Green → Refactor)** 를 모든 유닛에 적용한다.

## 2. 기술 스택

- 언어/런타임: Node.js + TypeScript
- HTTP 서버: Express (전략 스위칭 진입점 `/reserve`)
- 데이터 저장소: PostgreSQL(관계형 락), Redis(원자 연산/분산락/BullMQ 백엔드)
- 메시지 큐: Apache Kafka, BullMQ
- 테스트: Vitest + Testcontainers
- 인프라: Docker Compose (postgres, redis, kafka(KRaft), kafka-ui)

## 3. 아키텍처

두 개의 독립 교체 축을 가진 단일 티켓 예약 앱.

```
                  POST /reserve {eventId, userId}
                              │
                     ┌────────▼─────────┐
                     │  ReservationSvc   │  (도메인 핵심)
                     └───┬──────────┬────┘
            STRATEGY 축  │          │  MQ 축
        ┌────────────────▼──┐   ┌───▼──────────────┐
        │ ConcurrencyStrategy│   │  MessageQueue     │
        │  - naive           │   │   - kafka         │
        │  - db-pessimistic  │   │   - bullmq        │
        │  - db-optimistic   │   └───┬──────────────┘
        │  - db-atomic       │       │ enqueue(ConfirmEvent)
        │  - redis-decr      │   ┌───▼──────────────┐
        │  - redis-lock      │   │  Consumer Worker  │
        └───┬────────────────┘   │  (발권/실패주입)   │
            │ decrement stock     └───┬──────────────┘
   ┌────────▼────────┐                │ record metrics
   │ Postgres / Redis │          ┌────▼─────────┐
   └─────────────────┘          │ MetricsStore  │
                                 └──────────────┘
```

- `STRATEGY` 축과 `MQ` 축은 서로 독립적으로 env로 선택·조립한다.
- `ReservationService`는 구체 구현을 모르며 인터페이스에만 의존(DI).

## 4. 컴포넌트

각 유닛은 하나의 책임을 가지며 인터페이스로 소통한다.

### `ReservationService`
- 도메인 진입점. `reserve(eventId, userId): Promise<ReserveResult>`.
- 선택된 `ConcurrencyStrategy`로 재고를 차감하고, 성공 시 `MessageQueue`로 `ConfirmEvent`를 발행.
- 어떤 전략/MQ인지 모름(주입받음).

### `ConcurrencyStrategy` (인터페이스 + 6 구현)
- 계약: `decrementStock(eventId, userId): Promise<ReserveResult>`.
- 구현:
  - `naive` — 제어 없는 read-modify-write. 오버셀이 의도적으로 발생.
  - `db-pessimistic` — `SELECT ... FOR UPDATE`.
  - `db-optimistic` — version 컬럼 기반 CAS, 충돌 시 제한 횟수 재시도.
  - `db-atomic` — `UPDATE ... SET stock = stock - 1 WHERE stock > 0`.
  - `redis-decr` — Redis 원자적 `DECR`(또는 Lua 스크립트로 경계 검사).
  - `redis-lock` — 분산락(Redlock) 후 차감, lock TTL 필수.
- 각 구현은 Postgres 또는 Redis 중 하나에만 의존.

### `MessageQueue` (인터페이스 + 2 구현)
- 계약: `publish(event: ConfirmEvent): Promise<void>`, `subscribe(handler): Promise<void>`.
- 구현: `kafka`, `bullmq`. 동일한 `ConfirmEvent` 계약 사용.

### `ConsumerWorker`
- `ConfirmEvent`를 소비해 발권 처리를 시뮬레이션.
- `FAIL_RATE`(0~1) 확률로 예외를 던져 재시도/DLQ 경로를 학습.
- 멱등성: `reservationId`로 중복 소비를 감지(처리 기록 테이블/Redis SET).
- 처리 결과(성공/중복/실패/DLQ)와 타임스탬프를 `MetricsStore`에 기록.

### `MetricsStore`
- produce/consume 타임스탬프, 성공/중복/DLQ/재시도 카운트 수집.
- 벤치마크 리포트(처리량, p50/p95/p99) 생성에 사용.

### `config` / `factory`
- env(`STRATEGY`, `MQ`, `FAIL_RATE`, 재고/부하 파라미터)로 구현체를 선택·조립.

### 부하/검증 스크립트 (`bench/`)
- 제한된 재고에 동시 요청 N개를 발사.
- **불변식 검증**: 판매 수 == 초기 재고, 오버셀 0.
- 처리량·p50/p95/p99 측정 후 비교표 출력.

## 5. 데이터 흐름

1. 클라이언트가 동시에 `/reserve`를 다수 호출(재고 < 요청 수).
2. `ReservationService` → 선택된 `ConcurrencyStrategy`로 재고 차감.
3. 성공 → `ConfirmEvent`를 MQ에 발행 / 실패(품절) → 즉시 `409 SOLD_OUT`.
4. `ConsumerWorker`가 이벤트 소비 → 발권 처리(`FAIL_RATE` 확률로 예외) → 재시도/DLQ.
5. 모든 단계의 타임스탬프·결과를 `MetricsStore`에 기록 → 리포트 스크립트가 비교표 출력.

## 6. 에러 처리

- **품절(정상 거절)**: 재고 0 → `409 Conflict` + `{ status: 'SOLD_OUT' }`. 예외 아님.
- **오버셀(버그)**: `naive`에서만 발생하도록 의도. 불변식 검증이 "판매 수 > 재고"를 감지·리포트.
- **낙관적 락 충돌**: version 불일치 → 제한 횟수 재시도 후 실패 시 거절. 재시도 횟수를 메트릭 기록.
- **분산락 실패/타임아웃**: Redlock TTL·재시도 정책으로 처리. 데드락 방지를 위해 lock TTL 필수.
- **컨슈머 처리 실패**: `FAIL_RATE` 주입 → Kafka는 오프셋 커밋 보류 후 재처리 / BullMQ는 `attempts`·`backoff` 재시도 → 한도 초과 시 DLQ(Kafka: dead-letter 토픽, BullMQ: failed 상태/전용 큐).
- **멱등성**: at-least-once로 중복 소비 가능 → consumer가 `reservationId`로 중복 감지, 중복 카운트 기록.

## 7. TDD 테스트 전략

모든 유닛에 Red → Green → Refactor 적용.

- **단위 테스트(Vitest)**: 각 `ConcurrencyStrategy`를 인터페이스 계약으로 테스트. 동일한 "동시 차감" 슈트를 모든 구현에 재사용.
- **동시성 테스트**: `Promise.all`로 N개 동시 차감을 실제 DB/Redis에 실행 → "정확히 재고만큼만 성공" 불변식 검증. 각 락 전략이 통과해야 Green. `naive`는 오버셀이 발생함을 명시적으로 문서화(의도된 실패).
- **MQ 계약 테스트**: `MessageQueue` 인터페이스를 Kafka/BullMQ 양쪽에 동일 슈트로 — publish→subscribe 라운드트립, 재시도, DLQ 진입, 멱등 중복 처리.
- **통합 테스트(Testcontainers)**: Postgres/Redis/Kafka를 컨테이너로 띄워 실제 환경 검증. CI 재현 가능.
- **벤치마크는 테스트가 아님**: 측정·리포트는 별도 스크립트. 단, 불변식 검증은 테스트로 유지.

## 8. 프로젝트 구조

```
ticket/
├── docker-compose.yml          # postgres, redis, kafka(KRaft), kafka-ui
├── src/
│   ├── domain/                 # ReservationService, 타입(ReserveResult, ConfirmEvent)
│   ├── strategies/             # naive, db-*, redis-* + 공용 인터페이스
│   ├── mq/                     # kafka.ts, bullmq.ts + MessageQueue 인터페이스
│   ├── consumer/               # ConsumerWorker, 실패주입, DLQ
│   ├── metrics/                # MetricsStore, 리포트 생성
│   ├── config.ts               # env 기반 factory
│   ├── server.ts               # Express /reserve
│   └── worker.ts               # 컨슈머 엔트리포인트
├── test/                       # 단위/동시성/계약/통합 (구현과 미러링)
├── bench/                      # 부하 스크립트, 불변식 검증, 비교표 출력
├── db/migrations/              # events/reservations 스키마
└── docs/                       # 실험 결과 정리(동시성 비교표, Kafka vs BullMQ)
```

## 9. 벤치마크 설계

- **동시성 시나리오**: 재고 100, 동시 요청 1,000 → 전략별 (성공/오버셀/거절, throughput, p50/p95/p99) 표.
- **MQ 시나리오**: 확정 이벤트 N건 발행 → 소비 완료까지 throughput·지연, `FAIL_RATE` 주입 시 (재시도 횟수, DLQ 수, 중복 수) 비교.
- **도구**: 동시 요청은 커스텀 Node 스크립트(`Promise.all` + undici) 또는 autocannon. 결과는 `MetricsStore`에서 집계해 콘솔 표 + `docs/`에 마크다운.

## 10. 범위 밖 (YAGNI)

- 실제 결제 연동, 인증/인가, 좌석 선택 UI.
- 프로덕션 배포, 오토스케일, 모니터링 대시보드.
- MQ 직렬화를 동시성 제어 수단으로 쓰는 방식(이번 학습 범위에서 제외).
- Kafka 파티션 순서 보장/재생을 깊게 다루는 실험(전달 보장·벤치마크에 집중).
