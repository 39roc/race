# 선착순 티켓 동시성 + MQ 비교

## 목적

선착순 티켓 시스템을 소재로 **동시성 제어 6가지 방식**을 비교 학습하고, 예약 성공 이후의 **후속 발권 처리**를 **Kafka**와 **BullMQ**로 각각 구현해 전달 보장 / 재시도 / 처리량을 비교하는 학습 프로젝트.

## 아키텍처 한 줄 요약

단일 Express 앱에 두 개의 교체 축을 둔다: `ConcurrencyStrategy`(재고 차감 방식)와 `MessageQueue`(Kafka/BullMQ). 둘 다 env로 조립한다. 흐름은 `POST /reserve` → 전략으로 재고 차감 → 성공 시 MQ로 확정 이벤트 발행 → `ConsumerWorker`가 발권을 처리한다.

## 요구사항

- Docker
- Node.js 20+

## 실행법

```bash
# 1) 인프라 기동 (postgres 호스트포트 5433, redis 6379, kafka 9092, kafka-ui 8080)
docker compose up -d

# 2) 서버 (기본값으로)
npm run server
# 또는 전략/큐를 지정
STRATEGY=db-atomic MQ=bullmq npm run server

# 3) 워커
npm run worker
# 또는
MQ=bullmq FAIL_RATE=0 npm run worker

# 4) 재고 시드
curl -XPOST localhost:3000/seed -H 'content-type: application/json' -d '{"eventId":"e1","stock":100}'

# 5) 예약
curl -XPOST localhost:3000/reserve -H 'content-type: application/json' -d '{"eventId":"e1","userId":"u1"}'

# 6) 종료
docker compose down
```

## 환경변수

| 변수 | 값 | 설명 |
|------|-----|------|
| `STRATEGY` | `naive` \| `db-atomic` \| `db-pessimistic` \| `db-optimistic` \| `redis-decr` \| `redis-lock` | 동시성 제어 방식 (기본 `db-atomic`) |
| `MQ` | `kafka` \| `bullmq` | 메시지 큐 (기본 `bullmq`) |
| `FAIL_RATE` | `0`~`1` | 컨슈머 실패 주입 확률 (재시도/DLQ 학습용, 기본 `0`) |
| `PORT` | `3000` | 서버 포트 |
| `PG_URL` / `REDIS_URL` / `KAFKA_BROKERS` | 연결 문자열 | 각 인프라 연결 문자열 (기본값은 `.env.example` 참고) |

## 테스트

```bash
npm test
```

Vitest + Testcontainers 기반이라 **Docker가 필요**하다. 각 동시성 전략은 "재고만큼만 예약(오버셀 0)" 불변식으로 검증되며, `naive`는 의도적으로 오버셀이 발생함을 문서화한다. MQ는 동일한 계약 슈트(라운드트립 + 재시도)로 Kafka/BullMQ 양쪽을 검증한다.

## 벤치마크

```bash
# 전략별 실행 가이드 출력
npx tsx bench/report.ts

# 각 전략으로 서버를 띄운 뒤
STOCK=100 N=1000 npx tsx bench/load-reserve.ts
```

결과는 `docs/RESULTS.md`에 정리한다. (실측 예: `db-atomic`은 reserved 100 / oversell 0, `naive`는 oversell 발생.)

## 문서 링크

- 설계 spec: [`docs/superpowers/specs/2026-06-09-ticket-concurrency-mq-design.md`](docs/superpowers/specs/2026-06-09-ticket-concurrency-mq-design.md)
- 구현 계획: [`docs/superpowers/plans/2026-06-09-ticket-concurrency-mq.md`](docs/superpowers/plans/2026-06-09-ticket-concurrency-mq.md)
- 실험 결과: [`docs/RESULTS.md`](docs/RESULTS.md)
