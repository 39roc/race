# 실험 결과

## 동시성 전략 비교 (재고 100, 동시 요청 1000)

| 전략 | reserved | oversell | invariantOk | throughput(rps) | p50 | p95 | p99 |
|------|----------|----------|-------------|-----------------|-----|-----|-----|
| naive | 1000 | 900 | false | 842 | 1066 | 1141 | 1147 |
| db-atomic | 100 | 0 | true | 2162 | 360 | 428 | 445 |
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

> ⚠️ 재시도/DLQ 구현 계층 차이(주의): Kafka의 재시도 횟수는 KafkaMq의 인메모리 Map으로 추적되어 프로세스 재시작/리밸런스 시 사라지고, 앱 코드가 MAX_ATTEMPTS=3을 강제한다. 반면 BullMQ의 attempts:3은 Redis에 영속되어 재시작에도 살아남고 브로커(BullMQ)가 직접 강제한다. 즉 둘 다 at-least-once지만 재시도/DLQ가 구현되는 계층이 다르므로, 위 표의 재시도/DLQ 수치를 Kafka와 BullMQ 간 1:1로 비교할 때는 주의해야 한다.

> ⚠️ redis-decr 전략(주의): redis-decr는 재고 권위를 Redis(`stock:{eventId}`)에 두고 예약 행은 Postgres에 적재한다(나머지 전략은 재고 권위가 Postgres). 따라서 전략을 전환할 때는 `/seed`로 재고를 다시 초기화해야 한다.
