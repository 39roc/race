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
