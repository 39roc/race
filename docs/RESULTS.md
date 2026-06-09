# 실험 결과

## 동시성 전략 비교 (재고 100, 총 요청 1000, 연결 풀 200, MQ=bullmq)

> 측정 조건: `STOCK=100 N=1000 CONNECTIONS=200`. keep-alive 연결 200개로 다중화해 서버에 동시 ~200 요청이 가해진다. 지연 단위는 ms. (로컬 1회 측정치이므로 절대값보다 전략 간 상대 비교로 읽을 것.)

| 전략 | reserved | oversell | invariantOk | throughput(rps) | p50 | p95 | p99 |
|------|----------|----------|-------------|-----------------|-----|-----|-----|
| naive | 1000 | 900 | false | 532 | 1148 | 1811 | 1852 |
| db-atomic | 100 | 0 | true | 2472 | 297 | 385 | 387 |
| db-pessimistic | 100 | 0 | true | 1650 | 427 | 578 | 588 |
| db-optimistic | 100 | 0 | true | 353 | 2751 | 2818 | 2820 |
| redis-decr | 100 | 0 | true | 3481 | 203 | 263 | 275 |
| redis-lock | 100 | 0 | true | 765 | 656 | 1085 | 1197 |

**관찰:**
- **naive만 오버셀 발생** — 재고 100에 1000건이 전부 성공(oversell 900). 동시성 제어가 없으면 선착순이 깨진다는 걸 수치로 보여준다. 나머지 5종은 모두 정확히 100건만 예약(불변식 통과).
- **처리량 순위:** redis-decr(3481) > db-atomic(2472) > db-pessimistic(1650) > redis-lock(765) > naive(532) > db-optimistic(353).
- **redis-decr가 가장 빠름** — Redis 인메모리 원자 연산(Lua) 한 번으로 게이트를 통과하고, 품절이면 DB를 건드리지 않는다.
- **db-atomic이 그 다음** — 단일 조건부 UPDATE의 원자성만으로 충분해 락 오버헤드가 없다.
- **db-optimistic이 가장 느림(의외)** — 한 행의 version을 두고 ~200건이 경합하면 CAS 충돌→재시도가 폭증한다. 경합이 심한 선착순에는 낙관적 락이 불리함을 체감할 수 있는 대표 사례.
- **락 계열(pessimistic/redis-lock)** — 직렬화 비용으로 중간 수준. naive가 락 계열보다도 느린 건 1000건을 전부 INSERT+MQ publish까지 하기 때문(거절이 하나도 없어 일을 가장 많이 한다).

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
