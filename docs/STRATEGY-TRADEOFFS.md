# 동시성 전략별 문제점 정리

선착순 티켓 재고 차감을 6가지 방식으로 구현했다. 각 전략이 **실제로 가질 수 있는 문제점**을 구현 코드(`src/strategies/`) 기준으로 정리한다. 벤치 수치는 `docs/RESULTS.md` 참고(재고 100, 총 1000 요청, 연결 풀 200).

## 한눈에 보기

| 전략 | 오버셀 | 핵심 약점 | 한 줄 요약 |
|------|--------|-----------|-----------|
| naive | **발생** | TOCTOU 레이스 | 제어가 없어 선착순이 깨짐(반면교사) |
| db-atomic | 없음 | 비트랜잭션 2쿼리, 단일 핫로우 | 단순 차감엔 최적, 확장·내구성 약함 |
| db-pessimistic | 없음 | 직렬화·커넥션 점유 | 안전하지만 핫이벤트에서 병목 |
| db-optimistic | 없음 | 재시도 폭증 | 고경합 선착순엔 최악의 처리량 |
| redis-decr | 없음 | 이중 저장소 불일치·Redis 내구성 | 가장 빠르나 정합성 관리 부담 |
| redis-lock | 없음 | 분산락 안전성·TTL 튜닝 | 직렬화 비용 + 락 정확성 함정 |

> 공통 한계 두 가지를 먼저 기억하면 좋다.
> - **단일 핫 로우(hot row):** naive·db-atomic·db-pessimistic·db-optimistic·redis-lock은 모두 재고 권위를 DB의 *한 행*에 둔다. 한 이벤트에 트래픽이 몰리면 그 행이 병목이자 단일 장애점이 된다. 이를 벗어나는 건 재고를 Redis로 옮기는 redis-decr뿐이다(대신 이중 저장소 문제가 생긴다).
> - **차감과 예약 적재의 원자성:** "재고 차감"과 "예약 행 INSERT"를 한 트랜잭션으로 묶지 않으면, 둘 사이의 실패가 *유령 차감*(재고는 줄었는데 예약은 없음)을 만든다. 트랜잭션으로 감싼 전략은 db-pessimistic뿐이다.

---

## 1. naive — 제어 없음

```ts
const read = await pool.query('SELECT stock FROM events WHERE id = $1', [eventId]);
const stock = read.rows[0]?.stock;
if (stock <= 0) return { status: 'SOLD_OUT' };
await pool.query('UPDATE events SET stock = $1 WHERE id = $2', [stock - 1, eventId]); // 앱이 읽은 값으로 덮어씀
```

**문제점**
- **TOCTOU 레이스(오버셀):** SELECT와 UPDATE 사이에 `await` 경계가 있다. 동시 요청들이 같은 `stock`을 읽고 모두 `if` 가드를 통과 → 전부 예약 성공. 벤치에서 재고 100에 **1000건 전부 성공(oversell 900)**.
- **Lost Update(갱신 유실):** `stock = $1`로 앱이 읽은 값을 통째로 덮어쓴다. 동시 차감이 서로의 결과를 덮어써 재고 값 자체가 신뢰 불가가 된다.
- **선착순 보장 실패:** 동시성 제어가 없으므로 "먼저 온 N명만"이라는 핵심 요구가 깨진다.

**용도:** 절대 운영용이 아님. *왜 동시성 제어가 필요한가*를 보여주는 학습용 반면교사.

---

## 2. db-atomic — 조건부 원자 UPDATE

```ts
const upd = await pool.query(
  'UPDATE events SET stock = stock - 1 WHERE id = $1 AND stock > 0 RETURNING stock', [eventId]);
if (upd.rowCount === 0) return { status: 'SOLD_OUT' };
const ins = await pool.query('INSERT INTO reservations ...'); // 별도 쿼리
```

**문제점**
- **비트랜잭션 2쿼리 → 유령 차감:** UPDATE와 INSERT가 분리된 autocommit 쿼리다. 사이에 크래시/INSERT 실패가 나면 재고는 깎였는데 예약 행이 없다. 계약 테스트의 `reserved === persisted`도 이 케이스는 못 잡는다(테스트 중엔 크래시가 안 나므로).
- **단일 핫 로우 직렬화:** 모든 예약이 같은 행에 UPDATE → 행 배타 락에서 줄을 선다. 지금은 빠르지만(2472 rps) 초대형 트래픽에선 한 행이 병목.
- **품절 트래픽도 DB를 때림:** `WHERE stock > 0`이 거짓이어도 쿼리는 DB까지 도달. 매진 후 몰리는 "꽝" 요청이 전부 DB 자원을 소모.
- **로직 확장 한계:** SQL 한 줄로 표현되는 단순 차감엔 완벽하지만, 유저당 한도·우선순위·좌석 선택 같은 read-modify-write가 끼면 단일 UPDATE로 못 담는다.
- **중복요청 무방비:** `reservations`에 `(event_id, user_id)` 유니크 제약이 없어 같은 유저의 두 번 클릭이 두 번 차감된다.

**완화책:** 두 쿼리를 `BEGIN/COMMIT`으로 묶어 유령 차감 제거(처리량은 약간 희생). 유저 유니크 제약 추가로 중복 차감 방지.

---

## 3. db-pessimistic — 비관적 락 (SELECT … FOR UPDATE)

```ts
await client.query('BEGIN');
const read = await client.query('SELECT stock FROM events WHERE id = $1 FOR UPDATE', [eventId]);
// ... UPDATE ... INSERT ...
await client.query('COMMIT');
```

**문제점**
- **처리량 병목:** `FOR UPDATE`가 행을 잠가 트랜잭션을 한 번에 하나씩만 진행시킨다. 정확하지만 한 이벤트에 몰린 요청은 전부 직렬화된다(벤치 1650 rps).
- **락 보유 시간이 김:** 락을 잡은 채 UPDATE+INSERT까지 한 트랜잭션 안에서 끝낸다. INSERT가 느려지면 락 보유가 길어져 뒤 대기열 전체가 더 밀린다.
- **커넥션 풀 고갈:** 각 요청이 트랜잭션 동안 커넥션 하나를 점유한다. 풀 `max: 50`인데 1000명이 몰리면 950명은 커넥션을 기다린다 → 락 대기 + 커넥션 대기 이중 큐.
- **데드락 가능성(설계에 따라):** 지금은 단일 행만 잠그므로 데드락이 없지만, 여러 자원을 일관되지 않은 순서로 잠그도록 확장하면 데드락이 생긴다.

**완화책:** 트랜잭션을 짧게(락 잡고 최소 작업만), 풀 크기·`lock_timeout` 튜닝, 핫 이벤트는 샤딩/대기열로 분산.

---

## 4. db-optimistic — 낙관적 락 (version CAS)

```ts
for (let attempt = 0; attempt <= maxRetries; attempt++) {
  const { stock, version } = await read();
  if (stock <= 0) return SOLD_OUT;
  const upd = await pool.query(
    'UPDATE events SET stock=stock-1, version=version+1 WHERE id=$1 AND version=$2', [eventId, version]);
  if (upd.rowCount === 1) return RESERVED;   // 성공
  // version 충돌 → 재시도
}
return SOLD_OUT; // 재시도 소진
```

**문제점**
- **재시도 폭증(retry storm):** 한 행의 version에 수백 건이 경합하면 대부분의 CAS가 실패 → 재시도. 실패한 시도 하나하나가 *읽기+쓰기 DB 왕복*이라 경합이 셀수록 낭비 작업이 증폭된다. 벤치에서 **가장 느림(353 rps, p50 2751ms)** — 고경합 단일 품목 선착순에 낙관적 락이 불리하다는 대표 사례.
- **공정성/기아(starvation):** 운 나쁜 요청은 계속 충돌에서 져 늦게 처리되거나 재시도를 소진한다. "먼저 온 순서"가 보장되지 않는다.
- **재시도 소진 시 의미 혼동:** 한도를 넘기면 `SOLD_OUT`을 반환하는데, 실제론 *재고가 남았는데 못 잡은 것*이다("거짓 품절"). 응답 의미가 모호.
- **튜닝 취약성:** `maxRetries`가 작으면 거짓 품절이 늘고, 크면 폭주 시 지연이 폭발. 경합 수준에 따라 적정값이 달라 깨지기 쉽다.

**용도:** 경합이 *낮은* 일반 업데이트엔 훌륭하지만, 한 품목에 몰리는 선착순엔 부적합.

---

## 5. redis-decr — Redis 원자 차감 (재고 권위를 Redis로)

```ts
const ok = await redis.eval(DECR_IF_POSITIVE, 1, `stock:${eventId}`); // 0보다 크면 DECR
if (ok !== 1) return SOLD_OUT;
await pool.query('INSERT INTO reservations ...'); // 예약 행은 Postgres
```

**문제점**
- **이중 저장소 불일치(split-brain):** 재고 권위는 Redis(`stock:{eventId}`), 예약 행은 Postgres. 두 저장소에 걸친 트랜잭션이 없다. Redis DECR은 성공했는데 Postgres INSERT가 실패/크래시하면 → **Redis 카운트는 깎였는데 예약 행은 없음**, 두 저장소가 영구히 어긋난다. 보정(reconciliation) 로직이 필요.
- **Redis 내구성:** Redis가 재시작(영속화 미설정)·페일오버로 데이터를 잃으면 재고 카운터가 초기화/유실 → 오버셀 또는 언더셀. 전략 전환·재기동 시 `/seed`로 재고를 다시 맞춰야 한다.
- **단일 장애점:** 재고 권위가 단일 Redis에 집중. Redis가 죽으면 예약 자체가 멈춘다.
- **취소/환불 동기화:** 예약 취소 시 Redis 카운트를 되돌려야 하는데, 그 경로가 또 다른 desync 지점이 된다.

**강점(맥락):** 인메모리 원자 연산이라 가장 빠르고(3481 rps), 품절 요청이 DB에 닿지 않아 DB 부하를 막는다 — 단, 위 정합성 부담을 감수해야 한다.

---

## 6. redis-lock — Redis 분산락 (SET NX PX + Lua 해제)

```ts
const token = `${userId}-${Date.now()}-${Math.random()}`;
// SET key token NX PX ttl 를 retryCount 만큼 재시도해 획득
// ... 락 보유 중 Postgres 재고 읽고 차감 ...
await redis.eval(RELEASE, 1, key, token); // 토큰 일치 시에만 DEL
```

**문제점**
- **분산락 안전성 함정(가장 미묘):** 단일 Redis 인스턴스 락은 페일오버 시 안전하지 않을 수 있다(획득 직후 마스터 다운→복제 안 된 락 소실→두 클라이언트가 동시에 보유). 완전한 안전엔 *펜싱 토큰(fencing token)*이 필요한데 이 구현엔 없다.
- **TTL 튜닝 딜레마:** TTL이 너무 짧으면 작업 도중 락이 만료돼 **두 명이 동시에 보유 → 오버셀**. 너무 길면 보유자가 죽었을 때 다른 요청이 TTL 내내 대기 → 가용성 저하. GC 일시정지·느린 DB로 보유자가 자기도 모르게 TTL을 넘기면 위험.
- **낮은 처리량:** 모든 예약이 락을 통해 완전히 직렬화된다(벤치 765 rps). 더구나 **Redis 왕복 + DB 작업**을 둘 다 하므로 지연이 더 크다.
- **획득 타임아웃 → 예외:** 고경합에서 `retryCount`를 소진하면 예외를 던진다. 처리 안 하면 서버가 죽는다(그래서 `server.ts`에 try/catch로 500 응답 가드를 두었다).
- **단일 장애점:** Redis 의존.

**완화책:** 작업을 짧게 해 TTL 여유 확보, 강한 보장이 필요하면 펜싱 토큰/합의 기반 락(예: etcd) 고려. 사실 이 도메인에선 redis-decr나 db-atomic이 더 단순·빠른 대안인 경우가 많다.

---

## 정리

- **학습용 반면교사:** naive (오버셀로 문제의식 제공)
- **단순 차감의 실전 기본기:** db-atomic (단, 트랜잭션·유니크 제약 보완 권장)
- **정확성 최우선, 경합 보통:** db-pessimistic (병목·커넥션 점유 주의)
- **경합 낮은 일반 업데이트:** db-optimistic (선착순엔 부적합)
- **최고 성능 + DB 부하 차단:** redis-decr (이중 저장소 정합성 부담 감수)
- **분산 상호배제 학습:** redis-lock (안전성·TTL 함정, 처리량 낮음)

> 어떤 전략도 "공짜 점심"이 아니다. 선착순 단일 품목·고경합이라는 이 도메인 특성에선 **redis-decr(성능) ↔ db-pessimistic/db-atomic+트랜잭션(정합성·단순성)** 사이의 trade-off가 핵심이고, db-optimistic과 redis-lock은 *왜 어떤 도구가 특정 상황에 안 맞는지*를 배우는 데 좋은 비교군이다.
