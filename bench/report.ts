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
