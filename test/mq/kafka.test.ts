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
