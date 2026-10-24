import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { KafkaContainer } from '@testcontainers/kafka';

export const KAFKA_IMAGE = 'confluentinc/cp-kafka:7.5.0';
const KAFKA_EXTERNAL_PORT = 9093;

export interface KafkaServer {
  /** `host:port` bootstrap addresses. */
  readonly brokers: string[];
  stop(): Promise<void>;
}

function createClient(brokers: string[]): Kafka {
  return new Kafka({
    clientId: `relaybus-test-${randomUUID()}`,
    brokers,
    logLevel: logLevel.NOTHING,
    retry: { initialRetryTime: 100, retries: 10 },
  });
}

/**
 * The container reports started before the broker accepts every request, which made
 * the first test flaky in CI (`KafkaJSConnectionClosedError`). Produce and consume one
 * record through a consumer group so the broker and its group coordinator are ready.
 */
async function waitForBroker(brokers: string[]): Promise<void> {
  const topic = `readiness-${randomUUID()}`;
  for (let attempt = 1; ; attempt += 1) {
    const kafka = createClient(brokers);
    const admin = kafka.admin();
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: `readiness-${randomUUID()}` });
    try {
      await admin.connect();
      await admin.createTopics({ topics: [{ topic }], waitForLeaders: true });
      await producer.connect();
      await producer.send({ topic, messages: [{ value: 'ready' }] });
      await consumer.connect();
      await consumer.subscribe({ topics: [topic], fromBeginning: true });
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('broker warm-up timed out')), 30_000);
        void consumer.run({
          eachMessage: async () => {
            clearTimeout(timer);
            resolve();
          },
        });
      });
      return;
    } catch (error) {
      if (attempt >= 10) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    } finally {
      await Promise.allSettled([admin.disconnect(), producer.disconnect(), consumer.disconnect()]);
    }
  }
}

/** Starts the Kafka broker (KRaft, data on tmpfs) that the Kafka tests share for one run. */
export async function startKafkaServer(): Promise<KafkaServer> {
  const container = await new KafkaContainer(KAFKA_IMAGE)
    .withKraft()
    .withTmpFs({ '/var/lib/kafka/data': 'rw,mode=1777' })
    .start();
  const brokers = [`${container.getHost()}:${container.getMappedPort(KAFKA_EXTERNAL_PORT)}`];
  try {
    await waitForBroker(brokers);
  } catch (error) {
    await container.stop();
    throw error;
  }
  return {
    brokers,
    stop: async () => {
      await container.stop();
    },
  };
}
