import { randomUUID } from 'node:crypto';
import { Kafka, Partitioners, logLevel, type Consumer } from 'kafkajs';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';

/*
 * Broker and raw kafkajs helpers shared by the integration test files. Vitest
 * isolates test files, so every file that calls `startKafkaBroker` gets its own
 * container and its own copy of this module state.
 */

const KAFKA_IMAGE = 'confluentinc/cp-kafka:7.5.0';
const KAFKA_EXTERNAL_PORT = 9093;

let container: StartedKafkaContainer;
let brokers: string[];

export function createClient(): Kafka {
  return new Kafka({
    clientId: `relaybus-test-${randomUUID()}`,
    brokers,
    logLevel: logLevel.NOTHING,
    retry: { initialRetryTime: 100, retries: 10 },
  });
}

/** Unique topic name; the topic itself is created by `createTopics`. */
export function uniqueName(prefix: string): string {
  return `${prefix}-${randomUUID()}`;
}

/** A consumer cannot subscribe to a topic that does not exist yet, so tests create theirs. */
export async function createTopics(...topics: string[]): Promise<void> {
  const admin = createClient().admin();
  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: topics.map((topic) => ({ topic, numPartitions: 1 })),
  });
  await admin.disconnect();
}

export async function waitFor(condition: () => boolean, timeoutMs = 60_000): Promise<void> {
  const started = Date.now();
  while (!condition()) {
    if (Date.now() - started > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

export interface RawRecord {
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

/** Reads a topic from the beginning with a plain kafkajs consumer. */
export async function readRaw(topic: string, expected: number): Promise<RawRecord[]> {
  const consumer: Consumer = createClient().consumer({ groupId: uniqueName('raw') });
  const records: RawRecord[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      records.push({
        key: message.key?.toString() ?? null,
        value: message.value?.toString() ?? null,
        headers: Object.fromEntries(
          Object.entries(message.headers ?? {}).map(([name, value]) => [name, String(value)]),
        ),
      });
    },
  });
  await waitFor(() => records.length >= expected);
  await consumer.disconnect();
  return records;
}

export async function produceRaw(
  topic: string,
  messages: {
    key?: string | Buffer;
    value: string | Buffer | null;
    headers?: Record<string, string | Buffer>;
  }[],
): Promise<void> {
  const producer = createClient().producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  await producer.send({ topic, messages });
  await producer.disconnect();
}

/**
 * The container reports started before the broker accepts every request, which made
 * the first test flaky in CI (`KafkaJSConnectionClosedError`). Produce and consume one
 * record through a consumer group so the broker and its group coordinator are ready.
 */
async function waitForBroker(): Promise<void> {
  const topic = `readiness-${randomUUID()}`;
  for (let attempt = 1; ; attempt += 1) {
    const kafka = createClient();
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

/** Starts the broker shared by the tests of one file and waits until it serves consumer groups. */
export async function startKafkaBroker(): Promise<void> {
  container = await new KafkaContainer(KAFKA_IMAGE).withKraft().start();
  brokers = [`${container.getHost()}:${container.getMappedPort(KAFKA_EXTERNAL_PORT)}`];
  await waitForBroker();
}

export async function stopKafkaBroker(): Promise<void> {
  await container.stop();
}
