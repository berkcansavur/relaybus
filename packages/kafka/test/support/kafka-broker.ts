import { randomUUID } from 'node:crypto';
import { Kafka, Partitioners, logLevel, type Consumer } from 'kafkajs';
import { inject } from 'vitest';

/*
 * Raw kafkajs helpers shared by the integration test files. The broker is the one the
 * global setup started for this run; files isolate themselves with unique topic and
 * group names.
 */

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

/** The bootstrap addresses of the shared broker, for clients other than these helpers' kafkajs. */
export function brokerAddresses(): string[] {
  return brokers;
}

/** Points the helpers of the calling test file at the broker the global setup started. */
export function startKafkaBroker(): Promise<void> {
  brokers = inject('kafkaBrokers');
  return Promise.resolve();
}

/** The shared broker outlives the file: the global setup stops it. */
export function stopKafkaBroker(): Promise<void> {
  return Promise.resolve();
}
