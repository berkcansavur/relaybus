import { randomUUID } from 'node:crypto';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { describeSharedCatalogServices } from '../../testing/test/support/catalog-services.suite.js';
import { silentLogger } from '../../testing/test/support/catalog-services.js';
import { KafkaTransport } from '../src/index.js';

const KAFKA_IMAGE = 'confluentinc/cp-kafka:7.5.0';
const KAFKA_EXTERNAL_PORT = 9093;

let container: StartedKafkaContainer;
let brokers: string[];

function createClient(): Kafka {
  return new Kafka({
    clientId: `relaybus-catalog-${randomUUID()}`,
    brokers,
    logLevel: logLevel.NOTHING,
    retry: { initialRetryTime: 100, retries: 10 },
  });
}

/**
 * The container reports started before the broker accepts every request. Round-trip
 * one record through a consumer group so the broker and its group coordinator are ready.
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

function createTransport(groupId?: string): KafkaTransport {
  return new KafkaTransport({
    kafka: createClient(),
    ...(groupId === undefined ? {} : { groupId }),
    logger: silentLogger,
    producerConfig: { createPartitioner: Partitioners.DefaultPartitioner },
    consumerConfig: { retry: { initialRetryTime: 100, retries: 10 } },
  });
}

describeSharedCatalogServices({
  name: 'Kafka',
  async start() {
    container = await new KafkaContainer(KAFKA_IMAGE).withKraft().start();
    brokers = [`${container.getHost()}:${container.getMappedPort(KAFKA_EXTERNAL_PORT)}`];
    await waitForBroker();
  },
  async stop() {
    await container.stop();
  },
  async createDestinations() {
    const suffix = randomUUID().slice(0, 8);
    const destinations = { orders: `orders-${suffix}`, shipments: `shipments-${suffix}` };
    const admin = createClient().admin();
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        // Several partitions, so per-key order relies on the key, not on a single partition.
        { topic: destinations.orders, numPartitions: 3 },
        { topic: destinations.shipments, numPartitions: 3 },
      ],
    });
    await admin.disconnect();
    return destinations;
  },
  createTransport: (consumerGroup) => createTransport(consumerGroup),
  async replay(envelope, destination) {
    const transport = createTransport();
    try {
      await transport.publish(envelope, destination);
    } finally {
      await transport.close();
    }
  },
});
