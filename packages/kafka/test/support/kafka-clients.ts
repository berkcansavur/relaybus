import { randomUUID } from 'node:crypto';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import type { KafkaClientLike, KafkaTransportOptions } from '../../src/index.js';

/*
 * The two Kafka clients the transport supports. Integration scenarios run once per client
 * (`describe.each(KAFKA_CLIENTS)`), so both are held to the same behaviour.
 */

export interface KafkaClientUnderTest {
  readonly name: 'kafkajs' | 'confluent';
  /** A client for `brokers`; `connectionTimeout` and no retries when probing an unreachable broker. */
  createClient(brokers: string[], options?: { readonly unreachable?: boolean }): KafkaClientLike;
  /** The transport options a test transport on this client needs besides `kafka`. */
  readonly transportOptions: Partial<KafkaTransportOptions>;
}

export const KAFKAJS: KafkaClientUnderTest = {
  name: 'kafkajs',
  createClient: (brokers, options) =>
    new Kafka({
      clientId: `relaybus-test-${randomUUID()}`,
      brokers,
      logLevel: logLevel.NOTHING,
      ...(options?.unreachable === true
        ? { connectionTimeout: 500, retry: { retries: 0 } }
        : { retry: { initialRetryTime: 100, retries: 10 } }),
    }),
  transportOptions: {
    producerConfig: { createPartitioner: Partitioners.DefaultPartitioner },
    consumerConfig: { retry: { initialRetryTime: 100, retries: 10 } },
  },
};

export const CONFLUENT: KafkaClientUnderTest = {
  name: 'confluent',
  createClient: (brokers, options) =>
    new KafkaJS.Kafka({
      kafkaJS: {
        clientId: `relaybus-test-${randomUUID()}`,
        brokers,
        logLevel: KafkaJS.logLevel.NOTHING,
        ...(options?.unreachable === true ? { connectionTimeout: 500, retry: { retries: 0 } } : {}),
      },
    }),
  // librdkafka's default partitioner for the KafkaJS API is murmur2, like kafkajs' DefaultPartitioner.
  transportOptions: { client: 'confluent' },
};

export const KAFKA_CLIENTS: readonly KafkaClientUnderTest[] = [KAFKAJS, CONFLUENT];
