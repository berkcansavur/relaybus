import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import { GroupJoinTimeoutError, type KafkaConsumerConfig } from '../src/index.js';
import { createEnvelope } from './fakes.js';
import { setup } from './support/transport-setup.js';

const ASSIGN_PARTITIONS = { code: -175 };
const REVOKE_PARTITIONS = { code: -174 };

/** A transport on a fake Confluent client: no instrumentation events, membership via rebalance_cb. */
function confluent(options: Parameters<typeof setup>[0] = {}) {
  const context = setup({ client: 'confluent', ...options });
  context.consumer.events = undefined;
  context.consumer.emitOnRun = undefined;
  const rebalance = (error: { code: number }): unknown =>
    (context.kafka.consumerConfig as KafkaConsumerConfig).rebalance_cb!(error, []);
  return { ...context, rebalance };
}

async function subscribedConfluent(options: Parameters<typeof setup>[0] = {}) {
  const context = confluent(options);
  const subscribing = context.transport.subscribe(['orders'], async () => undefined);
  await vi.waitFor(() => expect(context.consumer.runCalls).toBe(1));
  context.rebalance(ASSIGN_PARTITIONS);
  await subscribing;
  return context;
}

describe('Confluent client', () => {
  it('puts the producer settings in the kafkaJS block and the librdkafka properties, where they win, and sends without acks', async () => {
    const { transport, kafka, producer } = confluent({
      maxInFlightRequests: 3,
      producerConfig: {
        'linger.ms': 5,
        'enable.idempotence': false,
        'max.in.flight': 5,
        acks: 0,
        kafkaJS: { idempotent: false, acks: 1, maxInFlightRequests: 5, clientId: 'mine' },
      },
    });
    await transport.publish(createEnvelope(), 'orders');
    expect(kafka.producerConfig).toEqual({
      'linger.ms': 5,
      'enable.idempotence': true,
      'max.in.flight': 3,
      acks: -1,
      kafkaJS: { clientId: 'mine', idempotent: true, maxInFlightRequests: 3, acks: -1 },
    });
    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0]).not.toHaveProperty('acks');
    expect(producer.sent[0]).toMatchObject({ topic: 'orders' });
  });

  it('builds a kafkaJS block when the user gave none', async () => {
    const { transport, kafka } = confluent();
    await transport.connect();
    expect(kafka.producerConfig).toEqual({
      kafkaJS: { idempotent: true, maxInFlightRequests: 1, acks: -1 },
      'enable.idempotence': true,
      'max.in.flight': 1,
      acks: -1,
    });
  });

  it('puts groupId and fromBeginning in the consumer kafkaJS block and subscribes without fromBeginning', async () => {
    const { kafka, consumer } = await subscribedConfluent({
      fromBeginning: false,
      consumerConfig: {
        'session.timeout.ms': 10_000,
        'group.id': 'other',
        'auto.offset.reset': 'earliest',
        kafkaJS: { groupId: 'other', fromBeginning: true, sessionTimeout: 9_000 },
      },
    });
    expect(kafka.consumerConfig).toMatchObject({
      'session.timeout.ms': 10_000,
      'group.id': 'billing',
      'auto.offset.reset': 'latest',
      kafkaJS: { groupId: 'billing', fromBeginning: false, sessionTimeout: 9_000 },
    });
    expect(consumer.subscription).toEqual({ topics: ['orders'] });
    expect(consumer.listeners.size).toBe(0);
  });

  it('starts a group without committed offsets from the earliest offset by default', async () => {
    const { kafka } = await subscribedConfluent();
    expect(kafka.consumerConfig).toMatchObject({
      'auto.offset.reset': 'earliest',
      kafkaJS: { fromBeginning: true },
    });
  });

  it('waits for the partition assignment before start-up resolves', async () => {
    const { transport, rebalance, logger, consumer } = confluent();
    let started = false;
    const subscribing = transport
      .subscribe(['orders'], async () => undefined)
      .then(() => {
        started = true;
      });
    await vi.waitFor(() => expect(consumer.runCalls).toBe(1));
    expect(started).toBe(false);
    expect(transport.isConsuming()).toBe(false);
    rebalance(ASSIGN_PARTITIONS);
    await subscribing;
    expect(transport.isConsuming()).toBe(true);
    expect(logger.log).toHaveBeenCalledWith(
      '[KafkaTransport](observeConsumer) Joined group | group=billing',
    );
  });

  it('fails start-up when no partitions are assigned in time', async () => {
    const { transport } = confluent({ startupTimeout: 5 });
    await expect(transport.subscribe(['orders'], async () => undefined)).rejects.toBeInstanceOf(
      GroupJoinTimeoutError,
    );
    expect(transport.isConsuming()).toBe(false);
  });

  it('follows revocations and reassignments, ignoring other rebalance codes', async () => {
    const { transport, rebalance, logger } = await subscribedConfluent();
    rebalance({ code: -1 });
    expect(transport.isConsuming()).toBe(true);
    rebalance(REVOKE_PARTITIONS);
    expect(transport.isConsuming()).toBe(false);
    expect(logger.warn).toHaveBeenCalledWith(
      '[KafkaTransport](observeConsumer) Rebalancing | group=billing',
    );
    rebalance(ASSIGN_PARTITIONS);
    expect(transport.isConsuming()).toBe(true);
  });

  it("still calls the user's rebalance_cb and returns what it returns", async () => {
    const userCallback = vi.fn((..._args: unknown[]) => 'alternate assignment');
    const { rebalance } = await subscribedConfluent({
      consumerConfig: { rebalance_cb: userCallback },
    });
    expect(rebalance(REVOKE_PARTITIONS)).toBe('alternate assignment');
    expect(userCallback).toHaveBeenLastCalledWith(REVOKE_PARTITIONS, []);
  });

  it('ignores a rebalance_cb that is not a function', async () => {
    const { rebalance } = await subscribedConfluent({ consumerConfig: { rebalance_cb: true } });
    expect(rebalance(REVOKE_PARTITIONS)).toBeUndefined();
  });

  it('rejects an unknown client name', () => {
    expect(() => setup({ client: 'node-rdkafka' as 'confluent' })).toThrow(ConfigurationError);
    expect(() => setup({ client: 'node-rdkafka' as 'confluent' })).toThrow(
      "Unknown Kafka client 'node-rdkafka'; use 'kafkajs' or 'confluent'",
    );
  });
});

describe('kafkajs client', () => {
  it("is the default, and 'kafkajs' selects it explicitly", async () => {
    for (const client of [undefined, 'kafkajs'] as const) {
      const { transport, kafka, producer } = setup(client === undefined ? {} : { client });
      await transport.publish(createEnvelope(), 'orders');
      expect(kafka.producerConfig).toEqual({ idempotent: true, maxInFlightRequests: 1 });
      expect(producer.sent[0]).toMatchObject({ acks: -1 });
    }
  });
});
