import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import {
  KafkaTransport,
  PublishTimeoutError,
  UnencodableEventError,
  type KafkaTransportOptions,
} from '../src/index.js';
import { FakeKafka, createEnvelope } from './fakes.js';
import { setup } from './support/transport-setup.js';

describe('options', () => {
  it('has a default name and accepts a custom one', () => {
    expect(setup().transport.name).toBe('kafka');
    expect(setup({ name: 'events' }).transport.name).toBe('events');
  });

  it.each([0, 6, 1.5])('rejects maxInFlightRequests %s', (value) => {
    expect(() => setup({ maxInFlightRequests: value })).toThrow(/maxInFlightRequests/);
  });

  it.each([0, 1.5])('rejects processing.maxAttempts %s', (value) => {
    expect(() => setup({ processing: { maxAttempts: value } })).toThrow(/maxAttempts/);
  });

  it('rejects malformed durations', () => {
    expect(() => setup({ startupTimeout: '1 minute' as '1m' })).toThrow(ConfigurationError);
  });

  it.each([
    ['publishTimeout', { publishTimeout: '1 minute' }],
    ['restart.initialBackoff', { restart: { initialBackoff: 'x' } }],
    ['restart.maxBackoff', { restart: { maxBackoff: 'x' } }],
    ['processing.heartbeatInterval', { processing: { heartbeatInterval: 'x' } }],
  ])('rejects a malformed %s', (_label, options) => {
    expect(() => setup(options as Partial<KafkaTransportOptions>)).toThrow(ConfigurationError);
  });

  it('logs to the console unless a logger is given', () => {
    const transport = new KafkaTransport({ kafka: new FakeKafka() });
    expect(transport.isConsuming()).toBe(false);
  });
});

describe('publish', () => {
  it('connects lazily with an idempotent producer and sends one acked CloudEvents record', async () => {
    const { transport, producer, kafka } = setup({
      maxInFlightRequests: 5,
      producerConfig: { allowAutoTopicCreation: false, idempotent: false },
    });
    await transport.publish(createEnvelope(), 'orders');
    expect(producer.connectCalls).toBe(1);
    expect(kafka.producerConfig).toEqual({
      allowAutoTopicCreation: false,
      idempotent: true,
      maxInFlightRequests: 5,
    });
    expect(producer.sent).toHaveLength(1);
    const [record] = producer.sent;
    expect(record).toMatchObject({ topic: 'orders', acks: -1 });
    expect(record!.messages[0]).toMatchObject({ key: 'order-1', value: '{"orderId":"1"}' });
    expect(record!.messages[0]!.headers).toMatchObject({ ce_id: createEnvelope().id });
  });

  it('defaults to one request in flight and shares one producer between calls', async () => {
    const { transport, producer, kafka } = setup();
    await transport.connect();
    await Promise.all([
      transport.publish(createEnvelope(), 'orders'),
      transport.publish(createEnvelope({ key: null }), 'orders'),
    ]);
    expect(producer.connectCalls).toBe(1);
    expect(kafka.producerConfig).toMatchObject({ maxInFlightRequests: 1 });
    expect(producer.sent[1]!.messages[0]!.key).toBeNull();
  });

  it('rejects when the broker does not ack, so the outbox retries', async () => {
    const { transport, producer } = setup();
    producer.sendError = new Error('not enough replicas');
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      'not enough replicas',
    );
  });

  it('can connect again after a failed connect', async () => {
    const { transport, producer } = setup();
    producer.connectError = new Error('broker down');
    await expect(transport.connect()).rejects.toThrow('broker down');
    producer.connectError = undefined;
    await transport.publish(createEnvelope(), 'orders');
    expect(producer.connectCalls).toBe(2);
    expect(producer.sent).toHaveLength(1);
  });

  it('refuses a payload JSON cannot represent without touching the broker', async () => {
    const { transport, producer } = setup();
    await expect(
      transport.publish(createEnvelope({ payload: undefined }), 'orders'),
    ).rejects.toThrow(UnencodableEventError);
    expect(producer.sent).toHaveLength(0);
  });

  it('rejects with a timeout error when the broker never acks, and ignores the late outcome', async () => {
    const { transport, producer } = setup({ publishTimeout: 10 });
    producer.hang = true;
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      PublishTimeoutError,
    );
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      /no broker ack for topic 'orders' within 10ms/,
    );
  });

  it('does not time out a send that is acked in time', async () => {
    const { transport, producer } = setup({ publishTimeout: '1m' });
    await transport.publish(createEnvelope(), 'orders');
    expect(producer.sent).toHaveLength(1);
  });

  it('refuses to publish or connect after close', async () => {
    const { transport } = setup();
    await transport.close();
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(/closed/);
    await expect(transport.connect()).rejects.toThrow(/closed/);
  });
});
