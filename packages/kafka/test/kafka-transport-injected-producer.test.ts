import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import { KafkaTransport, type KafkaTransportOptions } from '../src/index.js';
import { FakeKafka, FakeProducer, createEnvelope, createLogger } from './fakes.js';

function injected(options: Partial<KafkaTransportOptions> = {}) {
  const producer = new FakeProducer();
  const transport = new KafkaTransport({
    producer,
    logger: createLogger(),
    ...options,
  });
  return { producer, transport };
}

describe('an injected producer', () => {
  it('publishes through it without connecting it, and leaves it open on close', async () => {
    const { producer, transport } = injected();
    expect(transport.isReady()).toBe(false);
    await transport.connect();
    expect(transport.isReady()).toBe(true);
    await transport.publish(createEnvelope(), 'orders');
    expect(producer.sent).toEqual([
      expect.objectContaining({ topic: 'orders', acks: -1, messages: [expect.anything()] }),
    ]);
    await transport.close();
    expect(producer.connectCalls).toBe(0);
    expect(producer.disconnected).toBe(false);
    expect(transport.isReady()).toBe(false);
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(/closed/);
  });

  it('publishes before connect too, still without connecting it', async () => {
    const { producer, transport } = injected();
    await transport.publish(createEnvelope(), 'orders');
    expect(producer.connectCalls).toBe(0);
    expect(producer.sent).toHaveLength(1);
  });

  it('is used instead of a producer of the client, which still serves the consumer', async () => {
    const kafka = new FakeKafka();
    const { producer, transport } = injected({ kafka, groupId: 'billing' });
    await transport.connect();
    await transport.publish(createEnvelope(), 'orders');
    expect(kafka.producerConfig).toBeUndefined();
    expect(producer.sent).toHaveLength(1);
    await transport.subscribe(['orders'], async () => undefined);
    expect(kafka.consumerConfig).toEqual({ groupId: 'billing' });
    await transport.close();
    expect(kafka.consumer_.disconnected).toBe(true);
    expect(producer.disconnected).toBe(false);
  });

  it('gets Confluent records without acks', async () => {
    const { producer, transport } = injected({ client: 'confluent' });
    await transport.publish(createEnvelope(), 'orders');
    expect(producer.sent[0]).not.toHaveProperty('acks');
  });

  it('cannot subscribe without a client to create the consumer', async () => {
    const { transport } = injected({ groupId: 'billing' });
    await expect(transport.subscribe(['orders'], async () => undefined)).rejects.toThrow(
      "Transport 'kafka' needs 'kafka' to create its consumer",
    );
  });

  it('refuses producer settings it could not apply', () => {
    expect(() => injected({ producerConfig: { idempotent: true } })).toThrow(ConfigurationError);
    expect(() => injected({ maxInFlightRequests: 1 })).toThrow(
      "Give 'producerConfig' and 'maxInFlightRequests' to a producer you inject yourself",
    );
  });

  it('needs a client or a producer', () => {
    expect(() => new KafkaTransport({})).toThrow(
      "Give 'kafka', or an already-connected 'producer' to publish with",
    );
  });
});
