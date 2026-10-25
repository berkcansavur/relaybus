import { describe, expect, it, vi } from 'vitest';
import { EventValidationError } from '@relaybus/core';
import { fromHeader, plainJsonDecoder, type KafkaInboundRecord } from '../src/index.js';
import { asIncoming, createEnvelope } from './fakes.js';
import { setup, subscribed } from './support/transport-setup.js';

describe('close', () => {
  it('disconnects the consumer and the producer and is idempotent', async () => {
    const { transport, consumer, producer } = await subscribed();
    await transport.connect();
    await transport.close();
    await transport.close();
    expect(consumer.disconnected).toBe(true);
    expect(producer.disconnected).toBe(true);
  });

  it('does nothing when nothing was opened', async () => {
    const { transport, kafka } = setup();
    await transport.close();
    expect(kafka.consumer_.disconnected).toBe(false);
    expect(kafka.producer_.disconnected).toBe(false);
  });

  it('rejects a start-up that is still waiting for the join', async () => {
    const { transport, consumer } = setup();
    consumer.emitOnRun = undefined;
    const pending = transport.subscribe(['orders'], async () => undefined);
    const assertion = expect(pending).rejects.toThrow(/is closed/);
    const closed = expect(pending).rejects.toMatchObject({
      name: 'TransportClosedError',
      code: 'RELAYBUS_KAFKA_TRANSPORT_CLOSED',
    });
    await new Promise((resolve) => setTimeout(resolve, 5));
    await transport.close();
    await assertion;
    await closed;
  });

  it('closes while a producer connect is failing, and still closes the consumer', async () => {
    const { transport, producer, consumer } = await subscribed();
    producer.connectError = new Error('broker down');
    const connecting = transport.connect();
    const connectFailure = expect(connecting).rejects.toThrow('broker down');
    await transport.close();
    await connectFailure;
    expect(consumer.disconnected).toBe(true);
    expect(producer.disconnected).toBe(false);
  });

  it('disconnects both sides before reporting a failure', async () => {
    const { transport, consumer, producer } = await subscribed();
    await transport.connect();
    consumer.disconnectError = new Error('consumer stuck');
    await expect(transport.close()).rejects.toThrow('consumer stuck');
    expect(producer.disconnected).toBe(true);
  });
});

describe('inbound decoder', () => {
  it('hands the decoder the record with its location and completes the decoded event', async () => {
    const decode = vi.fn((record: KafkaInboundRecord) => ({
      id: `custom-${record.offset}`,
      type: 'order.placed.v1',
      payload: JSON.parse(String(record.value)) as unknown,
    }));
    const { consumer, received } = await subscribed({ decode });
    await consumer.deliver({
      key: Buffer.from('k-1'),
      value: Buffer.from('{"orderId":"5"}'),
      headers: { origin: 'legacy' },
      timestamp: '1767225600000',
    });
    expect(decode).toHaveBeenCalledWith({
      topic: 'orders',
      partition: 0,
      offset: '7',
      key: Buffer.from('k-1'),
      value: Buffer.from('{"orderId":"5"}'),
      headers: { origin: 'legacy' },
      timestamp: '1767225600000',
    });
    expect(received).toEqual([
      {
        id: 'custom-7',
        type: 'order.placed.v1',
        source: 'orders',
        key: 'k-1',
        payload: { orderId: '5' },
        headers: {},
        correlationId: null,
        causationId: null,
        occurredAt: new Date(1767225600000),
      },
    ]);
  });

  it('decodes plain JSON records with plainJsonDecoder', async () => {
    const { consumer, received } = await subscribed({
      decode: plainJsonDecoder({ type: fromHeader('event-type') }),
    });
    await consumer.deliver({
      value: Buffer.from('{"orderId":"5"}'),
      headers: { 'event-type': Buffer.from('order.placed.v1') },
      timestamp: '1767225600000',
    });
    expect(received[0]).toMatchObject({ id: 'kafka:orders:0:7', type: 'order.placed.v1' });
  });

  it('dead-letters a record whose custom decoder throws any error, and keeps consuming', async () => {
    const { consumer, onMessage, producer, logger } = await subscribed({
      decode: () => {
        throw new TypeError('cannot read properties of undefined');
      },
    });
    await consumer.deliver({ value: Buffer.from('garbage'), headers: { origin: 'legacy' } });
    expect(onMessage).not.toHaveBeenCalled();
    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0]!.topic).toBe('orders.dead');
    expect(producer.sent[0]!.messages[0]!.headers).toMatchObject({
      origin: 'legacy',
      'relaybus-invalid-reason': 'cannot read properties of undefined',
    });
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Dead-lettered malformed message'),
    );
  });

  it('applies the invalid-message policy to records plainJsonDecoder rejects', async () => {
    const policy = vi.fn(async () => undefined);
    const { consumer, onMessage } = await subscribed({
      decode: plainJsonDecoder({ type: 'order.placed.v1' }),
      onInvalidMessage: policy,
    });
    await consumer.deliver({ value: Buffer.from('not json') });
    expect(onMessage).not.toHaveBeenCalled();
    expect(policy).toHaveBeenCalledWith(
      expect.objectContaining({ offset: '7', reason: 'Message value is not valid JSON' }),
    );
  });

  it('still rejects plain JSON by default, so the default stays strict binary mode', async () => {
    const { consumer, onMessage, producer } = await subscribed();
    await consumer.deliver({
      value: Buffer.from('{"orderId":"5"}'),
      headers: { 'event-type': Buffer.from('order.placed.v1') },
    });
    expect(onMessage).not.toHaveBeenCalled();
    expect(producer.sent[0]!.messages[0]!.headers['relaybus-invalid-reason']).toBe(
      "ce_specversion must be '1.0'",
    );
  });
});

describe('error name and code of invalid records', () => {
  const invalid = { key: Buffer.from('k'), value: Buffer.from('{nope'), offset: '9' };

  it('adds the error name and code to the dead letter and its log, keeping the reason', async () => {
    const { consumer, producer, logger } = await subscribed();
    await consumer.deliver(invalid);
    expect(producer.sent[0]!.messages[0]!.headers).toMatchObject({
      'relaybus-invalid-reason': expect.stringContaining('ce_specversion'),
      'relaybus-error-name': 'InvalidKafkaMessageError',
      'relaybus-error-code': 'RELAYBUS_KAFKA_INVALID_MESSAGE',
    });
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringMatching(
        /Dead-lettered malformed message \| .* errorName=InvalidKafkaMessageError code=RELAYBUS_KAFKA_INVALID_MESSAGE$/,
      ),
    );
  });

  it('names an event the bus rejected for good', async () => {
    const { consumer, onMessage, producer } = await subscribed();
    onMessage.mockRejectedValueOnce(new EventValidationError('schema mismatch', []));
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(producer.sent[0]!.messages[0]!.headers).toMatchObject({
      'relaybus-invalid-reason': 'schema mismatch',
      'relaybus-error-name': 'EventValidationError',
      'relaybus-error-code': 'RELAYBUS_EVENT_VALIDATION',
    });
  });

  it('gives an error without a code only its name and drops a stale code header', async () => {
    const { consumer, producer, logger } = await subscribed({
      decode: () => {
        throw new TypeError('cannot read properties of undefined');
      },
    });
    await consumer.deliver({
      value: Buffer.from('garbage'),
      headers: { 'relaybus-error-code': 'STALE', 'relaybus-error-name': 'Stale' },
    });
    const { headers } = producer.sent[0]!.messages[0]!;
    expect(headers['relaybus-error-name']).toBe('TypeError');
    expect(headers).not.toHaveProperty('relaybus-error-code');
    expect(logger.error).toHaveBeenCalledWith(expect.stringMatching(/ errorName=TypeError$/));
  });

  it('logs the error name and code of a skipped record', async () => {
    const { consumer, logger } = await subscribed({ onInvalidMessage: 'skip' });
    await consumer.deliver(invalid);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringMatching(
        /Skipping malformed message \| .* errorName=InvalidKafkaMessageError code=RELAYBUS_KAFKA_INVALID_MESSAGE$/,
      ),
    );
  });
});
