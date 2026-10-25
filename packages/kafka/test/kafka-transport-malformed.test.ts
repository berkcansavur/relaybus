import { describe, expect, it, vi } from 'vitest';
import { EventValidationError, InvalidEnvelopeError, NonRetryableError } from '@relaybus/core';
import { type InvalidMessagePolicy } from '../src/index.js';
import { asIncoming, createEnvelope } from './fakes.js';
import { setup, subscribed } from './support/transport-setup.js';

describe('malformed messages', () => {
  const malformed = { key: Buffer.from('k'), value: Buffer.from('{nope'), offset: '9' };

  it('dead-letters them to <topic>.dead by default, with the original headers and error headers', async () => {
    const { consumer, onMessage, producer, logger } = await subscribed();
    await consumer.deliver({
      ...malformed,
      headers: { origin: 'svc' },
      key: Buffer.from('k'),
    });
    expect(onMessage).not.toHaveBeenCalled();
    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0]!.topic).toBe('orders.dead');
    expect(producer.sent[0]!.messages[0]!.headers).toMatchObject({
      origin: 'svc',
      'relaybus-source-topic': 'orders',
      'relaybus-source-offset': '9',
    });
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('deadLetterTopic=orders.dead'),
    );
  });

  it('names the dead letter topic with a function of the source topic', async () => {
    const { consumer, producer } = await subscribed({
      onInvalidMessage: { deadLetterTopic: (topic) => `dlq.${topic}` },
    });
    await consumer.deliver(malformed);
    expect(producer.sent[0]!.topic).toBe('dlq.orders');
  });

  it.each([
    ['the default', undefined, ['orders', 'orders.dead']],
    ['a function', { deadLetterTopic: (t: string) => `${t}.x` }, ['orders', 'orders.x']],
  ])(
    'rejects a subscription that consumes its own dead letter topic (%s)',
    async (_l, policy, topics) => {
      const { transport } = setup(
        policy === undefined ? {} : { onInvalidMessage: policy as InvalidMessagePolicy },
      );
      await expect(transport.subscribe(topics, async () => undefined)).rejects.toThrow(/loop/);
    },
  );

  it.each([
    ['skip', 'skip' as const],
    ['a custom handler', async () => undefined],
  ])('does not check dead letter loops for %s', async (_l, policy) => {
    const { transport } = setup({ onInvalidMessage: policy });
    await transport.subscribe(['orders', 'orders.dead'], async () => undefined);
    expect(transport.isConsuming()).toBe(true);
  });

  it('skips them with a logged error only when asked to', async () => {
    const { consumer, onMessage, logger, producer } = await subscribed({
      onInvalidMessage: 'skip',
    });
    await consumer.deliver({ ...asIncoming(createEnvelope()), value: Buffer.from('{nope') });
    expect(onMessage).not.toHaveBeenCalled();
    expect(producer.sent).toHaveLength(0);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining(
        '[KafkaTransport](handleInvalid) Skipping malformed message | topic=orders partition=0 offset=7',
      ),
    );
  });

  it('calls a custom handler with the record and the reason', async () => {
    const handler = vi.fn(async () => undefined);
    const { consumer, onMessage } = await subscribed({ onInvalidMessage: handler });
    await consumer.deliver(malformed);
    expect(onMessage).not.toHaveBeenCalled();
    expect(handler).toHaveBeenCalledWith({
      topic: 'orders',
      partition: 0,
      offset: '9',
      reason: expect.stringContaining('ce_specversion'),
      key: malformed.key,
      value: malformed.value,
    });
  });

  it('retries a failing custom handler instead of dropping the record', async () => {
    const handler = vi
      .fn()
      .mockRejectedValueOnce(new Error('sink down'))
      .mockResolvedValue(undefined);
    const { consumer } = await subscribed({ onInvalidMessage: handler });
    await consumer.deliver(malformed);
    expect(handler).toHaveBeenCalledTimes(2);
  });

  it('rethrows when a custom handler keeps failing', async () => {
    const handler = vi.fn().mockRejectedValue(new Error('sink down'));
    const { consumer } = await subscribed({ onInvalidMessage: handler });
    await expect(consumer.deliver(malformed)).rejects.toThrow('sink down');
    expect(handler).toHaveBeenCalledTimes(3);
  });

  it('dead-letters the untouched record with diagnostic headers', async () => {
    const { consumer, producer, logger } = await subscribed({
      onInvalidMessage: { deadLetterTopic: 'orders.dlq' },
    });
    await consumer.deliver({
      ...malformed,
      headers: { origin: 'svc', multi: ['a', Buffer.from('b')] },
    });
    expect(producer.sent).toHaveLength(1);
    const record = producer.sent[0]!;
    expect(record.topic).toBe('orders.dlq');
    expect(record.acks).toBe(-1);
    expect(record.messages[0]).toMatchObject({
      key: Buffer.from('k'),
      value: Buffer.from('{nope'),
    });
    expect(record.messages[0]!.headers).toMatchObject({
      origin: 'svc',
      multi: ['a', Buffer.from('b')],
      'relaybus-source-topic': 'orders',
      'relaybus-source-partition': '0',
      'relaybus-source-offset': '9',
      'relaybus-invalid-reason': "Header 'multi' is repeated",
    });
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('deadLetterTopic=orders.dlq'),
    );
  });

  it('dead-letters a record without key, value or headers', async () => {
    const { consumer, producer } = await subscribed({
      onInvalidMessage: { deadLetterTopic: 'orders.dlq' },
    });
    await consumer.deliver({ key: null, value: null, offset: '1' });
    expect(producer.sent[0]!.messages[0]).toMatchObject({ key: null, value: null });
  });

  it('does not advance past a record whose dead letter send failed', async () => {
    const { consumer, producer } = await subscribed({
      onInvalidMessage: { deadLetterTopic: 'orders.dlq' },
    });
    producer.sendError = new Error('dlq unavailable');
    await expect(consumer.deliver(malformed)).rejects.toThrow('dlq unavailable');
  });
});

describe('events the bus rejects for good', () => {
  it.each([
    ['an EventValidationError', new EventValidationError('schema mismatch', [])],
    ['a plain NonRetryableError', new NonRetryableError('poison')],
  ])('dead-letters %s instead of retrying', async (_label, error) => {
    const { consumer, onMessage, producer } = await subscribed();
    onMessage.mockRejectedValueOnce(error);
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(onMessage).toHaveBeenCalledTimes(1);
    expect(producer.sent[0]!.topic).toBe('orders.dead');
    expect(producer.sent[0]!.messages[0]!.headers).toMatchObject({
      'relaybus-invalid-reason': error.message,
    });
  });
});

describe('reporting malformed messages to the bus', () => {
  const malformed = { key: Buffer.from('k'), value: Buffer.from('{nope'), offset: '9' };

  it.each([
    ['dead-lettering', undefined],
    ['skipping', 'skip' as const],
    ['a custom policy', () => undefined],
  ])('reports a record it cannot decode once when %s', async (_label, onInvalidMessage) => {
    const onInvalid = vi.fn();
    const { consumer } = await subscribed(
      onInvalidMessage === undefined ? {} : { onInvalidMessage },
      { onInvalidMessage: onInvalid },
    );
    await consumer.deliver(malformed);
    expect(onInvalid).toHaveBeenCalledExactlyOnceWith({
      destination: 'orders',
      error: expect.any(Error),
    });
  });

  it('does not report records that decode, nor events the bus rejects itself', async () => {
    const onInvalid = vi.fn();
    const { consumer, onMessage } = await subscribed({}, { onInvalidMessage: onInvalid });
    onMessage.mockRejectedValueOnce(new NonRetryableError('bus says no'));
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(onMessage).toHaveBeenCalledTimes(1);
    expect(onInvalid).not.toHaveBeenCalled();
  });

  it('reports to nobody when the bus passes no hooks', async () => {
    const { consumer, producer } = await subscribed();
    await consumer.deliver(malformed);
    expect(producer.sent).toHaveLength(1);
  });
});

describe('events the bus rejects as malformed', () => {
  it('routes them to the invalid path instead of retrying, then moves on', async () => {
    const handler = vi.fn(async () => undefined);
    const { consumer, onMessage } = await subscribed({ onInvalidMessage: handler });
    onMessage.mockRejectedValueOnce(new InvalidEnvelopeError('bad envelope'));
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(onMessage).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ reason: 'bad envelope' }));
  });

  it('skips them with an error log when the policy is skip', async () => {
    const { consumer, onMessage, logger } = await subscribed({ onInvalidMessage: 'skip' });
    onMessage.mockRejectedValueOnce(new InvalidEnvelopeError('bad envelope'));
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('reason=bad envelope'));
  });
});
