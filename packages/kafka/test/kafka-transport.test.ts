import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  InvalidEnvelopeError,
  RelayBusError,
  type EventEnvelope,
} from '@relaybus/core';
import { KafkaTransport, type KafkaTransportOptions } from '../src/index.js';
import { EVENT_NAMES, FakeKafka, asIncoming, createEnvelope, createLogger } from './fakes.js';

const FAST = { maxAttempts: 3, initialBackoff: 1, maxBackoff: 2 } as const;

function setup(options: Partial<KafkaTransportOptions> = {}) {
  const kafka = new FakeKafka();
  const logger = createLogger();
  const transport = new KafkaTransport({
    kafka,
    groupId: 'billing',
    logger,
    processing: FAST,
    ...options,
  });
  return { kafka, logger, transport, producer: kafka.producer_, consumer: kafka.consumer_ };
}

async function subscribed(options: Partial<KafkaTransportOptions> = {}) {
  const context = setup(options);
  const received: EventEnvelope[] = [];
  const onMessage = vi.fn(async (envelope: EventEnvelope) => {
    received.push(envelope);
  });
  await context.transport.subscribe(['orders'], onMessage);
  return { ...context, received, onMessage };
}

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
    ).rejects.toThrow(RelayBusError);
    expect(producer.sent).toHaveLength(0);
  });

  it('refuses to publish or connect after close', async () => {
    const { transport } = setup();
    await transport.close();
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(/closed/);
    await expect(transport.connect()).rejects.toThrow(/closed/);
  });
});

describe('subscribe', () => {
  it('subscribes from the beginning by default and waits for the group join', async () => {
    const { consumer, kafka, transport, logger } = setup();
    consumer.emitOnRun = undefined;
    let resolved = false;
    const pending = transport
      .subscribe(['orders', 'billing'], async () => undefined)
      .then(() => {
        resolved = true;
      });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(resolved).toBe(false);
    expect(transport.isConsuming()).toBe(false);
    consumer.emit(EVENT_NAMES.GROUP_JOIN);
    await pending;
    expect(consumer.subscription).toEqual({ topics: ['orders', 'billing'], fromBeginning: true });
    expect(kafka.consumerConfig).toEqual({ groupId: 'billing' });
    expect(transport.isConsuming()).toBe(true);
    expect(logger.log).toHaveBeenCalledWith(expect.stringContaining('[KafkaTransport](subscribe)'));
  });

  it('honours fromBeginning=false and extra consumer config, which never overrides groupId', async () => {
    const { consumer, kafka, transport } = setup({
      fromBeginning: false,
      consumerConfig: { sessionTimeout: 9_000, groupId: 'other' },
    });
    await transport.subscribe(['orders'], async () => undefined);
    expect(consumer.subscription?.fromBeginning).toBe(false);
    expect(kafka.consumerConfig).toEqual({ sessionTimeout: 9_000, groupId: 'billing' });
  });

  it.each([
    ['no group id', { groupId: undefined }, ['orders'], /groupId/],
    ['an empty group id', { groupId: '' }, ['orders'], /groupId/],
    ['no topics', {}, [], /no topics/],
    [
      'a dead letter topic that is consumed',
      { onInvalidMessage: { deadLetterTopic: 'orders' } },
      ['orders'],
      /loop/,
    ],
  ])('rejects %s', async (_label, options, topics, message) => {
    const { transport } = setup(options as Partial<KafkaTransportOptions>);
    await expect(transport.subscribe(topics, async () => undefined)).rejects.toThrow(message);
  });

  it('rejects a second subscribe and a subscribe after close', async () => {
    const { transport } = await subscribed();
    await expect(transport.subscribe(['other'], async () => undefined)).rejects.toThrow(
      /already subscribed/,
    );
    await transport.close();
    await expect(transport.subscribe(['other'], async () => undefined)).rejects.toThrow(/closed/);
  });

  it.each(['connectError', 'subscribeError', 'runError'] as const)(
    'fails start-up when the consumer reports %s',
    async (failure) => {
      const { transport, consumer } = setup();
      consumer[failure] = new Error('broker unreachable');
      await expect(transport.subscribe(['orders'], async () => undefined)).rejects.toThrow(
        'broker unreachable',
      );
      expect(transport.isConsuming()).toBe(false);
    },
  );

  it('fails start-up when the consumer crashes before joining', async () => {
    const { transport, consumer, logger } = setup();
    consumer.emitOnRun = EVENT_NAMES.CRASH;
    consumer.emitPayloadOnRun = { error: new Error('not authorized'), restart: true };
    await expect(transport.subscribe(['orders'], async () => undefined)).rejects.toThrow(
      'not authorized',
    );
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('restart=true'));
    expect(transport.isConsuming()).toBe(false);
  });

  it('fails start-up when the group is not joined in time', async () => {
    const { transport, consumer } = setup({ startupTimeout: 5 });
    consumer.emitOnRun = undefined;
    await expect(transport.subscribe(['orders'], async () => undefined)).rejects.toThrow(
      /did not join group 'billing' within 5ms/,
    );
  });

  it.each([
    ['without instrumentation', { events: undefined }],
    ['without an on() method', { on: undefined }],
    ['without a group join event name', { events: { CRASH: 'crash' } }],
  ])('treats a client %s as consuming once run returned', async (_label, patch) => {
    const { transport, consumer } = setup();
    Object.assign(consumer, patch);
    await transport.subscribe(['orders'], async () => undefined);
    expect(transport.isConsuming()).toBe(true);
    expect(consumer.listeners.size).toBe(0);
  });

  it('skips event names the client does not define', async () => {
    const { transport, consumer } = setup();
    consumer.events = { GROUP_JOIN: 'join' };
    await transport.subscribe(['orders'], async () => undefined);
    expect([...consumer.listeners.keys()]).toEqual(['join']);
  });
});

describe('health', () => {
  it('follows rebalances, crashes, stops and disconnects until the next join', async () => {
    const { transport, consumer, logger } = await subscribed();
    for (const event of [
      EVENT_NAMES.REBALANCING,
      EVENT_NAMES.CRASH,
      EVENT_NAMES.STOP,
      EVENT_NAMES.DISCONNECT,
    ]) {
      expect(transport.isConsuming()).toBe(true);
      consumer.emit(
        event,
        event === EVENT_NAMES.CRASH ? { error: 'boom', restart: false } : undefined,
      );
      expect(transport.isConsuming()).toBe(false);
      consumer.emit(EVENT_NAMES.GROUP_JOIN);
    }
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('Rebalancing'));
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('restart=false'));
  });

  it('reports a crash without a payload', async () => {
    const { transport, consumer, logger } = await subscribed();
    consumer.emit(EVENT_NAMES.CRASH);
    expect(transport.isConsuming()).toBe(false);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('restart=undefined'));
  });

  it('is not consuming before subscribe and after close', async () => {
    const { transport } = setup();
    expect(transport.isConsuming()).toBe(false);
    await transport.subscribe(['orders'], async () => undefined);
    expect(transport.isConsuming()).toBe(true);
    await transport.close();
    expect(transport.isConsuming()).toBe(false);
  });
});

describe('inbound processing', () => {
  it('hands the decoded envelope to the handler and resolves after it', async () => {
    const { consumer, received, onMessage } = await subscribed();
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(received).toEqual([createEnvelope()]);
    expect(onMessage).toHaveBeenCalledTimes(1);
  });

  it('retries a failing handler with backoff and heartbeats, then succeeds', async () => {
    const { consumer, onMessage, logger } = await subscribed();
    onMessage
      .mockRejectedValueOnce(new Error('db down'))
      .mockRejectedValueOnce(new Error('db down'));
    const heartbeat = vi.fn(async () => undefined);
    await consumer.deliver(asIncoming(createEnvelope()), heartbeat);
    expect(onMessage).toHaveBeenCalledTimes(3);
    expect(heartbeat).toHaveBeenCalledTimes(2);
    expect(logger.warn).toHaveBeenCalledTimes(2);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        '[KafkaTransport](deliver) Attempt failed, retrying | topic=orders partition=0 offset=7 attempt=1 error=db down',
      ),
    );
  });

  it('rethrows after the attempts so the offset stays uncommitted, and recovers on the next delivery', async () => {
    const { consumer, onMessage, transport, logger } = await subscribed();
    onMessage.mockRejectedValue(new Error('db down'));
    await expect(consumer.deliver(asIncoming(createEnvelope()))).rejects.toThrow('db down');
    expect(onMessage).toHaveBeenCalledTimes(3);
    expect(transport.isConsuming()).toBe(false);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('attempts=3 error=db down'));
    onMessage.mockResolvedValue(undefined);
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(transport.isConsuming()).toBe(true);
  });

  it('describes non-Error rejections', async () => {
    const { consumer, onMessage, logger } = await subscribed({ processing: { maxAttempts: 1 } });
    onMessage.mockRejectedValue('plain string');
    await expect(consumer.deliver(asIncoming(createEnvelope()))).rejects.toBe('plain string');
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('error='));
  });

  it('wakes the backoff and stops retrying when the transport closes', async () => {
    const { consumer, onMessage, transport, logger } = await subscribed({
      processing: { maxAttempts: 5, initialBackoff: 60_000, maxBackoff: 60_000 },
    });
    onMessage.mockRejectedValue(new Error('db down'));
    const delivery = consumer.deliver(asIncoming(createEnvelope()));
    const assertion = expect(delivery).rejects.toThrow('db down');
    await vi.waitFor(() => expect(logger.warn).toHaveBeenCalledTimes(1));
    await transport.close();
    await assertion;
    expect(onMessage).toHaveBeenCalledTimes(2);
  });

  it('stops retrying at once when the transport closed during the attempt', async () => {
    const { consumer, onMessage, transport } = await subscribed({
      processing: { maxAttempts: 5, initialBackoff: 60_000, maxBackoff: 60_000 },
    });
    onMessage.mockImplementation(async () => {
      await transport.close();
      throw new Error('db down');
    });
    await expect(consumer.deliver(asIncoming(createEnvelope()))).rejects.toThrow('db down');
    expect(onMessage).toHaveBeenCalledTimes(1);
  });

  it('caps the backoff at maxBackoff', async () => {
    const spy = vi.spyOn(globalThis, 'setTimeout');
    const { consumer, onMessage } = await subscribed({
      processing: { maxAttempts: 4, initialBackoff: 1, maxBackoff: 3 },
    });
    onMessage.mockRejectedValue(new Error('x'));
    await expect(consumer.deliver(asIncoming(createEnvelope()))).rejects.toThrow('x');
    const delays = spy.mock.calls
      .map(([, delay]) => delay)
      .filter((d) => d !== undefined && d <= 3);
    expect(delays).toEqual([1, 2, 3]);
    spy.mockRestore();
  });

  it('lets a heartbeat failure (rebalance) propagate', async () => {
    const { consumer, onMessage } = await subscribed();
    onMessage.mockRejectedValueOnce(new Error('db down'));
    await expect(
      consumer.deliver(asIncoming(createEnvelope()), async () => {
        throw new Error('rebalance in progress');
      }),
    ).rejects.toThrow('rebalance in progress');
  });

  it('never treats an unexpected decoding bug as a malformed message', async () => {
    const { consumer, onMessage, logger } = await subscribed();
    const poisoned = {
      key: null,
      value: Buffer.from('1'),
      offset: '7',
      get headers(): never {
        throw new TypeError('decoder bug');
      },
    };
    await expect(
      consumer.eachMessage!({
        topic: 'orders',
        partition: 0,
        message: poisoned,
        heartbeat: async () => undefined,
      }),
    ).rejects.toThrow('decoder bug');
    expect(onMessage).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalledWith(expect.stringContaining('Skipping'));
  });
});

describe('malformed messages', () => {
  const malformed = { key: Buffer.from('k'), value: Buffer.from('{nope'), offset: '9' };

  it('skips them with a logged error by default', async () => {
    const { consumer, onMessage, logger, producer } = await subscribed();
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

describe('events the bus rejects as malformed', () => {
  it('routes them to the invalid path instead of retrying, then moves on', async () => {
    const handler = vi.fn(async () => undefined);
    const { consumer, onMessage } = await subscribed({ onInvalidMessage: handler });
    onMessage.mockRejectedValueOnce(new InvalidEnvelopeError('bad envelope'));
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(onMessage).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ reason: 'bad envelope' }));
  });

  it('skips them with an error log by default', async () => {
    const { consumer, onMessage, logger } = await subscribed();
    onMessage.mockRejectedValueOnce(new InvalidEnvelopeError('bad envelope'));
    await consumer.deliver(asIncoming(createEnvelope()));
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('reason=bad envelope'));
  });
});

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
