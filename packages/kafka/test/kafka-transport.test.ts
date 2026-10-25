import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  EventValidationError,
  InvalidEnvelopeError,
  NonRetryableError,
  type EventEnvelope,
  type TransportMessage,
  type TransportSubscriptionHooks,
} from '@relaybus/core';
import {
  HandlerFailedError,
  KafkaTransport,
  PublishTimeoutError,
  type InvalidMessagePolicy,
  UnencodableEventError,
  fromHeader,
  plainJsonDecoder,
  type KafkaInboundRecord,
  type KafkaTransportOptions,
} from '../src/index.js';
import { EVENT_NAMES, FakeKafka, asIncoming, createEnvelope, createLogger } from './fakes.js';

const FAST = { maxAttempts: 3, initialBackoff: 1, maxBackoff: 2 } as const;
const FAST_RESTART = { initialBackoff: 1, maxBackoff: 2 } as const;

function setup(options: Partial<KafkaTransportOptions> = {}) {
  const kafka = new FakeKafka();
  const logger = createLogger();
  const transport = new KafkaTransport({
    kafka,
    groupId: 'billing',
    logger,
    processing: FAST,
    restart: FAST_RESTART,
    ...options,
  });
  return { kafka, logger, transport, producer: kafka.producer_, consumer: kafka.consumer_ };
}

async function subscribed(
  options: Partial<KafkaTransportOptions> = {},
  hooks?: TransportSubscriptionHooks,
) {
  const context = setup(options);
  const received: EventEnvelope[] = [];
  const onMessage = vi.fn(async (envelope: EventEnvelope, _message?: TransportMessage) => {
    received.push(envelope);
  });
  await context.transport.subscribe(['orders'], onMessage, hooks);
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
      expect(consumer.disconnected).toBe(true);
    },
  );

  it('stops the consumer it started when start-up fails, so a later subscribe can retry', async () => {
    const { transport, consumer } = setup({ startupTimeout: 5 });
    consumer.emitOnRun = undefined;
    consumer.disconnectError = new Error('already gone');
    await expect(transport.subscribe(['orders'], async () => undefined)).rejects.toThrow(
      /did not join/,
    );
    expect(consumer.disconnected).toBe(true);
    consumer.emitOnRun = EVENT_NAMES.GROUP_JOIN;
    consumer.disconnectError = undefined;
    await transport.subscribe(['orders'], async () => undefined);
    expect(transport.isConsuming()).toBe(true);
  });

  it('does not restart a consumer that crashed before the first join; start-up fails instead', async () => {
    const { transport, consumer } = setup();
    consumer.emitOnRun = EVENT_NAMES.CRASH;
    consumer.emitPayloadOnRun = { error: new Error('no auth'), restart: false };
    await expect(transport.subscribe(['orders'], async () => undefined)).rejects.toThrow('no auth');
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(consumer.runCalls).toBe(1);
  });

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
    const subscription = transport.subscribe(['orders'], async () => undefined);
    await expect(subscription).rejects.toThrow(/did not join group 'billing' within 5ms/);
    await expect(subscription).rejects.toMatchObject({
      name: 'GroupJoinTimeoutError',
      code: 'RELAYBUS_KAFKA_GROUP_JOIN_TIMEOUT',
    });
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
        event === EVENT_NAMES.CRASH ? { error: 'boom', restart: true } : undefined,
      );
      expect(transport.isConsuming()).toBe(false);
      consumer.emit(EVENT_NAMES.GROUP_JOIN);
    }
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('Rebalancing'));
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('restart=true'));
  });

  it('reports a crash without a payload', async () => {
    const { transport, consumer, logger } = await subscribed();
    consumer.emit(EVENT_NAMES.CRASH);
    expect(transport.isConsuming()).toBe(false);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('restart=undefined'));
  });

  it('reports readiness only when connected and, once subscribed, consuming', async () => {
    const { transport, consumer } = setup();
    expect(transport.isReady()).toBe(false);
    await transport.connect();
    expect(transport.isReady()).toBe(true);
    await transport.subscribe(['orders'], async () => undefined);
    expect(transport.isReady()).toBe(true);
    consumer.emit(EVENT_NAMES.REBALANCING);
    expect(transport.isReady()).toBe(false);
    consumer.emit(EVENT_NAMES.GROUP_JOIN);
    expect(transport.isReady()).toBe(true);
    await transport.close();
    expect(transport.isReady()).toBe(false);
  });

  it('is not ready when the producer failed to connect', async () => {
    const { transport, producer } = setup();
    producer.connectError = new Error('down');
    await expect(transport.connect()).rejects.toThrow('down');
    expect(transport.isReady()).toBe(false);
  });

  it('does not report ready for a producer that connected after close', async () => {
    const { transport } = setup();
    const connecting = transport.connect();
    await transport.close();
    await connecting.catch(() => undefined);
    expect(transport.isReady()).toBe(false);
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
    // The topic it was read from goes along, for observers and traces.
    expect(onMessage).toHaveBeenCalledWith(createEnvelope(), { destination: 'orders' });
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
        '[KafkaTransport](withRetries) Attempt failed, retrying | operation=deliver topic=orders partition=0 offset=7 attempt=1 error=db down',
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
    await expect(consumer.deliver(asIncoming(createEnvelope()))).rejects.toThrow(
      HandlerFailedError,
    );
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('error='));
  });

  it.each([
    ['TypeError', new TypeError('x is not a function')],
    ['RangeError', new RangeError('out of range')],
    ['ReferenceError', new ReferenceError('y is not defined')],
    ['SyntaxError', new SyntaxError('bad json')],
  ])(
    'hands a %s to the client wrapped, so it can never be treated as fatal',
    async (name, error) => {
      const { consumer, onMessage } = await subscribed({ processing: { maxAttempts: 1 } });
      onMessage.mockRejectedValue(error);
      const failure = await consumer.deliver(asIncoming(createEnvelope())).catch((e: unknown) => e);
      expect(failure).toBeInstanceOf(HandlerFailedError);
      expect(failure).not.toBeInstanceOf(TypeError);
      expect((failure as Error).message).toBe(`${name}: ${error.message}`);
      expect((failure as Error).cause).toBeUndefined();
    },
  );

  it('heartbeats in slices during a long backoff', async () => {
    const { consumer, onMessage } = await subscribed({
      processing: { maxAttempts: 2, initialBackoff: 25, maxBackoff: 25, heartbeatInterval: 10 },
    });
    onMessage.mockRejectedValueOnce(new Error('db down'));
    const heartbeat = vi.fn(async () => undefined);
    await consumer.deliver(asIncoming(createEnvelope()), heartbeat);
    expect(heartbeat).toHaveBeenCalledTimes(3);
  });

  it('stops the sliced backoff when the transport closes', async () => {
    const { consumer, onMessage, transport, logger } = await subscribed({
      processing: {
        maxAttempts: 5,
        initialBackoff: 60_000,
        maxBackoff: 60_000,
        heartbeatInterval: 10,
      },
    });
    onMessage.mockRejectedValue(new Error('db down'));
    const heartbeat = vi.fn(async () => undefined);
    const delivery = consumer.deliver(asIncoming(createEnvelope()), heartbeat);
    const assertion = expect(delivery).rejects.toThrow('db down');
    await vi.waitFor(() => expect(logger.warn).toHaveBeenCalledTimes(1));
    await transport.close();
    await assertion;
    expect(heartbeat.mock.calls.length).toBeLessThan(100);
  });

  it('tracks stalls per partition: one partition recovering does not hide another', async () => {
    const { consumer, onMessage, transport } = await subscribed({ processing: { maxAttempts: 1 } });
    const deliver = (partition: number) =>
      consumer.eachMessage!({
        topic: 'orders',
        partition,
        message: asIncoming(createEnvelope()),
        heartbeat: async () => undefined,
      });
    onMessage.mockRejectedValue(new Error('db down'));
    await expect(deliver(0)).rejects.toThrow();
    await expect(deliver(1)).rejects.toThrow();
    onMessage.mockResolvedValue(undefined);
    await deliver(1);
    expect(transport.isConsuming()).toBe(false);
    await deliver(0);
    expect(transport.isConsuming()).toBe(true);
  });

  it('forgets stalls on a rebalance, because the assignment changes', async () => {
    const { consumer, onMessage, transport } = await subscribed({ processing: { maxAttempts: 1 } });
    onMessage.mockRejectedValue(new Error('db down'));
    await expect(consumer.deliver(asIncoming(createEnvelope()))).rejects.toThrow();
    consumer.emit(EVENT_NAMES.REBALANCING);
    consumer.emit(EVENT_NAMES.GROUP_JOIN);
    expect(transport.isConsuming()).toBe(true);
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

  it('wraps an unexpected decoding bug so the client never sees a raw TypeError', async () => {
    const { consumer } = await subscribed();
    const poisoned = {
      key: null,
      value: Buffer.from('1'),
      offset: '7',
      get headers(): never {
        throw new TypeError('decoder bug');
      },
    };
    const failure = await consumer.eachMessage!({
      topic: 'orders',
      partition: 0,
      message: poisoned,
      heartbeat: async () => undefined,
    }).catch((e: unknown) => e);
    expect(failure).toBeInstanceOf(HandlerFailedError);
  });
});

describe('consumer crash recovery', () => {
  it('restarts the run loop after an unrecoverable crash and recovers', async () => {
    const { transport, consumer, logger } = await subscribed();
    consumer.emit(EVENT_NAMES.CRASH, { error: new TypeError('fatal'), restart: false });
    expect(transport.isConsuming()).toBe(false);
    await vi.waitFor(() => expect(consumer.runCalls).toBe(2));
    await vi.waitFor(() => expect(transport.isConsuming()).toBe(true));
    expect(consumer.disconnected).toBe(true);
    expect(logger.log).toHaveBeenCalledWith(
      expect.stringContaining('[KafkaTransport](restartConsumer) Consumer restarted'),
    );
  });

  it('keeps restarting with backoff while restarts fail', async () => {
    const { transport, consumer, logger } = await subscribed();
    consumer.runError = new Error('broker down');
    consumer.emit(EVENT_NAMES.CRASH, { error: 'boom', restart: false });
    await vi.waitFor(() => expect(consumer.runCalls).toBeGreaterThanOrEqual(4));
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('[KafkaTransport](restartConsumer) Restart failed, will retry'),
    );
    expect(transport.isConsuming()).toBe(false);
    consumer.runError = undefined;
    await vi.waitFor(() => expect(transport.isConsuming()).toBe(true));
  });

  it('ignores a second crash report while a restart is already running', async () => {
    const { consumer } = await subscribed({ restart: { initialBackoff: 20, maxBackoff: 20 } });
    consumer.emit(EVENT_NAMES.CRASH, { error: 'a', restart: false });
    consumer.emit(EVENT_NAMES.CRASH, { error: 'b', restart: false });
    await vi.waitFor(() => expect(consumer.runCalls).toBe(2));
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(consumer.runCalls).toBe(2);
  });

  it('restarts even when disconnecting the dead consumer fails', async () => {
    const { transport, consumer } = await subscribed();
    consumer.disconnectError = new Error('already disconnected');
    consumer.emit(EVENT_NAMES.CRASH, { error: 'a', restart: false });
    await vi.waitFor(() => expect(consumer.runCalls).toBe(2));
    await vi.waitFor(() => expect(transport.isConsuming()).toBe(true));
  });

  it('can restart again after a later crash', async () => {
    const { consumer } = await subscribed();
    consumer.emit(EVENT_NAMES.CRASH, { error: 'a', restart: false });
    await vi.waitFor(() => expect(consumer.runCalls).toBe(2));
    await vi.waitFor(() => expect(consumer.disconnected).toBe(true));
    await new Promise((resolve) => setTimeout(resolve, 5));
    consumer.emit(EVENT_NAMES.CRASH, { error: 'b', restart: false });
    await vi.waitFor(() => expect(consumer.runCalls).toBe(3));
  });

  it('does not restart when the client restarts by itself', async () => {
    const { consumer } = await subscribed();
    consumer.emit(EVENT_NAMES.CRASH, { error: 'a', restart: true });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(consumer.runCalls).toBe(1);
  });

  it('stops restarting when the transport closes during the backoff', async () => {
    const { transport, consumer } = await subscribed({
      restart: { initialBackoff: 60_000, maxBackoff: 60_000 },
    });
    consumer.emit(EVENT_NAMES.CRASH, { error: 'a', restart: false });
    await transport.close();
    expect(consumer.runCalls).toBe(1);
  });

  it('stops restarting when the transport closes while the restart waits for the join', async () => {
    const { transport, consumer } = await subscribed();
    consumer.emitOnRun = undefined;
    consumer.emit(EVENT_NAMES.CRASH, { error: 'a', restart: false });
    await vi.waitFor(() => expect(consumer.runCalls).toBe(2));
    await transport.close();
    expect(transport.isConsuming()).toBe(false);
  });

  it('stops restarting when the transport closes between a failed attempt and the next', async () => {
    const { transport, consumer } = await subscribed({
      restart: { initialBackoff: 5, maxBackoff: 5 },
    });
    consumer.runError = new Error('down');
    consumer.emit(EVENT_NAMES.CRASH, { error: 'a', restart: false });
    await vi.waitFor(() => expect(consumer.runCalls).toBeGreaterThanOrEqual(2));
    await transport.close();
    const calls = consumer.runCalls;
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(consumer.runCalls).toBe(calls);
  });
});

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
