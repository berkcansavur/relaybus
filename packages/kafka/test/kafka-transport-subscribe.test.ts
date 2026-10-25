import { describe, expect, it } from 'vitest';
import { type KafkaTransportOptions } from '../src/index.js';
import { EVENT_NAMES } from './fakes.js';
import { setup, subscribed } from './support/transport-setup.js';

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
