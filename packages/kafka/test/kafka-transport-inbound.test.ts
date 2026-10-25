import { describe, expect, it, vi } from 'vitest';
import { HandlerFailedError } from '../src/index.js';
import { EVENT_NAMES, asIncoming, createEnvelope } from './fakes.js';
import { subscribed } from './support/transport-setup.js';

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
