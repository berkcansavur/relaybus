import { describe, expect, it, vi } from 'vitest';
import {
  InMemoryEventStore,
  InvalidEnvelopeError,
  RelayBusError,
  type EventEnvelope,
  type Transport,
} from '@relaybus/core';
import { InMemoryTransport } from '@relaybus/testing';
import { OrderPlaced, createTestBus, deferred, waitFor } from './helpers.js';

function remoteEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: '0190a000-0000-7000-8000-0000000000aa',
    type: 'order.placed.v1',
    source: 'remote-service',
    key: 'order-1',
    payload: { orderId: '1' },
    headers: {},
    correlationId: 'flow-1',
    causationId: null,
    occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    ...overrides,
  };
}

describe('lifecycle', () => {
  it('sets up the store, connects transports, subscribes and dispatches until stopped', async () => {
    const store = new InMemoryEventStore();
    const setup = vi.fn(async () => undefined);
    const close = vi.fn(async () => undefined);
    Object.assign(store, { setup, close });
    const transport = new InMemoryTransport('broker');
    const { bus, logger } = createTestBus({
      store,
      transactionMode: 'optional',
      transports: [transport],
      subscriptions: [{ transport: 'broker', destinations: ['orders'] }],
      dispatcher: { pollInterval: 5 },
    });
    const handled = vi.fn();
    bus.subscribe(OrderPlaced, handled, { id: 'h' });
    await bus.start();
    expect(bus.isRunning).toBe(true);
    expect(setup).toHaveBeenCalledTimes(1);
    expect(transport.connected).toBe(true);
    expect(logger.log).toHaveBeenCalledWith(
      '[RelayBus](start) relaybus started | handlers=1 dispatcher=true',
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await waitFor(() => handled.mock.calls.length === 1);
    await transport.deliver('orders', remoteEnvelope());
    await waitFor(() => handled.mock.calls.length === 2);
    await expect(bus.start()).rejects.toThrow('Cannot start a relaybus that is running');
    await bus.stop();
    await bus.stop();
    expect(bus.isRunning).toBe(false);
    expect(transport.closed).toBe(true);
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('skips store setup and dispatching when disabled', async () => {
    const store = new InMemoryEventStore();
    const setup = vi.fn(async () => undefined);
    Object.assign(store, { setup });
    const { bus } = createTestBus({
      store,
      transactionMode: 'optional',
      setupStore: false,
      dispatcher: { enabled: false, pollInterval: 1 },
      retention: { completed: false },
    });
    const handled = vi.fn();
    bus.subscribe(OrderPlaced, handled, { id: 'h' });
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(handled).not.toHaveBeenCalled();
    expect(setup).not.toHaveBeenCalled();
    await bus.stop();
  });

  it('closes transports after a failed start and refuses a second start', async () => {
    const store = new InMemoryEventStore();
    Object.assign(store, { setup: vi.fn().mockRejectedValueOnce(new Error('migrations failed')) });
    const transport = new InMemoryTransport('broker');
    const { bus } = createTestBus({ store, transports: [transport] });
    await expect(bus.start()).rejects.toThrow('migrations failed');
    expect(bus.isRunning).toBe(false);
    expect(transport.closed).toBe(true);
    await expect(bus.start()).rejects.toThrow(
      'A stopped relaybus cannot be started again; create a new instance',
    );
  });

  it('is ready only while running with every transport ready', async () => {
    const ready = new InMemoryTransport('ready');
    let brokerUp = true;
    const flaky: Transport = {
      name: 'flaky',
      publish: async () => undefined,
      isReady: () => brokerUp,
      close: async () => undefined,
    };
    const { bus } = createTestBus({ transports: [ready, flaky] });
    expect(bus.isReady()).toBe(false);
    await bus.start();
    expect(bus.isReady()).toBe(true);
    brokerUp = false;
    expect(bus.isReady()).toBe(false);
    await bus.stop();
  });

  it('logs transports that fail to close', async () => {
    const failing: Transport = {
      name: 'failing',
      publish: async () => undefined,
      close: async () => {
        throw new Error('socket hang up');
      },
    };
    const { bus, logger } = createTestBus({ transports: [failing] });
    await bus.start();
    await bus.stop();
    expect(logger.error).toHaveBeenCalledWith(
      '[RelayBus](closeTransports) Failed to close transport | transport=failing error=socket hang up',
    );
  });

  it('waits for running handlers on stop', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional', dispatcher: { pollInterval: 5 } });
    const gate = deferred();
    let finished = false;
    bus.subscribe(
      OrderPlaced,
      async () => {
        await gate.promise;
        finished = true;
      },
      { id: 'h' },
    );
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    const stopping = bus.stop();
    setTimeout(() => gate.resolve(), 20);
    await stopping;
    expect(finished).toBe(true);
    expect((await bus.listDeliveries())[0]!.status).toBe('completed');
  });

  it('aborts handlers still running after the shutdown timeout', async () => {
    const { bus } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, shutdownTimeout: 20 },
    });
    let signal: AbortSignal | undefined;
    bus.subscribe(
      OrderPlaced,
      (_event, context) => {
        signal = context.signal;
        return new Promise(() => undefined);
      },
      { id: 'h' },
    );
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await waitFor(() => signal !== undefined);
    await bus.stop();
    expect(signal!.aborted).toBe(true);
    expect((signal!.reason as Error).name).toBe('DispatcherStoppedError');
    expect((await bus.listDeliveries())[0]).toMatchObject({
      status: 'pending',
      attempts: 0,
      lastError: null,
    });
  });

  it('keeps polling after a failed claim', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5 },
    });
    vi.spyOn(store, 'claim').mockRejectedValueOnce(new Error('db unavailable'));
    const handled = vi.fn();
    bus.subscribe(OrderPlaced, handled, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.start();
    await waitFor(() => handled.mock.calls.length === 1);
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'claim',
      error: expect.objectContaining({ message: 'db unavailable' }),
    });
    await bus.stop();
  });

  it('keeps claiming while batches come back full', async () => {
    const { bus } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 10_000, concurrency: 4, batchSize: 1 },
    });
    const handled = vi.fn();
    bus.subscribe(OrderPlaced, handled, { id: 'h' });
    for (let index = 0; index < 3; index += 1) {
      await bus.publish(new OrderPlaced({ orderId: String(index) }));
    }
    await bus.start();
    await waitFor(() => handled.mock.calls.length === 3);
    await bus.stop();
  });
});

describe('drain', () => {
  it('stops when nothing is due and refuses to loop forever', async () => {
    const { bus } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 100, initialDelay: 0, maxDelay: 0, jitter: 'none' },
    });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new Error('always');
      },
      { id: 'h' },
    );
    expect(await bus.drain()).toBe(0);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await expect(bus.drain(3)).rejects.toThrow(RelayBusError);
  });
});

describe('transports', () => {
  it('forwards routed events to the broker after commit, retrying outages', async () => {
    const transport = new InMemoryTransport('broker');
    const { bus, clock } = createTestBus({
      transactionMode: 'optional',
      transports: [transport],
      routes: [
        { event: OrderPlaced, transport: 'broker', retry: { initialDelay: 100, jitter: 'none' } },
        {
          event: 'order.placed.v1',
          transport: 'broker',
          destination: 'audit',
          retry: { attempts: 2, initialDelay: 100, jitter: 'none' },
        },
      ],
    });
    transport.failNextPublishes(1);
    const envelope = await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    clock.advance(100);
    await bus.runOnce();
    expect(transport.published).toEqual(
      expect.arrayContaining([
        { destination: 'order.placed.v1', envelope },
        { destination: 'audit', envelope },
      ]),
    );
    expect(bus.handlers().map((registration) => registration.id)).toEqual([
      'relaybus.transport:broker:order.placed.v1:order.placed.v1',
      'relaybus.transport:broker:audit:order.placed.v1',
    ]);
  });

  it('stores received events for local handlers only, once', async () => {
    const transport = new InMemoryTransport('broker');
    const { bus, observer } = createTestBus({
      transports: [transport],
      routes: [{ event: OrderPlaced, transport: 'broker' }],
    });
    const handled = vi.fn();
    bus.subscribe(OrderPlaced, handled, { id: 'local' });
    const envelope = remoteEnvelope();
    expect(await bus.receive(envelope)).toBe('accepted');
    expect(await bus.receive(envelope)).toBe('duplicate');
    expect(await bus.receive(remoteEnvelope({ id: 'other', type: 'unknown.v1' }))).toBe('ignored');
    await bus.drain();
    expect(handled).toHaveBeenCalledTimes(1);
    expect(handled.mock.calls[0]![0]).toEqual(envelope);
    expect(transport.published).toEqual([]);
    expect(observer.onReceived).toHaveBeenCalledWith({ envelope, status: 'duplicate' });
    expect(observer.onReceived).toHaveBeenCalledWith({
      envelope: expect.objectContaining({ type: 'unknown.v1' }),
      status: 'ignored',
    });
  });

  it('redelivers to the broker when storing fails', async () => {
    const transport = new InMemoryTransport('broker');
    const store = new InMemoryEventStore();
    const { bus } = createTestBus({
      store,
      transports: [transport],
      subscriptions: [{ transport: 'broker', destinations: ['orders'] }],
      dispatcher: { enabled: false },
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'local' });
    await bus.start();
    vi.spyOn(store, 'append').mockRejectedValueOnce(new Error('db down'));
    expect(await transport.deliver('orders', remoteEnvelope(), 1)).toBe(2);
    await expect(transport.deliver('orders', remoteEnvelope(), 0)).resolves.toBe(1);
    expect(await bus.listDeliveries()).toHaveLength(1);
    vi.spyOn(store, 'append').mockRejectedValue(new Error('still down'));
    await expect(transport.deliver('orders', remoteEnvelope({ id: 'x' }), 0)).rejects.toThrow(
      'still down',
    );
    expect(await transport.deliver('nobody-listens', remoteEnvelope())).toBe(0);
    await bus.stop();
  });

  it.each([
    [{ id: '' }, /Received event has no id/],
    [{ type: 'bad type' }, InvalidEnvelopeError],
    [{ occurredAt: new Date('invalid') }, /invalid occurredAt/],
    [{ occurredAt: 'not a date' as never }, /invalid occurredAt/],
    [{ type: 42 as never }, /has no type/],
  ])('rejects malformed received events %#', async (overrides, error) => {
    const { bus } = createTestBus();
    await expect(bus.receive(remoteEnvelope(overrides))).rejects.toThrow(error);
  });
});

describe('administration', () => {
  it('reports statistics', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    expect(await bus.statistics()).toMatchObject({ pending: 1, completed: 0 });
  });

  it('purges by the retention policy', async () => {
    const { bus, clock, observer } = createTestBus({
      transactionMode: 'optional',
      retention: { completed: '1h', dead: '30d' },
      retry: { attempts: 1 },
    });
    bus.subscribe(
      OrderPlaced,
      (event) => {
        if (event.payload.orderId === 'bad') {
          throw new Error('bad');
        }
      },
      { id: 'h' },
    );
    await bus.publish(new OrderPlaced({ orderId: 'good' }));
    await bus.publish(new OrderPlaced({ orderId: 'bad' }));
    await bus.runOnce();
    clock.advance(3_600_001);
    expect(await bus.purgeExpired()).toEqual({ deletedDeliveries: 1, deletedEvents: 1 });
    clock.advance(30 * 86_400_000);
    expect(await bus.purgeExpired()).toEqual({ deletedDeliveries: 1, deletedEvents: 1 });
    expect(observer.onPurged).toHaveBeenCalledTimes(2);
  });

  it('keeps everything when retention is disabled', async () => {
    const { bus, store } = createTestBus({ retention: { completed: false } });
    const purge = vi.spyOn(store, 'purge');
    await bus.purgeExpired();
    expect(purge).toHaveBeenCalledWith({});
  });

  it('reports purge failures', async () => {
    const { bus, store, observer } = createTestBus();
    vi.spyOn(store, 'purge').mockRejectedValue(new Error('locked'));
    await expect(bus.purgeExpired()).rejects.toThrow('locked');
    expect(observer.onFailure).toHaveBeenCalledWith(expect.objectContaining({ stage: 'purge' }));
  });

  it('runs the retention job on its interval while running', async () => {
    const { bus, store } = createTestBus({
      retention: { interval: 10 },
      dispatcher: { enabled: false },
    });
    const purge = vi.spyOn(store, 'purge').mockRejectedValueOnce(new Error('transient'));
    await bus.start();
    await waitFor(() => purge.mock.calls.length >= 2);
    await bus.stop();
  });
});

describe('observers', () => {
  it('isolates the bus from observers that throw', async () => {
    const failing = {
      onPublished: () => {
        throw new Error('metrics backend down');
      },
      onFailure: () => {
        throw new Error('failure hook broken');
      },
    };
    const { bus, observer, logger, store } = createTestBus({ transactionMode: 'optional' });
    const busWithFailing = createTestBus({
      store,
      transactionMode: 'optional',
      observers: [failing, observer],
      logger,
    }).bus;
    busWithFailing.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await busWithFailing.publish(new OrderPlaced({ orderId: '1' }));
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'observer',
      error: expect.objectContaining({ message: 'metrics backend down' }),
    });
    expect(logger.warn).toHaveBeenCalledWith(
      '[Notifier](notify) relaybus observer threw | Error: metrics backend down',
    );
    vi.spyOn(store, 'purge').mockRejectedValue(new Error('locked'));
    await expect(busWithFailing.purgeExpired()).rejects.toThrow('locked');
    expect(logger.warn).toHaveBeenCalledWith(
      '[Notifier](notify) relaybus observer threw | Error: failure hook broken',
    );
    expect(logger.error).toHaveBeenCalledWith(
      '[Notifier](failure) relaybus purge failed | Error: locked',
    );
    expect(bus).toBeDefined();
  });

  it('logs an observer whose failure hook throws while reporting another observer', async () => {
    const first = {
      onPublished: () => {
        throw new Error('first');
      },
    };
    const second = {
      onFailure: () => {
        throw new Error('second');
      },
    };
    const { bus, logger } = createTestBus({
      transactionMode: 'optional',
      observers: [first, second],
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    expect(logger.warn).toHaveBeenCalledWith(
      '[Notifier](notify) relaybus observer threw | Error: second',
    );
  });

  it('defaults to console logging', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const store = new InMemoryEventStore();
    const { RelayBus } = await import('@relaybus/core');
    const bus = new RelayBus({ store });
    vi.spyOn(store, 'purge').mockRejectedValue(new Error('locked'));
    await expect(bus.purgeExpired()).rejects.toThrow('locked');
    expect(error).toHaveBeenCalled();
    error.mockRestore();
  });
});
