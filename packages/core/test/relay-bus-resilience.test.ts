import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  DEFAULT_ROUTE_RETRY_POLICY,
  DispatcherStoppedError,
  InMemoryEventStore,
  MAX_TIMER_DELAY_MS,
  RelayBus,
  createRetryPolicy,
  defineEvent,
  systemClock,
  type StandardSchemaV1,
  type Transport,
} from '@relaybus/core';
import { InMemoryTransport, createEnvelope } from '@relaybus/testing';
import { Dispatcher } from '../src/dispatcher.js';
import { HandlerRegistry } from '../src/handler-registry.js';
import { Notifier } from '../src/notifier.js';
import {
  OrderPlaced,
  OrderShipped,
  createLogger,
  createTestBus,
  deferred,
  waitFor,
} from './helpers.js';

describe('configuration limits', () => {
  it('rejects timer durations Node.js cannot represent, but allows long publish delays', async () => {
    const store = new InMemoryEventStore();
    expect(() => new RelayBus({ store, handlerTimeout: '30d' })).toThrow(
      /must not exceed 2147483647ms/,
    );
    expect(() => new RelayBus({ store, retention: { interval: MAX_TIMER_DELAY_MS + 1 } })).toThrow(
      /retention.interval must not exceed/,
    );
    expect(() => new RelayBus({ store, dispatcher: { shutdownTimeout: 0 } })).toThrow(
      /dispatcher.shutdownTimeout must be greater than zero/,
    );
    expect(() => new RelayBus({ store, unroutedEvents: 'ignore' as never })).toThrow(
      "unroutedEvents must be one of 'warn', 'throw', received 'ignore'",
    );
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }), { delay: '60d' });
    expect(await bus.runOnce()).toBe(0);
  });

  it('accepts unlimited attempts and raises the cap with a large initial delay', () => {
    expect(createRetryPolicy({ attempts: Number.POSITIVE_INFINITY }).attempts).toBe(
      Number.POSITIVE_INFINITY,
    );
    expect(createRetryPolicy({ initialDelay: '20m' }).maxDelayMs).toBe(1_200_000);
    expect(DEFAULT_ROUTE_RETRY_POLICY).toMatchObject({
      attempts: Number.POSITIVE_INFINITY,
      maxDelayMs: 300_000,
    });
  });
});

describe('unrouted events', () => {
  it('can reject events nobody handles, rolling back the caller', async () => {
    const { bus } = createTestBus({ unroutedEvents: 'throw' });
    bus.subscribe(OrderShipped, () => undefined, { id: 'h' });
    await expect(
      bus.transaction(() =>
        bus.publishAll([new OrderShipped({ orderId: '1' }), new OrderPlaced({ orderId: '1' })]),
      ),
    ).rejects.toThrow(
      /No handler or route for event type, event not stored \| type=order.placed.v1/,
    );
    expect(await bus.listDeliveries()).toEqual([]);
  });
});

describe('correlation', () => {
  it('gives every event of one publishAll the same correlation id', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'placed' });
    bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });
    const [first, second] = await bus.publishAll([
      new OrderPlaced({ orderId: '1' }),
      new OrderShipped({ orderId: '1' }),
    ]);
    expect(first!.correlationId).toBe(first!.id);
    expect(second!.correlationId).toBe(first!.id);
  });
});

describe('received envelopes', () => {
  it('normalizes fields that JSON wire formats drop', async () => {
    const { bus } = createTestBus();
    const received: unknown[] = [];
    bus.subscribe(OrderPlaced, (event) => void received.push(event), { id: 'h' });
    const raw = {
      id: 'remote-1',
      type: 'order.placed.v1',
      source: 7,
      payload: { orderId: '1' },
      occurredAt: '2026-02-03T04:05:06.007Z',
      key: '',
      correlationId: undefined,
    } as never;
    expect(await bus.receive(raw)).toBe('accepted');
    await bus.drain();
    expect(received[0]).toEqual({
      id: 'remote-1',
      type: 'order.placed.v1',
      source: 'unknown',
      key: null,
      payload: { orderId: '1' },
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date('2026-02-03T04:05:06.007Z'),
    });
  });

  it('keeps unkeyed received events in parallel', async () => {
    const { bus } = createTestBus();
    bus.subscribe(OrderPlaced, () => new Promise((resolve) => setTimeout(resolve, 5)), { id: 'h' });
    for (const id of ['a', 'b', 'c']) {
      await bus.receive({
        ...createEnvelope({ id, type: 'order.placed.v1' }),
        key: undefined,
      } as never);
    }
    expect(await bus.runOnce()).toBe(3);
  });
});

describe('routes', () => {
  it('lets many event types share one destination and retries until the broker recovers', async () => {
    const transport = new InMemoryTransport('broker');
    const { bus, clock } = createTestBus({
      transactionMode: 'optional',
      transports: [transport],
      routes: [
        { event: OrderPlaced, transport: 'broker', destination: 'orders' },
        { event: OrderShipped, transport: 'broker', destination: 'orders' },
      ],
    });
    const [placed] = bus.handlers();
    expect(placed).toMatchObject({ timeoutMs: 30_000, forwardsToTransport: true });
    expect(placed!.retry.attempts).toBe(Number.POSITIVE_INFINITY);
    transport.failNextPublishes(25);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    for (let round = 0; round < 25; round += 1) {
      await bus.runOnce();
      clock.advance(300_000);
    }
    await bus.runOnce();
    expect(transport.published).toHaveLength(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'completed', attempts: 26 });
  });

  it('times out a hanging broker publish', async () => {
    const hanging: Transport = {
      name: 'hanging',
      publish: () => new Promise(() => undefined),
      close: async () => undefined,
    };
    const { bus } = createTestBus({
      transactionMode: 'optional',
      transports: [hanging],
      routes: [{ event: OrderPlaced, transport: 'hanging', timeout: 10 }],
      dispatcher: { leaseDuration: 30 },
    });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect((await bus.listDeliveries())[0]).toMatchObject({
      status: 'pending',
      lastError: { name: 'HandlerTimeoutError' },
    });
  });
});

describe('lease self-fencing', () => {
  it('aborts a handler whose lease cannot be renewed once it expires', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      clock: systemClock,
      dispatcher: { leaseDuration: 30 },
    });
    let renewals = 0;
    vi.spyOn(store, 'extendLease').mockImplementation(() => {
      renewals += 1;
      return Promise.reject(new Error('store unreachable'));
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
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(signal!.aborted).toBe(true);
    expect((signal!.reason as Error).name).toBe('LeaseLostError');
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
    expect(renewals).toBeGreaterThanOrEqual(1);
  });

  it('never overlaps lease renewals', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      clock: systemClock,
      dispatcher: { leaseDuration: 30 },
    });
    const pending = deferred<boolean>();
    const extend = vi.spyOn(store, 'extendLease').mockReturnValue(pending.promise);
    bus.subscribe(OrderPlaced, () => new Promise(() => undefined), { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(extend).toHaveBeenCalledTimes(1);
    pending.resolve(true);
  });

  it('notices a lease lost before the handler even started', async () => {
    const slowSchema: StandardSchemaV1<unknown, { orderId: string }> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: async (value) => {
          await new Promise((resolve) => setTimeout(resolve, 40));
          return { value: value as { orderId: string } };
        },
      },
    };
    class SlowEvent extends defineEvent('slow.v1', { schema: slowSchema }) {}
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      clock: systemClock,
      dispatcher: { leaseDuration: 6 },
    });
    vi.spyOn(store, 'extendLease').mockResolvedValue(false);
    const handler = vi.fn();
    bus.subscribe(SlowEvent, handler, { id: 'h' });
    await bus.publish(new SlowEvent({ orderId: '1' }));
    await bus.runOnce();
    expect(handler).not.toHaveBeenCalled();
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
  });
});

describe('abandoned handlers', () => {
  it('waits for a timed-out handler before scheduling its retry', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional', handlerTimeout: 10 });
    let running = 0;
    let peak = 0;
    bus.subscribe(
      OrderPlaced,
      async () => {
        running += 1;
        peak = Math.max(peak, running);
        await new Promise((resolve) => setTimeout(resolve, 40));
        running -= 1;
      },
      { id: 'h', retry: { initialDelay: 0, maxDelay: 0, jitter: 'none', attempts: 3 } },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.drain();
    expect(peak).toBe(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'dead', attempts: 3 });
  });

  it('keeps a transactional handler transaction open until the handler settles', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional', handlerTimeout: 10 });
    let finished = false;
    bus.subscribe(
      OrderPlaced,
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 40));
        finished = true;
      },
      { id: 'h', transactional: true },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(finished).toBe(true);
    expect((await bus.listDeliveries())[0]).toMatchObject({
      status: 'pending',
      lastError: { name: 'HandlerTimeoutError' },
    });
  });
});

describe('completion', () => {
  it('retries a transient completion failure', async () => {
    const { bus, store, observer } = createTestBus({ transactionMode: 'optional' });
    const complete = store.complete.bind(store);
    vi.spyOn(store, 'complete')
      .mockRejectedValueOnce(new Error('connection reset'))
      .mockImplementation(complete);
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(observer.onDeliverySucceeded).toHaveBeenCalledTimes(1);
    expect(observer.onFailure).not.toHaveBeenCalled();
  });
});

describe('shutdown', () => {
  it('releases aborted deliveries without spending an attempt', async () => {
    const { bus, observer } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 1 },
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
    expect(signal!.reason).toBeInstanceOf(DispatcherStoppedError);
    expect(observer.onDeliveryReleased).toHaveBeenCalledTimes(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'pending', attempts: 0 });
  });

  it.each([
    ['throws', () => Promise.reject(new Error('db gone')), 'failure'],
    ['refuses', () => Promise.resolve(false), 'leaseLost'],
  ] as const)('reports a release that %s', async (_label, release, expected) => {
    const store = new InMemoryEventStore();
    vi.spyOn(store, 'release').mockImplementation(release);
    const { bus, observer } = createTestBus({
      store,
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, shutdownTimeout: 10 },
    });
    let started = false;
    bus.subscribe(
      OrderPlaced,
      () => {
        started = true;
        return new Promise(() => undefined);
      },
      { id: 'h' },
    );
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await waitFor(() => started);
    await bus.stop();
    if (expected === 'failure') {
      expect(observer.onFailure).toHaveBeenCalledWith(
        expect.objectContaining({ stage: 'release' }),
      );
    } else {
      expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
    }
  });

  it('bounds the wait for a claim that never returns', async () => {
    const store = new InMemoryEventStore();
    const registry = new HandlerRegistry();
    registry.register(
      {
        eventTypes: ['t'],
        schema: undefined,
        handle: () => undefined,
        options: { id: 'h' },
        forwardsToTransport: false,
      },
      { retry: createRetryPolicy(undefined), timeoutMs: null },
    );
    vi.spyOn(store, 'claim').mockReturnValue(new Promise(() => undefined));
    const dispatcher = new Dispatcher({
      store,
      registry,
      clock: systemClock,
      random: () => 0,
      notifier: new Notifier([], createLogger()),
      settings: {
        owner: 'o',
        concurrency: 1,
        batchSize: 1,
        pollIntervalMs: 5,
        leaseDurationMs: 1_000,
        shutdownTimeoutMs: 20,
      },
    });
    dispatcher.start();
    const started = Date.now();
    await dispatcher.stop();
    expect(Date.now() - started).toBeLessThan(500);
  });

  it('stops a bus that is still starting', async () => {
    const slow = new InMemoryTransport('slow');
    const connect = deferred();
    vi.spyOn(slow, 'connect').mockReturnValue(connect.promise);
    const { bus } = createTestBus({ transports: [slow] });
    const starting = bus.start();
    const stopping = bus.stop();
    connect.resolve();
    await starting;
    await stopping;
    expect(bus.isRunning).toBe(false);
    await expect(bus.start()).rejects.toThrow(
      'A stopped relaybus cannot be started again; create a new instance',
    );
  });

  it('stops after a failed start without throwing', async () => {
    const store = new InMemoryEventStore();
    const setup = deferred();
    Object.assign(store, { setup: () => setup.promise });
    const { bus } = createTestBus({ store });
    const starting = bus.start();
    const stopping = bus.stop();
    setup.reject(new Error('migrations failed'));
    await expect(starting).rejects.toThrow('migrations failed');
    await stopping;
    expect(bus.isRunning).toBe(false);
  });

  it('shares one stop between concurrent callers and ends stopped even when closing fails', async () => {
    const store = new InMemoryEventStore();
    Object.assign(store, { close: () => Promise.reject(new Error('pool already closed')) });
    const { bus } = createTestBus({ store });
    await bus.start();
    const first = bus.stop();
    const second = bus.stop();
    await expect(first).rejects.toThrow('pool already closed');
    await expect(second).rejects.toThrow('pool already closed');
    expect(bus.isRunning).toBe(false);
    await expect(bus.start()).rejects.toThrow(/cannot be started again/);
  });

  it('logs transports that throw non-errors while closing', async () => {
    const odd: Transport = {
      name: 'odd',
      publish: async () => undefined,
      // The point of the test is a transport that rejects with a non-Error.
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
      close: () => Promise.reject('just a string'),
    };
    const { bus, logger } = createTestBus({ transports: [odd] });
    await bus.start();
    await bus.stop();
    expect(logger.error).toHaveBeenCalledWith(
      '[RelayBus](closeTransports) Failed to close transport | transport=odd error=just a string',
    );
  });
});

describe('scope isolation', () => {
  it('does not leak a caller transaction into handlers', async () => {
    const { bus } = createTestBus({ retry: { attempts: 1 } });
    bus.subscribe(OrderPlaced, () => bus.publish(new OrderShipped({ orderId: '1' })), {
      id: 'ship',
    });
    bus.subscribe(OrderShipped, () => undefined, { id: 'notify' });
    await bus.transaction(() => bus.publish(new OrderPlaced({ orderId: '1' })));
    await bus.transaction(() => bus.runOnce());
    expect((await bus.listDeliveries({ handler: 'ship' }))[0]!.lastError).toMatchObject({
      name: 'TransactionRequiredError',
    });
  });
});

describe('errors', () => {
  it('keep their names after minification', () => {
    expect(new ConfigurationError('x').name).toBe('ConfigurationError');
    expect(new DispatcherStoppedError('x').name).toBe('DispatcherStoppedError');
  });
});

describe('transactional leases', () => {
  it('never renews the lease while a transactional handler runs', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      clock: systemClock,
      dispatcher: { leaseDuration: 60 },
    });
    const extend = vi.spyOn(store, 'extendLease');
    bus.subscribe(OrderPlaced, () => new Promise((resolve) => setTimeout(resolve, 45)), {
      id: 'h',
      transactional: true,
    });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(extend).not.toHaveBeenCalled();
    expect((await bus.listDeliveries())[0]!.status).toBe('completed');
  });

  it('extends the lease once to cover a long timeout', async () => {
    const { bus, store, clock } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { leaseDuration: '30s' },
    });
    const extend = vi.spyOn(store, 'extendLease');
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h', transactional: true, timeout: '2m' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(extend).toHaveBeenCalledTimes(1);
    expect(extend.mock.calls[0]![1]).toEqual(new Date(clock.now().getTime() + 150_000));
    expect((await bus.listDeliveries())[0]!.status).toBe('completed');
  });

  it('gives up when the lease cannot be extended for the timeout', async () => {
    const { bus, store, observer } = createTestBus({ transactionMode: 'optional' });
    vi.spyOn(store, 'extendLease').mockResolvedValue(false);
    const handler = vi.fn();
    bus.subscribe(OrderPlaced, handler, { id: 'h', transactional: true, timeout: '5m' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(handler).not.toHaveBeenCalled();
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
  });

  it('aborts a transactional handler that outlives its lease', async () => {
    const { bus, observer } = createTestBus({
      transactionMode: 'optional',
      clock: systemClock,
      dispatcher: { leaseDuration: 20 },
    });
    let signal: AbortSignal | undefined;
    bus.subscribe(
      OrderPlaced,
      async (_event, context) => {
        signal = context.signal;
        await new Promise((resolve) => setTimeout(resolve, 60));
      },
      { id: 'h', transactional: true },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect((signal!.reason as Error).message).toMatch(/expired/);
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
    // The lease ran out locally while the store still lists the delivery as ours: the attempt counts as
    // failed and the delivery is rescheduled instead of staying in flight until some other worker takes the lane over.
    expect((await bus.listDeliveries())[0]).toMatchObject({
      status: 'pending',
      attempts: 1,
      lastError: { name: 'LeaseLostError' },
    });
  });
});
