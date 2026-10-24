import { describe, expect, it, vi } from 'vitest';
import {
  HandlerTimeoutError,
  InMemoryEventStore,
  NonRetryableError,
  type EventEnvelope,
} from '@relaybus/core';
import { Dispatcher } from '../src/dispatcher.js';
import { HandlerRegistry } from '../src/handler-registry.js';
import { Notifier } from '../src/notifier.js';
import { OrderPlaced, createLogger, createTestBus, deferred, waitFor } from './helpers.js';

describe('retries and dead letters', () => {
  it('retries with backoff and succeeds', async () => {
    const { bus, clock, observer } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 5, initialDelay: '1s', factor: 2, jitter: 'none' },
    });
    const attempts: number[] = [];
    bus.subscribe(
      OrderPlaced,
      (_event, context) => {
        attempts.push(context.attempt);
        if (context.attempt < 3) {
          throw new TypeError(`failure ${context.attempt}`);
        }
      },
      { id: 'flaky' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    let [record] = await bus.listDeliveries();
    expect(record).toMatchObject({
      status: 'pending',
      attempts: 1,
      lastError: { name: 'TypeError', message: 'failure 1' },
    });
    expect(record!.availableAt).toEqual(new Date(clock.now().getTime() + 1_000));
    expect(await bus.runOnce()).toBe(0);
    clock.advance(1_000);
    await bus.runOnce();
    [record] = await bus.listDeliveries();
    expect(record!.availableAt).toEqual(new Date(clock.now().getTime() + 2_000));
    clock.advance(2_000);
    await bus.runOnce();
    expect(attempts).toEqual([1, 2, 3]);
    expect((await bus.listDeliveries())[0]!.status).toBe('completed');
    expect(observer.onDeliveryRetryScheduled).toHaveBeenCalledTimes(2);
  });

  it('dead-letters after the last attempt and can be requeued', async () => {
    const { bus, clock, observer } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 2, initialDelay: 10, jitter: 'none' },
    });
    let healthy = false;
    bus.subscribe(
      OrderPlaced,
      () => {
        if (!healthy) {
          throw new Error('down');
        }
      },
      { id: 'h' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    clock.advance(10);
    await bus.runOnce();
    const [dead] = await bus.listDeliveries({ status: 'dead' });
    expect(dead).toMatchObject({ attempts: 2, lastError: { message: 'down' } });
    expect(observer.onDeliveryDead).toHaveBeenCalledTimes(1);
    healthy = true;
    expect(await bus.requeue([dead!.id])).toBe(1);
    expect(await bus.requeue([dead!.id])).toBe(0);
    await bus.runOnce();
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'completed', attempts: 1 });
  });

  it('dead-letters non-retryable errors immediately', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new NonRetryableError('invalid order');
      },
      { id: 'h' },
    );
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new RangeError('filtered');
      },
      { id: 'filtered', retry: { retryIf: (error) => !(error instanceof RangeError) } },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    const records = await bus.listDeliveries();
    expect(records.map((record) => [record.status, record.attempts])).toEqual([
      ['dead', 1],
      ['dead', 1],
    ]);
  });

  it('records the class name and code of a relaybus error in the dead letter and the observer event', async () => {
    const { bus, observer } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new NonRetryableError('invalid order');
      },
      { id: 'h' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    const [dead] = await bus.listDeliveries({ status: 'dead' });
    const error = {
      name: 'NonRetryableError',
      message: 'invalid order',
      code: 'RELAYBUS_NON_RETRYABLE',
    };
    expect(dead!.lastError).toMatchObject(error);
    expect(observer.onDeliveryDead).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining(error) }),
    );
  });

  it('dead-letters a delivery whose lease expired on its last attempt without running it', async () => {
    const { bus, store, clock } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 1 },
    });
    const handler = vi.fn();
    bus.subscribe(OrderPlaced, handler, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await store.claim({
      handlers: ['h'],
      limit: 1,
      leaseOwner: 'crashed-worker',
      leaseExpiresAt: new Date(clock.now().getTime() + 1),
      now: clock.now(),
    });
    clock.advance(1);
    await bus.runOnce();
    expect(handler).not.toHaveBeenCalled();
    const [record] = await bus.listDeliveries();
    expect(record).toMatchObject({ status: 'dead', attempts: 2 });
    expect(record!.lastError!.message).toBe(
      'Delivery reached attempt 2 of 1 after its lease expired',
    );
    expect(record!.lastError).toMatchObject({
      name: 'AttemptsExhaustedError',
      code: 'RELAYBUS_ATTEMPTS_EXHAUSTED',
    });
  });

  it('times out slow handlers, aborts their signal and retries', async () => {
    const { bus } = createTestBus({
      transactionMode: 'optional',
      handlerTimeout: 20,
      dispatcher: { leaseDuration: 60 },
    });
    let signal: AbortSignal | undefined;
    bus.subscribe(
      OrderPlaced,
      (_event, context) => {
        signal = context.signal;
        return new Promise(() => undefined);
      },
      { id: 'slow' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(signal!.aborted).toBe(true);
    expect(signal!.reason).toBeInstanceOf(HandlerTimeoutError);
    const [record] = await bus.listDeliveries();
    expect(record).toMatchObject({
      status: 'pending',
      lastError: {
        name: 'HandlerTimeoutError',
        message: "Handler 'slow' did not settle within 20ms",
      },
    });
  });
});

describe('leases', () => {
  it('renews the lease while a handler runs', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { leaseDuration: 30 },
    });
    const extend = vi.spyOn(store, 'extendLease');
    bus.subscribe(OrderPlaced, () => new Promise((resolve) => setTimeout(resolve, 60)), {
      id: 'h',
    });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(extend.mock.calls.length).toBeGreaterThanOrEqual(2);
    expect((await bus.listDeliveries())[0]!.status).toBe('completed');
  });

  it('abandons a delivery whose lease was taken over', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { leaseDuration: 30 },
    });
    vi.spyOn(store, 'extendLease').mockResolvedValue(false);
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
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
    expect((await bus.listDeliveries())[0]!.status).toBe('processing');
  });

  it('reports heartbeat failures and keeps going', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { leaseDuration: 30 },
    });
    vi.spyOn(store, 'extendLease').mockRejectedValue(new Error('db down'));
    bus.subscribe(OrderPlaced, () => new Promise((resolve) => setTimeout(resolve, 40)), {
      id: 'h',
    });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'extend-lease',
      error: expect.objectContaining({ message: 'db down' }),
    });
    expect((await bus.listDeliveries())[0]!.status).toBe('completed');
  });

  it('treats a refused completion as a lost lease', async () => {
    const { bus, store, observer } = createTestBus({ transactionMode: 'optional' });
    vi.spyOn(store, 'complete').mockResolvedValue(false);
    bus.subscribe(OrderPlaced, () => undefined, { id: 'plain' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'transactional', transactional: true });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(2);
    expect(observer.onDeliverySucceeded).not.toHaveBeenCalled();
  });

  it('leaves the delivery to lease expiry when completion fails', async () => {
    const { bus, store, observer } = createTestBus({ transactionMode: 'optional' });
    vi.spyOn(store, 'complete').mockRejectedValue(new Error('connection reset'));
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'complete',
      error: expect.objectContaining({ message: 'connection reset' }),
    });
    expect(observer.onDeliverySucceeded).not.toHaveBeenCalled();
    expect((await bus.listDeliveries())[0]!.status).toBe('processing');
  });

  it('retries when a transactional completion fails to commit', async () => {
    const { bus, store } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h', transactional: true });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    vi.spyOn(store, 'transaction').mockRejectedValueOnce(new Error('serialization failure'));
    await bus.runOnce();
    expect((await bus.listDeliveries())[0]).toMatchObject({
      status: 'pending',
      lastError: { message: 'serialization failure' },
    });
  });

  it('reports reschedule failures and leases lost at reschedule time', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 2 },
    });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new Error('handler failed');
      },
      { id: 'h' },
    );
    vi.spyOn(store, 'reschedule')
      .mockRejectedValueOnce(new Error('reschedule down'))
      .mockResolvedValueOnce(false);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.publish(new OrderPlaced({ orderId: '2' }));
    await bus.runOnce();
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'reschedule',
      error: expect.objectContaining({ message: 'reschedule down' }),
    });
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
    expect(observer.onDeliveryRetryScheduled).not.toHaveBeenCalled();
  });
});

describe('store failures while burying', () => {
  it('reports bury failures and lost leases', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 1 },
    });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new Error('fatal');
      },
      { id: 'h' },
    );
    const bury = vi
      .spyOn(store, 'bury')
      .mockRejectedValueOnce(new Error('bury down'))
      .mockResolvedValueOnce(false);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.publish(new OrderPlaced({ orderId: '2' }));
    await bus.runOnce();
    expect(bury).toHaveBeenCalledTimes(2);
    expect(observer.onFailure).toHaveBeenCalledWith(expect.objectContaining({ stage: 'bury' }));
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(1);
  });
});

describe('concurrency', () => {
  it('limits concurrent deliveries per handler and overall', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional', dispatcher: { concurrency: 3 } });
    let running = 0;
    let peakLimited = 0;
    let peakTotal = 0;
    const track = async (limited: boolean) => {
      running += 1;
      peakTotal = Math.max(peakTotal, running);
      if (limited) {
        peakLimited += 1;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
      running -= 1;
    };
    bus.subscribe(OrderPlaced, () => track(true), { id: 'limited', concurrency: 1 });
    bus.subscribe(OrderPlaced, () => track(false), { id: 'free' });
    for (let index = 0; index < 4; index += 1) {
      await bus.publish(new OrderPlaced({ orderId: String(index) }));
    }
    expect(await bus.runOnce()).toBe(3);
    expect(peakTotal).toBeLessThanOrEqual(3);
    expect(peakLimited).toBe(1);
    await bus.drain();
    const statuses = (await bus.listDeliveries()).map((record) => record.status);
    expect(statuses).toEqual(Array(8).fill('completed'));
  });

  it('skips limited handlers that are at capacity', async () => {
    const store = new InMemoryEventStore();
    const registry = new HandlerRegistry();
    const gate = deferred();
    const logger = createLogger();
    registry.register(
      {
        eventType: 'order.placed.v1',
        schema: undefined,
        handle: () => gate.promise,
        options: { id: 'single', concurrency: 1 },
        forwardsToTransport: false,
      },
      {
        retry: {
          attempts: 1,
          initialDelayMs: 0,
          factor: 1,
          maxDelayMs: 0,
          jitter: 'none',
          retryIf: undefined,
        },
        timeoutMs: null,
      },
    );
    registry.register(
      {
        eventType: 'order.placed.v1',
        schema: undefined,
        handle: () => gate.promise,
        options: { id: 'other' },
        forwardsToTransport: false,
      },
      {
        retry: {
          attempts: 1,
          initialDelayMs: 0,
          factor: 1,
          maxDelayMs: 0,
          jitter: 'none',
          retryIf: undefined,
        },
        timeoutMs: null,
      },
    );
    const dispatcher = new Dispatcher({
      store,
      registry,
      clock: { now: () => new Date(0) },
      random: () => 0,
      notifier: new Notifier([], logger),
      settings: {
        owner: 'o',
        concurrency: 5,
        batchSize: 5,
        pollIntervalMs: 5,
        leaseDurationMs: 60_000,
        shutdownTimeoutMs: 100,
      },
    });
    await store.transaction((transaction) =>
      store.append(
        transaction,
        ['a', 'b'].map((id) => ({
          event: {
            id,
            type: 'order.placed.v1',
            source: 's',
            key: null,
            payload: {},
            headers: {},
            correlationId: null,
            causationId: null,
            occurredAt: new Date(0),
          } satisfies EventEnvelope,
          deliveries: [
            { id: `d-${id}`, handler: 'single', availableAt: new Date(0) },
            { id: `o-${id}`, handler: 'other', availableAt: new Date(id === 'a' ? 0 : 1e12) },
          ],
        })),
        new Date(0),
      ),
    );
    const first = dispatcher.processBatch();
    await waitFor(() => dispatcher.inFlightCount === 2);
    expect(await dispatcher.processBatch()).toBe(0);
    gate.resolve();
    expect(await first).toBe(2);
    dispatcher.start();
    dispatcher.start();
    expect(dispatcher.isRunning).toBe(true);
    await waitFor(() => dispatcher.inFlightCount === 0);
    await dispatcher.stop();
    await dispatcher.stop();
    expect(dispatcher.isRunning).toBe(false);
  });
});

describe('dispatcher safety net', () => {
  it('reports unexpected errors instead of crashing the process', async () => {
    const store = new InMemoryEventStore();
    const registry = new HandlerRegistry();
    const logger = createLogger();
    const failures: string[] = [];
    vi.spyOn(registry, 'get').mockReturnValue(undefined);
    const dispatcher = new Dispatcher({
      store,
      registry,
      clock: { now: () => new Date(0) },
      random: () => 0,
      notifier: new Notifier([{ onFailure: (event) => void failures.push(event.stage) }], logger),
      settings: {
        owner: 'o',
        concurrency: 1,
        batchSize: 1,
        pollIntervalMs: 5,
        leaseDurationMs: 60_000,
        shutdownTimeoutMs: 100,
      },
    });
    vi.spyOn(registry, 'all').mockReturnValue([{ id: 'ghost', concurrency: null } as never]);
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [
          {
            event: {
              id: 'e',
              type: 't',
              source: 's',
              key: null,
              payload: {},
              headers: {},
              correlationId: null,
              causationId: null,
              occurredAt: new Date(0),
            },
            deliveries: [{ id: 'd', handler: 'ghost', availableAt: new Date(0) }],
          },
        ],
        new Date(0),
      ),
    );
    expect(await dispatcher.processBatch()).toBe(1);
    expect(failures).toEqual(['dispatch']);
  });
});
