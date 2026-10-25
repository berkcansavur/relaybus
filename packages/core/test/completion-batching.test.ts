import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError, InMemoryEventStore } from '@relaybus/core';
import { Dispatcher } from '../src/dispatcher.js';
import { HandlerRegistry } from '../src/handler-registry.js';
import { Notifier } from '../src/notifier.js';
import { OrderPlaced, createLogger, createTestBus, deferred, waitFor } from './helpers.js';

async function publishOrders(bus: ReturnType<typeof createTestBus>['bus'], count: number) {
  for (let index = 0; index < count; index += 1) {
    await bus.publish(new OrderPlaced({ orderId: String(index) }));
  }
}

describe('completion batching', () => {
  it('completes deliveries that finish together with one store call', async () => {
    const { bus, store } = createTestBus({ transactionMode: 'optional' });
    const completeMany = vi.spyOn(store, 'completeMany');
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await publishOrders(bus, 5);
    expect(await bus.runOnce()).toBe(5);
    expect(completeMany).toHaveBeenCalledTimes(1);
    expect(completeMany.mock.calls[0]![0]).toHaveLength(5);
    expect(await bus.statistics()).toMatchObject({ completed: 5, pending: 0, processing: 0 });
  });

  it('completes a lone delivery at once, without waiting for the window', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { completionBatchWindow: '1h' },
    });
    const completeMany = vi.spyOn(store, 'completeMany');
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await publishOrders(bus, 1);
    await bus.runOnce();
    expect(completeMany).toHaveBeenCalledTimes(1);
    expect(completeMany.mock.calls[0]![0]).toHaveLength(1);
  });

  it('writes a completion alone when its neighbours are still running after the window', async () => {
    const gate = deferred();
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { completionBatchWindow: 5 },
    });
    const completeMany = vi.spyOn(store, 'completeMany');
    bus.subscribe(
      OrderPlaced,
      (event) => (event.payload.orderId === '0' ? gate.promise : undefined),
      {
        id: 'h',
      },
    );
    await publishOrders(bus, 2);
    const batch = bus.runOnce();
    await waitFor(() => completeMany.mock.calls.length === 1);
    expect(completeMany.mock.calls[0]![0]).toHaveLength(1);
    gate.resolve();
    await batch;
    expect(completeMany).toHaveBeenCalledTimes(2);
  });

  it('writes a batch as soon as it is full', async () => {
    const gate = deferred();
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { completionBatchWindow: '1h', completionBatchSize: 2 },
    });
    const completeMany = vi.spyOn(store, 'completeMany');
    bus.subscribe(
      OrderPlaced,
      (event) => (event.payload.orderId === '0' ? gate.promise : undefined),
      {
        id: 'h',
      },
    );
    await publishOrders(bus, 3);
    const batch = bus.runOnce();
    await waitFor(() => completeMany.mock.calls.length === 1);
    expect(completeMany.mock.calls[0]![0]).toHaveLength(2);
    gate.resolve();
    await batch;
    expect(await bus.statistics()).toMatchObject({ completed: 3 });
  });

  it.each([
    [
      'rejects',
      (store: InMemoryEventStore) =>
        vi.spyOn(store, 'completeMany').mockRejectedValueOnce(new Error('connection reset')),
    ],
    [
      'throws',
      (store: InMemoryEventStore) =>
        vi.spyOn(store, 'completeMany').mockImplementationOnce(() => {
          throw new Error('connection reset');
        }),
    ],
  ])('retries every completion of a batch when the store call %s', async (_label, fail) => {
    const { bus, store, observer } = createTestBus({ transactionMode: 'optional' });
    fail(store);
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await publishOrders(bus, 3);
    await bus.runOnce();
    expect(await bus.statistics()).toMatchObject({ completed: 3, processing: 0 });
    expect(observer.onFailure).not.toHaveBeenCalled();
  });

  it('treats a refused completion of a batch as a lost lease', async () => {
    const { bus, store, observer } = createTestBus({ transactionMode: 'optional' });
    vi.spyOn(store, 'completeMany').mockImplementation(async (leases) => leases.map(() => false));
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await publishOrders(bus, 2);
    await bus.runOnce();
    expect(observer.onLeaseLost).toHaveBeenCalledTimes(2);
    expect(observer.onDeliverySucceeded).not.toHaveBeenCalled();
  });

  it('falls back to complete for a store without completeMany', async () => {
    const store = new InMemoryEventStore({ retainCompleted: true });
    Object.defineProperty(store, 'completeMany', { value: undefined });
    const { bus } = createTestBus({ store, transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await publishOrders(bus, 2);
    await bus.runOnce();
    expect(await bus.statistics()).toMatchObject({ completed: 2 });
  });

  it('completes one by one when the window is 0', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { completionBatchWindow: 0 },
    });
    const completeMany = vi.spyOn(store, 'completeMany');
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await publishOrders(bus, 2);
    await bus.runOnce();
    expect(completeMany).not.toHaveBeenCalled();
    expect(await bus.statistics()).toMatchObject({ completed: 2 });
  });

  it.each([
    ['completionBatchWindow', { completionBatchWindow: -1 }],
    ['completionBatchSize', { completionBatchSize: 0 }],
    ['completionBatchSize', { completionBatchSize: 1.5 }],
  ])('rejects an invalid %s', (_name, dispatcher) => {
    expect(() => createTestBus({ dispatcher })).toThrow(ConfigurationError);
  });
});

describe('relinquish at shutdown', () => {
  it('hands the lease groups of this dispatcher back after it stopped', async () => {
    const store = new InMemoryEventStore();
    const relinquish = vi.fn(async () => undefined);
    Object.assign(store, { relinquish });
    const { bus } = createTestBus({ store, transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.start();
    await bus.stop();
    expect(relinquish).toHaveBeenCalledTimes(1);
    expect(relinquish).toHaveBeenCalledWith(expect.stringContaining(':'), expect.any(Date));
  });

  it('reports a failing relinquish and still stops', async () => {
    const store = new InMemoryEventStore();
    Object.assign(store, { relinquish: vi.fn().mockRejectedValue(new Error('db down')) });
    const { bus, observer } = createTestBus({ store, transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.start();
    await bus.stop();
    expect(bus.isRunning).toBe(false);
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'relinquish',
      error: expect.objectContaining({ message: 'db down' }),
    });
  });
});

describe('dispatcher wake-up', () => {
  it('does not sleep when a wake-up arrived while it was claiming', async () => {
    const store = new InMemoryEventStore();
    const registry = new HandlerRegistry();
    registry.register(
      {
        eventTypes: ['order.placed.v1'],
        schema: undefined,
        handle: () => undefined,
        options: { id: 'h' },
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
    const firstClaim = deferred<never[]>();
    const claim = vi
      .spyOn(store, 'claim')
      .mockImplementationOnce(() => firstClaim.promise)
      .mockResolvedValue([]);
    const dispatcher = new Dispatcher({
      store,
      registry,
      clock: { now: () => new Date(0) },
      random: () => 0,
      notifier: new Notifier([], createLogger()),
      settings: {
        owner: 'o',
        concurrency: 1,
        batchSize: 1,
        pollIntervalMs: 60_000,
        leaseDurationMs: 60_000,
        shutdownTimeoutMs: 1_000,
      },
    });
    dispatcher.start();
    dispatcher.wake();
    firstClaim.resolve([]);
    await waitFor(() => claim.mock.calls.length >= 2);
    await dispatcher.stop();
  });
});
