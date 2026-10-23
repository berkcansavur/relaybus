import { afterEach, describe, expect, it, vi } from 'vitest';
import { InMemoryEventStore, type ClaimRequest, type ClaimedDelivery } from '@relaybus/core';
import { Dispatcher } from '../src/dispatcher.js';
import { HandlerRegistry } from '../src/handler-registry.js';
import { Notifier } from '../src/notifier.js';
import { OrderPlaced, createObserver, createTestBus, deferred, waitFor } from './helpers.js';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

/** A store that reports new deliveries like the database stores do, through the listener it was given. */
class WatchingStore extends InMemoryEventStore {
  listener: (() => void) | undefined;
  readonly unwatch = vi.fn(async () => undefined);
  readonly claims: ClaimRequest[] = [];

  override async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    this.claims.push(request);
    return super.claim(request);
  }

  async watchNewDeliveries(listener: () => void): Promise<() => Promise<void>> {
    this.listener = listener;
    return this.unwatch;
  }
}

function createDispatcher(store: InMemoryEventStore, observer = createObserver()) {
  const registry = new HandlerRegistry();
  registry.register(
    {
      eventType: 'order.placed.v1',
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
  return new Dispatcher({
    store,
    registry,
    clock: { now: () => new Date(0) },
    random: () => 0,
    notifier: new Notifier([observer], { log: vi.fn(), warn: vi.fn(), error: vi.fn() }),
    settings: {
      owner: 'o',
      concurrency: 1,
      batchSize: 1,
      pollIntervalMs: 60_000,
      leaseDurationMs: 60_000,
      shutdownTimeoutMs: 1_000,
    },
  });
}

describe('dispatcher and the store reports of new deliveries', () => {
  it('claims when the store reports new deliveries and stops listening when it stops', async () => {
    const store = new WatchingStore();
    const { bus } = createTestBus({ store, transactionMode: 'optional' });
    const handled: string[] = [];
    bus.subscribe(OrderPlaced, (event) => void handled.push(event.payload.orderId), { id: 'h' });
    await bus.start();
    // The start-up claim, then a catch-up claim once the store listens.
    await waitFor(() => store.listener !== undefined && store.claims.length === 2);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(store.claims).toHaveLength(2);
    // The bus leaves waking to the store: a publish alone does not claim.
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(handled).toEqual([]);
    store.listener!();
    await waitFor(() => handled.length === 1);
    await bus.stop();
    expect(store.unwatch).toHaveBeenCalledTimes(1);
  });

  it('keeps polling and reports the failure when the store cannot start listening', async () => {
    const store = new InMemoryEventStore();
    Object.assign(store, {
      watchNewDeliveries: () => Promise.reject(new Error('no listener')),
    });
    const observer = createObserver();
    const dispatcher = createDispatcher(store, observer);
    dispatcher.start();
    await waitFor(() => vi.mocked(observer.onFailure).mock.calls.length === 1);
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'watch',
      error: expect.objectContaining({ message: 'no listener' }),
    });
    expect(dispatcher.watchesNewDeliveries).toBe(false);
    await dispatcher.stop();
  });

  it('reports a failure to stop listening and still stops', async () => {
    const store = new WatchingStore();
    store.unwatch.mockRejectedValueOnce(new Error('close failed'));
    const observer = createObserver();
    const dispatcher = createDispatcher(store, observer);
    dispatcher.start();
    await waitFor(() => dispatcher.watchesNewDeliveries);
    await dispatcher.stop();
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'watch',
      error: expect.objectContaining({ message: 'close failed' }),
    });
    expect(dispatcher.watchesNewDeliveries).toBe(false);
  });

  it('stops a listener that started only after the dispatcher stopped', async () => {
    const store = new WatchingStore();
    const started = deferred<() => Promise<void>>();
    vi.spyOn(store, 'watchNewDeliveries').mockReturnValue(started.promise);
    const dispatcher = createDispatcher(store);
    dispatcher.start();
    const stopping = dispatcher.stop();
    started.resolve(store.unwatch);
    await stopping;
    expect(store.unwatch).toHaveBeenCalledTimes(1);
    expect(dispatcher.watchesNewDeliveries).toBe(false);
  });

  it('polls when the store cannot listen', async () => {
    const store = new WatchingStore();
    vi.spyOn(store, 'watchNewDeliveries').mockResolvedValue(undefined as never);
    const dispatcher = createDispatcher(store);
    dispatcher.start();
    await waitFor(() => store.claims.length === 1);
    expect(dispatcher.watchesNewDeliveries).toBe(false);
    await dispatcher.stop();
  });
});

describe('dispatcher claim scheduling', () => {
  it('merges wake-ups that follow a claim closely into one claim', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setImmediate', 'Date'] });
    const store = new WatchingStore();
    const dispatcher = createDispatcher(store);
    dispatcher.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(store.claims).toHaveLength(1);
    // Right after the start-up claim: the first wake-up waits for the end of the cooldown, the others join it.
    dispatcher.wake();
    dispatcher.wake();
    dispatcher.wake();
    await vi.advanceTimersByTimeAsync(0);
    expect(store.claims).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(10);
    expect(store.claims).toHaveLength(2);
    // After a quiet period a wake-up claims at once.
    await vi.advanceTimersByTimeAsync(100);
    dispatcher.wake();
    await vi.advanceTimersByTimeAsync(0);
    expect(store.claims).toHaveLength(3);
    const stopping = dispatcher.stop();
    await vi.advanceTimersByTimeAsync(10);
    await stopping;
  });

  it('refills a single free slot after a short quiet time instead of waiting for the poll', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { concurrency: 4, pollInterval: '1h' },
    });
    const claim = vi.spyOn(store, 'claim');
    const slow = deferred();
    const started: string[] = [];
    bus.subscribe(
      OrderPlaced,
      async (event) => {
        started.push(event.payload.orderId);
        if (event.payload.orderId.startsWith('slow')) {
          await slow.promise;
        }
      },
      { id: 'h' },
    );
    await bus.publishAll(
      ['fast', 'slow-1', 'slow-2', 'slow-3'].map((orderId) => new OrderPlaced({ orderId })),
    );
    await bus.start();
    await waitFor(() => started.length === 4);
    // Three slots stay busy: one free slot is below the refill threshold (3 of 4), so the event
    // published now is claimed alone after the quiet time, not after the poll interval.
    await bus.publish(new OrderPlaced({ orderId: 'next' }));
    await waitFor(() => started.includes('next'));
    expect(claim.mock.calls[0]![0].limit).toBe(4);
    expect(claim.mock.calls.slice(1).map(([request]) => request.limit)).toContain(1);
    expect(claim.mock.calls.every(([request]) => [1, 4].includes(request.limit))).toBe(true);
    slow.resolve();
    await bus.stop();
  });
});
