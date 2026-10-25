import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  InMemoryEventStore,
  LifecycleError,
  RelayBus,
  type CloseOptions,
  type InMemoryTransaction,
  type RelayBusOptions,
  type StopResult,
} from '@relaybus/core';
import { FakeClock } from '@relaybus/testing';
import {
  OrderPlaced,
  createLogger,
  createTestBus,
  deferred,
  waitFor,
  type TestBus,
} from './helpers.js';

/** Shutdown has a deadline: nothing it waits for may hold it longer than the shutdown timeout. */
describe('shutdown deadline', () => {
  it('does not wait for a store that never answers the relinquish call', async () => {
    const { bus, store } = createTestBus({
      dispatcher: { pollInterval: 5, shutdownTimeout: 20 },
    });
    Object.assign(store, { relinquish: () => new Promise<void>(() => undefined) });
    await bus.start();
    const started = Date.now();
    await bus.stop();
    expect(Date.now() - started).toBeLessThan(2_000);
  }, 10_000);

  it('writes the completions still waiting for the batch window as soon as shutdown begins', async () => {
    const { bus } = createTestBus({
      transactionMode: 'optional',
      dispatcher: {
        pollInterval: 5,
        shutdownTimeout: 20,
        concurrency: 2,
        batchSize: 2,
        completionBatchWindow: 700,
      },
    });
    let slowRunning = false;
    bus.subscribe(
      OrderPlaced,
      async (event, context) => {
        if (event.payload.orderId === 'slow') {
          slowRunning = true;
          await new Promise<void>((resolve) =>
            context.signal.addEventListener('abort', () => resolve()),
          );
        }
      },
      { id: 'h' },
    );
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: 'slow' }));
    await waitFor(() => slowRunning, 3_000);
    await bus.publish(new OrderPlaced({ orderId: 'fast' }));
    // The fast delivery finished and waits for company in the batch window while the slow one still runs.
    await new Promise((resolve) => setTimeout(resolve, 100));
    const started = Date.now();
    await bus.stop();
    expect(Date.now() - started).toBeLessThan(400);
  }, 10_000);
});

/** `stop` is bounded end to end: past the deadline it aborts, gives up waiting and forces closing. */
describe('stop deadline', () => {
  const never = (): Promise<never> => new Promise<never>(() => undefined);

  /** A bus on a clock that owns every timer, with a handler that never settles and ignores its signal. */
  async function startStuckBus(
    options: Partial<RelayBusOptions<InMemoryTransaction>> = {},
  ): Promise<TestBus & { readonly signals: AbortSignal[] }> {
    const clock = new FakeClock(0, { controlTimers: true });
    const testBus = createTestBus({
      clock,
      transactionMode: 'optional',
      retention: { completed: false },
      dispatcher: { shutdownTimeout: 1_000, leaseDuration: 3_000 },
      ...options,
    });
    const signals: AbortSignal[] = [];
    testBus.bus.subscribe(
      OrderPlaced,
      (_event, context) => {
        signals.push(context.signal);
        return never();
      },
      { id: 'stuck' },
    );
    await testBus.bus.publish(new OrderPlaced({ orderId: 'o-1' }));
    await testBus.bus.start();
    await waitFor(() => signals.length === 1);
    return { ...testBus, clock, signals };
  }

  it('aborts running handlers at four fifths of the timeout and releases their deliveries', async () => {
    const { bus, clock, logger, observer, signals } = await startStuckBus();
    const stopping = bus.stop();
    clock.advance(799);
    await flush();
    expect(signals[0]!.aborted).toBe(false);
    clock.advance(1);
    await expect(stopping).resolves.toEqual({
      graceful: true,
      abandonedDeliveries: 0,
      durationMs: 800,
    });
    expect(signals[0]!.aborted).toBe(true);
    expect(observer.onDeliveryReleased).toHaveBeenCalledTimes(1);
    expect(logger.warn).not.toHaveBeenCalled();
    expect(clock.pendingTimers).toBe(0);
  });

  it('resolves at the deadline with the delivery whose release hangs, and warns', async () => {
    const { bus, clock, logger, store } = await startStuckBus();
    vi.spyOn(store, 'release').mockImplementation(never);
    let result: StopResult | undefined;
    const stopping = bus.stop().then((stopped) => (result = stopped));
    clock.advance(999);
    await flush();
    expect(result).toBeUndefined();
    clock.advance(1);
    await stopping;
    expect(result).toEqual({ graceful: false, abandonedDeliveries: 1, durationMs: 1_000 });
    expect(logger.warn).toHaveBeenCalledWith(
      '[RelayBus](stop) Shutdown deadline passed, forced close | abandoned=1 | timeoutMs=1000',
    );
    expect(clock.pendingTimers).toBe(0);
  });

  it('takes the timeout of the call over dispatcher.shutdownTimeout', async () => {
    const { bus, clock, store } = await startStuckBus();
    vi.spyOn(store, 'release').mockImplementation(never);
    const stopping = bus.stop({ timeout: '200ms' });
    clock.advance(200);
    await expect(stopping).resolves.toMatchObject({ graceful: false, durationMs: 200 });
  });

  it('rejects an invalid timeout before stopping anything', async () => {
    const { bus } = createTestBus();
    await bus.start();
    await expect(bus.stop({ timeout: -1 })).rejects.toThrow(ConfigurationError);
    expect(bus.isRunning).toBe(true);
    await bus.stop();
  });

  it('reports a graceful stop without a warning', async () => {
    const { bus, logger } = createTestBus();
    await bus.start();
    const result = await bus.stop();
    expect(result).toEqual({ graceful: true, abandonedDeliveries: 0, durationMs: 0 });
    expect(logger.warn).not.toHaveBeenCalled();
    await expect(bus.stop()).resolves.toBe(result);
  });

  it('reports a graceful stop of a bus that never started or failed to start', async () => {
    const idle = createTestBus().bus;
    await expect(idle.stop()).resolves.toEqual({
      graceful: true,
      abandonedDeliveries: 0,
      durationMs: 0,
    });
    const failing = createTestBus({
      transports: [
        {
          name: 'broken',
          connect: () => Promise.reject(new Error('refused')),
          publish: () => Promise.resolve(),
          close: () => Promise.resolve(),
        },
      ],
    }).bus;
    await expect(failing.start()).rejects.toThrow('refused');
    await expect(failing.stop()).resolves.toEqual({
      graceful: true,
      abandonedDeliveries: 0,
      durationMs: 0,
    });
  });

  it('bounds an idle bus that recovers settlements on a store that never answers', async () => {
    const clock = new FakeClock(0, { controlTimers: true });
    const { bus, store } = createTestBus({
      clock,
      transactionMode: 'optional',
      dispatcher: { shutdownTimeout: 500 },
    });
    bus.subscribe(OrderPlaced, () => Promise.resolve(), { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
    vi.spyOn(store, 'complete').mockRejectedValue(new Error('down'));
    const running = bus.runOnce();
    // Three retries of the completion, then it is left to recovery.
    for (let step = 0; step < 4; step += 1) {
      await flush();
      clock.advance(1_000);
    }
    await running;
    vi.spyOn(store, 'complete').mockImplementation(never);
    const stopping = bus.stop();
    await flush();
    clock.advance(500);
    await expect(stopping).resolves.toEqual({
      graceful: false,
      abandonedDeliveries: 0,
      durationMs: 500,
    });
  });

  it('force-closes a transport and a store whose close never resolves', async () => {
    const transportSignals: (AbortSignal | undefined)[] = [];
    const storeSignals: (AbortSignal | undefined)[] = [];
    const store = new InMemoryEventStore();
    Object.assign(store, {
      close: (options?: CloseOptions) => {
        storeSignals.push(options?.signal);
        return never();
      },
    });
    const clock = new FakeClock(0, { controlTimers: true });
    const { bus } = createTestBus({
      clock,
      store,
      dispatcher: { shutdownTimeout: 300 },
      transports: [
        {
          name: 'hung',
          publish: () => Promise.resolve(),
          close: (options?: CloseOptions) => {
            transportSignals.push(options?.signal);
            return never();
          },
        },
      ],
    });
    await bus.start();
    const stopping = bus.stop();
    await flush();
    expect(transportSignals).toHaveLength(1);
    expect(transportSignals[0]!.aborted).toBe(false);
    clock.advance(300);
    await expect(stopping).resolves.toEqual({
      graceful: false,
      abandonedDeliveries: 0,
      durationMs: 300,
    });
    expect(transportSignals[0]!.aborted).toBe(true);
    // The store was still closed, with a signal that already says to force.
    expect(storeSignals).toHaveLength(1);
    expect(storeSignals[0]!.aborted).toBe(true);
  });

  it('closes every transport and the store when the deadline passed while handlers ran', async () => {
    const closed: boolean[] = [];
    const { bus, clock, store } = await startStuckBus({
      transports: [
        {
          name: 'a',
          publish: () => Promise.resolve(),
          close: (options?: CloseOptions) => {
            closed.push(options!.signal!.aborted);
            return Promise.resolve();
          },
        },
      ],
    });
    vi.spyOn(store, 'release').mockImplementation(never);
    const stopping = bus.stop();
    clock.advance(1_000);
    await expect(stopping).resolves.toMatchObject({ graceful: false, abandonedDeliveries: 1 });
    expect(closed).toEqual([true]);
  });

  it('gives up a start that hangs, force-closes and makes the start reject when it resumes', async () => {
    const store = new InMemoryEventStore();
    const setup = deferred();
    const closeSignals: (AbortSignal | undefined)[] = [];
    Object.assign(store, {
      setup: () => setup.promise,
      close: (options?: CloseOptions) => {
        closeSignals.push(options?.signal);
        return Promise.resolve();
      },
    });
    const clock = new FakeClock(0, { controlTimers: true });
    const { bus, logger } = createTestBus({ clock, store, dispatcher: { shutdownTimeout: 400 } });
    const starting = bus.start();
    const stopping = bus.stop();
    clock.advance(400);
    await expect(stopping).resolves.toEqual({
      graceful: false,
      abandonedDeliveries: 0,
      durationMs: 400,
    });
    expect(closeSignals[0]!.aborted).toBe(true);
    expect(logger.warn).toHaveBeenCalledWith(
      '[RelayBus](stop) Shutdown deadline passed, forced close | abandoned=0 | timeoutMs=400',
    );
    setup.resolve();
    await expect(starting).rejects.toThrow(LifecycleError);
    expect(bus.isRunning).toBe(false);
    expect(clock.pendingTimers).toBe(0);
  });

  it('still rejects when the store fails to close before the deadline', async () => {
    const store = new InMemoryEventStore();
    Object.assign(store, { close: () => Promise.reject(new Error('close failed')) });
    const { bus } = createTestBus({ store });
    await bus.start();
    await expect(bus.stop()).rejects.toThrow('close failed');
  });

  it('redelivers an abandoned delivery once its lease expires', async () => {
    const { bus, clock, store } = await startStuckBus();
    // The database hangs: the release of the aborted delivery never answers.
    vi.spyOn(store, 'release').mockImplementation(never);
    const stopping = bus.stop();
    clock.advance(1_000);
    await expect(stopping).resolves.toMatchObject({ abandonedDeliveries: 1 });
    vi.mocked(store.release).mockRestore();
    const attempts: number[] = [];
    const next = new RelayBus<InMemoryTransaction>({
      store,
      clock,
      logger: createLogger(),
      dispatcher: { enabled: false, leaseDuration: 3_000 },
    });
    next.subscribe(OrderPlaced, (_event, context) => void attempts.push(context.attempt), {
      id: 'stuck',
    });
    await expect(next.runOnce()).resolves.toBe(0);
    clock.advance(3_000);
    await expect(next.runOnce()).resolves.toBe(1);
    expect(attempts).toEqual([2]);
    await next.stop();
  });

  it('does not run deliveries a claim returns after the deadline', async () => {
    const clock = new FakeClock(0, { controlTimers: true });
    const { bus, store } = createTestBus({
      clock,
      transactionMode: 'optional',
      dispatcher: { shutdownTimeout: 100 },
    });
    const handled: string[] = [];
    bus.subscribe(OrderPlaced, (event) => void handled.push(event.payload.orderId), { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: 'late' }));
    const claim = store.claim.bind(store);
    const claimed = deferred();
    vi.spyOn(store, 'claim').mockImplementation(async (request) => {
      await claimed.promise;
      return claim(request);
    });
    await bus.start();
    const stopping = bus.stop();
    clock.advance(100);
    await expect(stopping).resolves.toMatchObject({ graceful: false, abandonedDeliveries: 0 });
    claimed.resolve();
    await flush();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(handled).toEqual([]);
  });

  it('stops retrying settlements that fail after the deadline', async () => {
    const { bus, clock, store, observer } = await startStuckBus();
    const release = deferred<boolean>();
    vi.spyOn(store, 'release').mockReturnValue(release.promise);
    const stopping = bus.stop();
    clock.advance(1_000);
    await expect(stopping).resolves.toMatchObject({ abandonedDeliveries: 1 });
    release.reject(new Error('store closed'));
    await flush();
    expect(observer.onFailure).toHaveBeenCalledWith(expect.objectContaining({ stage: 'release' }));
    expect(clock.pendingTimers).toBe(0);
  });
});

async function flush(): Promise<void> {
  for (let turn = 0; turn < 20; turn += 1) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}
