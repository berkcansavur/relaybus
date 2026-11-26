import { afterEach, describe, expect, it, vi } from 'vitest';
import { InMemoryEventStore, RelayBus, createRetryPolicy } from '@relaybus/core';
import { toMilliseconds } from '../src/duration.js';
import { computeRetryDelay } from '../src/retry-policy.js';
import { OrderPlaced, createLogger, createTestBus, deferred, waitFor } from './helpers.js';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

// These tests assert the correct behaviour of open runtime defects. They are written as `it.fails`
// so the suite stays green while a defect exists and turns red once it is fixed: when a fix lands,
// change `it.fails` to `it`.

describe('retry delay and durations', () => {
  // retry-policy.ts: initialDelayMs * factor ** (attempt - 1) is 0 * Infinity = NaN, so the
  // delivery would never become due again.
  it.fails('keeps the retry delay finite for a zero initial delay and a huge attempt count', () => {
    const policy = createRetryPolicy({ attempts: Infinity, initialDelay: 0, jitter: 'none' });
    const delay = computeRetryDelay(policy, 2_000);
    expect(Number.isFinite(delay)).toBe(true);
    expect(delay).toBeGreaterThanOrEqual(0);
  });

  // duration.ts: the number path rejects Infinity, the string path has no finiteness check.
  it.fails('rejects a duration string that overflows to Infinity like a number does', () => {
    expect(() => toMilliseconds(Infinity, 'x')).toThrow();
    expect(() => toMilliseconds(`${'9'.repeat(400)}ms` as never, 'x')).toThrow();
  });
});

describe('dispatcher robustness', () => {
  // claim-loop.ts: a logger that throws inside notifier.failure escapes the loop and ends it.
  it.fails('keeps claiming after the logger threw while reporting a failed claim', async () => {
    const store = new InMemoryEventStore({ retainCompleted: true });
    const logger = createLogger();
    logger.error.mockImplementation(() => {
      throw new Error('logger down');
    });
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => void unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);
    const bus = new RelayBus({
      store,
      logger,
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, shutdownTimeout: 200 },
    });
    try {
      const handled: string[] = [];
      bus.subscribe(OrderPlaced, (event) => void handled.push(event.payload.orderId), { id: 'h' });
      const realClaim = store.claim.bind(store);
      let failures = 0;
      vi.spyOn(store, 'claim').mockImplementation(async (request) => {
        if (failures === 0) {
          failures += 1;
          throw new Error('db blip');
        }
        return realClaim(request);
      });
      await bus.start();
      await bus.publish(new OrderPlaced({ orderId: '1' }));
      await waitFor(() => handled.length === 1, 1_000).catch(() => undefined);
      expect({ handled, unhandled: unhandled.length }).toEqual({ handled: ['1'], unhandled: 0 });
    } finally {
      await bus.stop().catch(() => undefined);
      process.off('unhandledRejection', onUnhandled);
    }
  });

  // delivery-executor.ts (guardTransactionalLease): a failing extendLease before the handler ran is
  // charged as a handler failure.
  it.fails(
    'does not dead-letter a delivery whose lease extension failed once before the handler ran',
    async () => {
      const { bus, store } = createTestBus({ transactionMode: 'optional' });
      const handler = vi.fn();
      bus.subscribe(OrderPlaced, handler, {
        id: 'h',
        transactional: true,
        timeout: '1m', // not below the lease, so the dispatcher extends the lease once first
        retry: { attempts: 1 },
      });
      await bus.publish(new OrderPlaced({ orderId: '1' }));
      vi.spyOn(store, 'extendLease').mockRejectedValueOnce(new Error('connection reset'));
      await bus.runOnce();
      const [record] = await bus.listDeliveries();
      expect({ handlerCalls: handler.mock.calls.length, status: record!.status }).toEqual({
        handlerCalls: 0,
        status: 'pending',
      });
    },
  );

  // delivery-executor.ts (guardTransactionalLease): the lease guard timer exceeds the platform
  // timer maximum, fires after 1 ms and aborts a healthy handler.
  it.fails(
    'does not abort a healthy transactional handler whose timeout is near the timer maximum',
    async () => {
      const { bus } = createTestBus({ transactionMode: 'optional' });
      let aborted: unknown;
      bus.subscribe(
        OrderPlaced,
        async (_event, context) => {
          await new Promise((resolve) => setTimeout(resolve, 20));
          aborted = context.signal.aborted ? context.signal.reason : undefined;
        },
        { id: 'h', transactional: true, timeout: 2_147_483_000 },
      );
      await bus.publish(new OrderPlaced({ orderId: '1' }));
      await bus.runOnce();
      expect(aborted).toBeUndefined();
    },
  );

  // handler-invocation.ts (waitUntil): stop() abandons a handler that ignores its abort, but the
  // wait for it (one lease duration) stays scheduled, which keeps the process alive.
  it.fails(
    'leaves no timer behind after stop() abandoned a handler that ignores its abort',
    async () => {
      vi.useFakeTimers();
      const store = new InMemoryEventStore({ retainCompleted: true });
      const bus = new RelayBus({
        store,
        logger: createLogger(),
        transactionMode: 'optional',
        handlerTimeout: 100,
        dispatcher: { pollInterval: 1_000, leaseDuration: 60_000, shutdownTimeout: 500 },
      });
      bus.subscribe(OrderPlaced, () => new Promise<void>(() => undefined), { id: 'h' });
      await bus.start();
      await bus.publish(new OrderPlaced({ orderId: '1' }));
      await vi.advanceTimersByTimeAsync(300);
      const stopping = bus.stop();
      await vi.advanceTimersByTimeAsync(600);
      const result = await stopping;
      expect(result.abandonedDeliveries).toBe(1);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  // claim-loop.ts (claimAndStart): a claim that was stalled past the shutdown timeout still resumes
  // the loop's claim round for the remaining handlers after stop() returned and closed the store.
  it.fails(
    'does not claim again after stop() returned when a claim stalled past the shutdown timeout',
    async () => {
      const store = new InMemoryEventStore({ retainCompleted: true });
      const bus = new RelayBus({
        store,
        logger: createLogger(),
        transactionMode: 'optional',
        dispatcher: { pollInterval: 5, shutdownTimeout: 50, concurrency: 10 },
      });
      bus.subscribe(OrderPlaced, () => undefined, { id: 'h1', concurrency: 2 });
      bus.subscribe(OrderPlaced, () => undefined, { id: 'h2', concurrency: 2 });
      const gate = deferred<void>();
      const realClaim = store.claim.bind(store);
      let stopReturned = false;
      let claimsAfterStop = 0;
      let calls = 0;
      vi.spyOn(store, 'claim').mockImplementation(async (request) => {
        calls += 1;
        if (stopReturned) {
          claimsAfterStop += 1;
        }
        if (calls === 1) {
          await gate.promise;
        }
        return realClaim(request);
      });
      await bus.start();
      await waitFor(() => calls >= 1);
      await bus.stop();
      stopReturned = true;
      gate.resolve();
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(claimsAfterStop).toBe(0);
    },
  );

  // dispatcher.ts (processBatch): concurrent runOnce calls each plan against the same free slots,
  // so together they exceed the dispatcher concurrency.
  it.fails(
    'never runs more unkeyed deliveries at once than the dispatcher concurrency across concurrent runOnce calls',
    async () => {
      const { bus } = createTestBus({
        transactionMode: 'optional',
        dispatcher: { concurrency: 1 },
      });
      let running = 0;
      let peak = 0;
      const gate = deferred<void>();
      bus.subscribe(
        OrderPlaced,
        async () => {
          running += 1;
          peak = Math.max(peak, running);
          await gate.promise;
          running -= 1;
        },
        { id: 'h', concurrency: 1 },
      );
      await bus.publish(new OrderPlaced({ orderId: '1' }));
      await bus.publish(new OrderPlaced({ orderId: '2' }));
      const first = bus.runOnce();
      const second = bus.runOnce();
      await new Promise((resolve) => setTimeout(resolve, 20));
      gate.resolve();
      await Promise.all([first, second]);
      expect(peak).toBe(1);
    },
  );
});
