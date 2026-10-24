import { describe, expect, it, vi } from 'vitest';
import { systemClock } from '../src/clock.js';
import { OrderPlaced, createTestBus, deferred, waitFor } from './helpers.js';

/** A completion that keeps failing after the dispatcher gave up on it must be retried until it lands. */
describe('completion recovery', () => {
  function failingBus(failures: number) {
    const test = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, completionBatchWindow: 0 },
    });
    const original = test.store.complete.bind(test.store);
    let remaining = failures;
    vi.spyOn(test.store, 'complete').mockImplementation(async (lease, now, transaction) => {
      if (remaining > 0) {
        remaining -= 1;
        throw new Error('connection reset');
      }
      return original(lease, now, transaction);
    });
    test.bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    return test;
  }

  it('completes the delivery once the store answers again, without waiting for the lease to expire', async () => {
    const { bus, observer } = failingBus(5);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(observer.onFailure).toHaveBeenCalledWith({
      stage: 'complete',
      error: expect.objectContaining({ message: 'connection reset' }),
    });
    await waitFor(() => vi.mocked(observer.onDeliverySucceeded).mock.calls.length === 1, 3_000);
    expect(await bus.statistics()).toMatchObject({ completed: 1, processing: 0 });
  }, 10_000);

  it('reports a lease that was lost while the completion was pending', async () => {
    const { bus, store, observer } = failingBus(4);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    vi.mocked(store.complete).mockResolvedValue(false);
    await waitFor(() => vi.mocked(observer.onLeaseLost).mock.calls.length === 1, 3_000);
    expect(observer.onDeliverySucceeded).not.toHaveBeenCalled();
  }, 10_000);

  it('keeps retrying while the store stays down and gives up quietly at shutdown', async () => {
    const { bus, store } = failingBus(1_000_000);
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await waitFor(() => vi.mocked(store.complete).mock.calls.length >= 8, 5_000);
    await bus.stop();
    const calls = vi.mocked(store.complete).mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(vi.mocked(store.complete).mock.calls.length).toBe(calls);
  }, 10_000);
});

describe('shutdown during a recovery pass', () => {
  it('waits for the recovery pass that is running before it stops', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, completionBatchWindow: 0 },
    });
    const original = store.complete.bind(store);
    const gate = deferred<void>();
    let calls = 0;
    vi.spyOn(store, 'complete').mockImplementation(async (lease, now, transaction) => {
      calls += 1;
      if (calls <= 4) {
        // The initial attempt and its three retries; the fifth call is the recovery pass of the timer.
        throw new Error('connection reset');
      }
      await gate.promise;
      return original(lease, now, transaction);
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    // The recovery pass of the timer is now inside the store.
    await waitFor(() => calls >= 5, 5_000);
    let stopped = false;
    const stopping = bus.stop().then(() => {
      stopped = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(stopped).toBe(false);
    gate.resolve();
    await stopping;
    const finished = calls;
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(calls).toBe(finished);
    expect(await bus.statistics()).toMatchObject({ completed: 1, processing: 0 });
  }, 10_000);
});

describe('shutdown while the store does not answer a recovery pass', () => {
  it('stops after the shutdown timeout and calls the store no more afterwards', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, completionBatchWindow: 0, shutdownTimeout: 20 },
    });
    const gate = deferred<void>();
    let hanging = false;
    vi.spyOn(store, 'complete').mockImplementation(async () => {
      if (hanging) {
        await gate.promise;
      }
      throw new Error('connection reset');
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.publish(new OrderPlaced({ orderId: '2' }));
    await waitFor(() => vi.mocked(observer.onFailure).mock.calls.length >= 2, 8_000);
    const before = vi.mocked(store.complete).mock.calls.length;
    hanging = true;
    // The pass of the timer hangs on the first pending settlement and the second is still waiting.
    await waitFor(() => vi.mocked(store.complete).mock.calls.length > before, 3_000);
    await bus.stop();
    const calls = vi.mocked(store.complete).mock.calls.length;
    gate.resolve();
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(vi.mocked(store.complete).mock.calls.length).toBe(calls);
  }, 15_000);
});

describe('shutdown of a dispatcher that only ran batches by hand', () => {
  it('stops the recovery of settlements that were left over', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, completionBatchWindow: 0, shutdownTimeout: 20 },
    });
    vi.spyOn(store, 'complete').mockRejectedValue(new Error('connection reset'));
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    await bus.stop();
    const calls = vi.mocked(store.complete).mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(vi.mocked(store.complete).mock.calls.length).toBe(calls);
  }, 10_000);
});

describe('shutdown while the last recovery pass does not get an answer', () => {
  it('stops after the shutdown timeout', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 60_000, completionBatchWindow: 0, shutdownTimeout: 20 },
    });
    let hanging = false;
    vi.spyOn(store, 'complete').mockImplementation(async () => {
      if (hanging) {
        await new Promise(() => undefined);
      }
      throw new Error('connection reset');
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await waitFor(() => vi.mocked(observer.onFailure).mock.calls.length >= 1, 8_000);
    hanging = true;
    const started = Date.now();
    await bus.stop();
    expect(Date.now() - started).toBeLessThan(2_000);
  }, 15_000);
});

describe('settlement recovery', () => {
  it('keeps retrying a reschedule that failed until it lands', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 3 },
      dispatcher: { pollInterval: 5, completionBatchWindow: 0 },
    });
    const original = store.reschedule.bind(store);
    let failures = 3;
    vi.spyOn(store, 'reschedule').mockImplementation(async (...args) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('connection reset');
      }
      return original(...args);
    });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new Error('handler failed');
      },
      { id: 'h' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    await waitFor(
      () => vi.mocked(observer.onDeliveryRetryScheduled).mock.calls.length === 1,
      3_000,
    );
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'pending', attempts: 1 });
  });

  it('keeps retrying a bury that failed until it lands', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      retry: { attempts: 1 },
      dispatcher: { pollInterval: 5, completionBatchWindow: 0 },
    });
    const original = store.bury.bind(store);
    let failures = 3;
    vi.spyOn(store, 'bury').mockImplementation(async (...args) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('connection reset');
      }
      return original(...args);
    });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new Error('handler failed');
      },
      { id: 'h' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    await waitFor(() => vi.mocked(observer.onDeliveryDead).mock.calls.length === 1, 3_000);
    expect(await bus.listDeliveries({ status: 'dead' })).toHaveLength(1);
  });

  it('retries a release that failed at shutdown before it stops', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, completionBatchWindow: 0, shutdownTimeout: 20 },
    });
    const original = store.release.bind(store);
    let failures = 1;
    vi.spyOn(store, 'release').mockImplementation(async (...args) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('connection reset');
      }
      return original(...args);
    });
    bus.subscribe(OrderPlaced, () => new Promise<void>(() => undefined), { id: 'h' });
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await waitFor(() => vi.mocked(observer.onDeliveryStarted).mock.calls.length === 1, 3_000);
    await bus.stop();
    expect(vi.mocked(observer.onDeliveryReleased)).toHaveBeenCalledTimes(1);
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'pending', attempts: 0 });
  });

  it('does not let an old recovery attempt forget the pending completion of a newer claim', async () => {
    const { bus, store, clock } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, completionBatchWindow: 0, leaseDuration: '30s' },
    });
    const original = store.complete.bind(store);
    const slow = deferred<boolean>();
    let calls = 0;
    let healthy = false;
    vi.spyOn(store, 'complete').mockImplementation(async (lease, now, transaction) => {
      calls += 1;
      if (healthy) {
        return original(lease, now, transaction);
      }
      if (calls === 5) {
        return slow.promise; // the first recovery attempt, still in flight
      }
      throw new Error('connection reset');
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce(); // initial attempt and its retries fail, the completion is retained
    await waitFor(() => calls >= 5, 3_000);
    // The lease expires and the delivery is claimed again; that attempt's completion fails too.
    clock.advance(31_000);
    await bus.runOnce();
    // The old recovery attempt now answers; it must not drop the newer pending completion.
    slow.resolve(true);
    await new Promise((resolve) => setTimeout(resolve, 30));
    healthy = true;
    let completed = 0;
    for (let attempt = 0; attempt < 200 && completed === 0; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      completed = (await bus.statistics()).completed;
    }
    expect(await bus.statistics()).toMatchObject({ completed: 1, processing: 0 });
  }, 15_000);
});

describe('a delivery whose lease ran out locally', () => {
  function outlivingBus(attempts: number) {
    const test = createTestBus({
      transactionMode: 'optional',
      clock: systemClock,
      retry: { attempts, initialDelay: 1, factor: 1, jitter: 'none' },
      dispatcher: { leaseDuration: 20, pollInterval: 5 },
    });
    test.bus.subscribe(OrderPlaced, () => new Promise((resolve) => setTimeout(resolve, 60)), {
      id: 'h',
      transactional: true,
    });
    return test;
  }

  it('retries the reschedule until the store answers', async () => {
    const { bus, store } = outlivingBus(3);
    const original = store.reschedule.bind(store);
    let failures = 2;
    vi.spyOn(store, 'reschedule').mockImplementation(async (...args) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('connection reset');
      }
      return original(...args);
    });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    for (let attempt = 0; attempt < 200; attempt += 1) {
      if ((await bus.listDeliveries())[0]!.status === 'pending') {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect((await bus.listDeliveries())[0]).toMatchObject({ status: 'pending', attempts: 1 });
  });

  it('is dead-lettered once its attempts are used up, not retried for ever', async () => {
    const { bus, observer } = outlivingBus(2);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    for (let round = 0; round < 6 && (await bus.statistics()).dead === 0; round += 1) {
      await bus.runOnce();
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    expect(await bus.statistics()).toMatchObject({ dead: 1, pending: 0 });
    expect(vi.mocked(observer.onDeliveryDead)).toHaveBeenCalledTimes(1);
  });
});

describe('settlement of an older claim', () => {
  it('does not overwrite the pending settlement of a newer claim of the same delivery', async () => {
    const { bus, store, clock } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, completionBatchWindow: 0, leaseDuration: '30s' },
    });
    const original = store.complete.bind(store);
    const gate = deferred<void>();
    let firstToken: string | undefined;
    let held = false;
    let healthy = false;
    vi.spyOn(store, 'complete').mockImplementation(async (lease, now, transaction) => {
      firstToken ??= lease.leaseToken;
      if (lease.leaseToken === firstToken && !held) {
        held = true;
        await gate.promise; // the first attempt of the first claim hangs while its lease runs out
      }
      if (healthy) {
        return original(lease, now, transaction);
      }
      throw new Error('connection reset');
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    const first = bus.runOnce();
    await waitFor(() => held, 3_000);
    clock.advance(31_000);
    // The delivery is claimed again; its completion fails and is retained before the first claim gives up.
    await bus.runOnce();
    gate.resolve();
    await first;
    healthy = true;
    for (let attempt = 0; attempt < 300 && (await bus.statistics()).completed === 0; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(await bus.statistics()).toMatchObject({ completed: 1, processing: 0 });
  }, 15_000);
});
