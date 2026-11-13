import { describe, expect, it, vi } from 'vitest';
import { toMilliseconds } from '../src/duration.js';
import { OrderPlaced, createTestBus, deferred, waitFor } from './helpers.js';

describe('duration strings', () => {
  it('rejects malformed duration strings and keeps fractional amounts', () => {
    for (const bad of ['', ' 5s', '5S', '.5s', '1e3ms', '5', 'ms', '1_000ms']) {
      expect(() => toMilliseconds(bad as never, 'x')).toThrow();
    }
    expect(toMilliseconds('0.5s', 'x')).toBe(500);
    expect(() => toMilliseconds(Number.NaN, 'x')).toThrow();
    expect(() => toMilliseconds(-0.0001, 'x')).toThrow();
  });
});

describe('ordering and stop', () => {
  it('never runs a later event of a key before an earlier one that waits for its retry', async () => {
    const { bus, clock } = createTestBus({ transactionMode: 'optional' });
    const seen: string[] = [];
    let firstFailed = false;
    bus.subscribe(
      OrderPlaced,
      (event) => {
        seen.push(event.payload.orderId);
        if (event.payload.orderId === 'a' && !firstFailed) {
          firstFailed = true;
          throw new Error('once');
        }
      },
      { id: 'h', retry: { initialDelay: '10s', jitter: 'none' } },
    );
    await bus.publish(new OrderPlaced({ orderId: 'a' }), { key: 'k' });
    await bus.publish(new OrderPlaced({ orderId: 'b' }), { key: 'k' });
    await bus.drain();
    clock.advance(11_000);
    await bus.drain();
    expect(seen).toEqual(['a', 'a', 'b']);
  });

  it('stop() waits for a claim that is still in flight and then releases what it returned', async () => {
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      dispatcher: { pollInterval: 5, shutdownTimeout: 2_000 },
    });
    const ran = vi.fn();
    bus.subscribe(OrderPlaced, ran, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    const gate = deferred();
    const realClaim = store.claim.bind(store);
    let claimStarted = false;
    vi.spyOn(store, 'claim').mockImplementation(async (request) => {
      claimStarted = true;
      await gate.promise;
      return realClaim(request);
    });
    await bus.start();
    await waitFor(() => claimStarted);
    const stopping = bus.stop();
    gate.resolve();
    const result = await stopping;
    expect(result.graceful).toBe(true);
    const [record] = await bus.listDeliveries();
    expect(['completed', 'pending']).toContain(record!.status);
    expect(ran.mock.calls.length).toBeLessThanOrEqual(1);
  });
});
