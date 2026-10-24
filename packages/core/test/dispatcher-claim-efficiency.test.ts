import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  InMemoryEventStore,
  type ClaimRequest,
  type Duration,
  type ClaimedDelivery,
  type LeaseReference,
} from '@relaybus/core';
import { expectWithinBudget } from '../../../test-support/perf-budget.js';
import { OrderPlaced, createTestBus } from './helpers.js';

/** Simulated round trip of every store call: a real store answers later, not in the same tick. */
const ROUND_TRIP_MS = 1;

/**
 * Design tests of the dispatcher's claim scheduling, without a database: a store that counts what
 * the dispatcher asks of it and answers after a simulated round trip. Timers are fake, so the
 * counts are the same on every machine.
 */
class CountingStore extends InMemoryEventStore {
  constructor() {
    let sequence = 0;
    super({ generateIdentifier: () => `lease-${String((sequence += 1)).padStart(8, '0')}` });
  }

  readonly claims: Array<{ readonly limit: number; readonly claimed: number }> = [];
  completeManyCalls = 0;

  override async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    await roundTrip();
    const claimed = await super.claim(request);
    this.claims.push({ limit: request.limit, claimed: claimed.length });
    return claimed;
  }

  override async completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    this.completeManyCalls += 1;
    await roundTrip();
    return super.completeMany(leases, now);
  }
}

function roundTrip(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ROUND_TRIP_MS));
}

/** Handler run time between 1 and 5 ms, varying by delivery, so completions do not all arrive at once. */
function work(orderId: string): Promise<void> {
  const milliseconds = (Number.parseInt(orderId.replace(/\D/g, ''), 10) % 5) + 1;
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function advanceUntil(condition: () => boolean, limitMs = 60_000): Promise<void> {
  for (let elapsed = 0; !condition(); elapsed += 1) {
    if (elapsed > limitMs) {
      throw new Error('advanceUntil: condition not met');
    }
    await vi.advanceTimersByTimeAsync(1);
  }
}

function createCountingBus(options: { pollInterval?: Duration } = {}) {
  const store = new CountingStore();
  let sequence = 0;
  const test = createTestBus({
    store,
    transactionMode: 'optional',
    retention: { completed: false },
    // Ids in creation order: claims then take deliveries in publish order, which makes every count repeatable.
    generateIdentifier: () => `id-${String((sequence += 1)).padStart(8, '0')}`,
    ...(options.pollInterval === undefined
      ? {}
      : { dispatcher: { pollInterval: options.pollInterval } }),
  });
  return { ...test, store };
}

const BATCH_SIZE = 10;

/** Stops the bus while fake timers keep moving, so its waits end. */
async function stopBus(bus: { stop(): Promise<void> }): Promise<void> {
  let stopped = false;
  const stopping = bus.stop().then(() => {
    stopped = true;
  });
  await advanceUntil(() => stopped);
  await stopping;
}

describe('dispatcher claim efficiency (default settings: concurrency 10, batch 10)', () => {
  beforeEach(() => {
    vi.useFakeTimers({
      toFake: [
        'setTimeout',
        'clearTimeout',
        'setInterval',
        'clearInterval',
        'setImmediate',
        'Date',
      ],
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('drains a backlog with full claims instead of one claim per freed slot', async () => {
    const { bus, store } = createCountingBus();
    const total = 1_000;
    let handled = 0;
    bus.subscribe(
      OrderPlaced,
      (event) => work(event.payload.orderId).then(() => void (handled += 1)),
      { id: 'h' },
    );
    await bus.publishAll(
      Array.from({ length: total }, (_, index) => new OrderPlaced({ orderId: String(index) })),
    );
    await bus.start();
    await advanceUntil(() => handled === total);
    await stopBus(bus);
    const productive = store.claims.filter((claim) => claim.claimed > 0);
    expectWithinBudget(
      'core.dispatcher.backlog.claimsPer100Deliveries',
      (store.claims.length / total) * 100,
    );
    // Smaller is better: 0 means every claim was as large as the batch size.
    expectWithinBudget(
      'core.dispatcher.backlog.missingClaimSizePercent',
      Math.round(
        100 -
          (productive.reduce((sum, claim) => sum + claim.claimed, 0) /
            productive.length /
            BATCH_SIZE) *
            100,
      ),
    );
    expectWithinBudget(
      'core.dispatcher.backlog.completeManyCallsPer100Deliveries',
      (store.completeManyCalls / total) * 100,
    );
  });

  it('claims the successors of hot keys in groups, not one per completion', async () => {
    const { bus, store } = createCountingBus();
    const keys = 10;
    const perKey = 50;
    let handled = 0;
    bus.subscribe(
      OrderPlaced,
      (event) => work(event.payload.orderId).then(() => void (handled += 1)),
      { id: 'h' },
    );
    for (let round = 0; round < perKey; round += 1) {
      for (let key = 0; key < keys; key += 1) {
        await bus.publish(new OrderPlaced({ orderId: `${key}-${round}` }), { key: `k${key}` });
      }
    }
    await bus.start();
    await advanceUntil(() => handled === keys * perKey);
    await stopBus(bus);
    expectWithinBudget(
      'core.dispatcher.hotKeys.claimsPer100Deliveries',
      (store.claims.length / (keys * perKey)) * 100,
    );
  });

  it('claims once for a delivery published to an idle dispatcher', async () => {
    const { bus, store } = createCountingBus();
    let handled = 0;
    bus.subscribe(
      OrderPlaced,
      (event) => work(event.payload.orderId).then(() => void (handled += 1)),
      { id: 'h' },
    );
    await bus.start();
    // Let the start-up claim finish and the loop go to sleep.
    await vi.advanceTimersByTimeAsync(50);
    const before = store.claims.length;
    const deliveries = 5;
    for (let index = 0; index < deliveries; index += 1) {
      await bus.publish(new OrderPlaced({ orderId: String(index) }));
      await advanceUntil(() => handled === index + 1);
      // The settlement may still wake the loop: give it the time to do so before counting.
      await vi.advanceTimersByTimeAsync(100);
    }
    await stopBus(bus);
    expectWithinBudget(
      'core.dispatcher.trickle.claimsPerDelivery',
      (store.claims.length - before) / deliveries,
    );
  });

  it('claims once per poll interval while idle', async () => {
    const { bus, store } = createCountingBus({ pollInterval: '1s' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.start();
    await vi.advanceTimersByTimeAsync(10_000);
    const claims = store.claims.length;
    await stopBus(bus);
    expect(store.claims.every((claim) => claim.claimed === 0)).toBe(true);
    expectWithinBudget('core.dispatcher.idle.claimsPer10PollIntervals', claims);
  });
});
