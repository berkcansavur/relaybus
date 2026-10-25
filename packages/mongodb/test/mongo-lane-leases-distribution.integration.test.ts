import { describe, expect, it } from 'vitest';
import { at, createEnvelope } from '@relaybus/testing';
import { LANE_COUNT } from '../src/lanes.js';
import {
  error,
  useReplicaSet,
  createStore,
  append,
  claim,
  eventsInLanes,
} from './store-support.js';

useReplicaSet();

describe('spreading work over lanes and workers', () => {
  it('holds only the lanes a small batch needs', async () => {
    const { store, lanes } = await createStore();
    await append(
      store,
      Array.from({ length: 600 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const first = await claim(store, ['h'], 'worker-1', at(0), 10);
    expect(first).toHaveLength(10);
    expect(await lanes.countDocuments({ owner: 'worker-1' })).toBeLessThanOrEqual(12);
    const second = await claim(store, ['h'], 'worker-2', at(0), 300);
    expect(second).toHaveLength(300);
    const ids = [...first, ...second].map((delivery) => delivery.deliveryId);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('drains a backlog with several workers, each delivery exactly once', async () => {
    const { store } = await createStore();
    await append(
      store,
      Array.from({ length: 600 }, (_, index) => ({
        event: createEnvelope({ key: index % 4 === 0 ? `key-${index % 40}` : null }),
        handlers: ['h'],
      })),
    );
    const seen: string[] = [];
    await Promise.all(
      Array.from({ length: 4 }, async (_, index) => {
        let idleRounds = 0;
        while (idleRounds < 3) {
          const batch = await claim(store, ['h'], `worker-${index}`, at(0), 25);
          idleRounds = batch.length === 0 ? idleRounds + 1 : 0;
          seen.push(...batch.map((delivery) => delivery.deliveryId));
          expect(await store.completeMany(batch, at(0))).toEqual(batch.map(() => true));
          await store.relinquish('worker-' + String(index));
        }
      }),
    );
    expect(new Set(seen).size).toBe(seen.length);
    expect(seen).toHaveLength(600);
    expect(await store.statistics(at(0))).toMatchObject({ pending: 0, processing: 0 });
  });

  it('claims from every lane it holds when it holds more lanes than one query covers', async () => {
    const { store, lanes } = await createStore();
    const allLanes = Array.from({ length: LANE_COUNT }, (_, lane) => lane);
    await append(
      store,
      eventsInLanes(allLanes).map((event) => ({ event, handlers: ['h'] })),
    );
    const first = await claim(store, ['h'], 'worker-1', at(0), 1_000);
    expect(first).toHaveLength(LANE_COUNT);
    expect(await lanes.countDocuments({ owner: 'worker-1' })).toBe(LANE_COUNT);
    await store.completeMany(first, at(1));
    await append(
      store,
      eventsInLanes(allLanes).map((event) => ({ event, handlers: ['h'] })),
      at(1),
    );
    const small = await claim(store, ['h'], 'worker-1', at(2), 10);
    expect(small).toHaveLength(10);
    const rest = await claim(store, ['h'], 'worker-1', at(2), 1_000);
    expect(rest).toHaveLength(LANE_COUNT - 10);
    const ids = [...small, ...rest].map((delivery) => delivery.deliveryId);
    expect(new Set(ids).size).toBe(LANE_COUNT);
  });
});

describe('completion history', () => {
  it('lists, filters, counts and purges completed deliveries', async () => {
    const { store } = await createStore({ completionHistory: true, purgeBatchSize: 2 });
    const kept = createEnvelope({ type: 'kept.v1' });
    await append(store, [
      { event: kept, handlers: ['a', 'b'] },
      ...Array.from({ length: 4 }, () => ({ event: createEnvelope(), handlers: ['a'] })),
    ]);
    const claimed = await claim(store, ['a'], 'worker-1');
    expect(claimed).toHaveLength(5);
    await store.completeMany(claimed, at(7));
    expect(await store.statistics(at(8))).toMatchObject({ completed: 5, pending: 1 });
    const completed = await store.listDeliveries({ status: 'completed', limit: 3 });
    expect(completed).toHaveLength(3);
    expect(completed[0]).toMatchObject({
      status: 'completed',
      attempts: 1,
      leaseOwner: null,
      leaseExpiresAt: null,
      lastError: null,
    });
    expect(completed[0]!.completedAt).toEqual(at(7));
    expect(completed[0]!.updatedAt).toEqual(at(7));
    expect(await store.listDeliveries({ eventType: 'kept.v1' })).toHaveLength(2);
    expect(await store.listDeliveries({ handler: 'a', status: 'pending' })).toEqual([]);
    expect(await store.listDeliveries({ eventId: kept.id, status: 'completed' })).toHaveLength(1);
    expect(await store.listDeliveries({ status: 'dead' })).toEqual([]);
    expect(await store.purge({ completedBefore: at(7) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 0,
    });
    // Four events lose their only record; the shared one keeps its pending delivery of handler b.
    expect(await store.purge({ completedBefore: at(8) })).toEqual({
      deletedDeliveries: 5,
      deletedEvents: 4,
    });
    expect(await store.listDeliveries({})).toHaveLength(1);
  });

  it('keeps an event while a completion record references it', async () => {
    const { store } = await createStore({ completionHistory: true });
    await append(store, [{ event: createEnvelope(), handlers: ['a'] }]);
    const [delivery] = await claim(store, ['a'], 'worker-1');
    await store.complete(delivery!, at(1));
    expect(await store.purge({ deadBefore: at(1_000) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 0,
    });
    expect(await store.listDeliveries({ status: 'completed' })).toHaveLength(1);
  });

  it('records the attempt that completed after earlier failures', async () => {
    const { store } = await createStore({ completionHistory: true });
    await append(store, [{ event: createEnvelope(), handlers: ['a'] }]);
    const [failed] = await claim(store, ['a'], 'worker-1');
    await store.reschedule(failed!, at(1), error, at(1));
    const [retried] = await claim(store, ['a'], 'worker-1', at(1));
    expect(retried!.attempt).toBe(2);
    await store.complete(retried!, at(2));
    expect(await store.listDeliveries({})).toEqual([
      expect.objectContaining({ status: 'completed', attempts: 2 }),
    ]);
  });
});

describe('purge without a completion history', () => {
  it('sweeps the events completed deliveries left behind in batches', async () => {
    const { store, events } = await createStore({ purgeBatchSize: 3 });
    await append(
      store,
      Array.from({ length: 8 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const claimed = await claim(store, ['h'], 'worker-1', at(0), 8);
    await store.completeMany(claimed, at(1));
    expect(await events.countDocuments()).toBe(8);
    expect(await store.purge({ completedBefore: at(2) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 8,
    });
    expect(await events.countDocuments()).toBe(0);
  });
});
