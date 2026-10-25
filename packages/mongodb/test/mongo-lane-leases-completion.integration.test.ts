import { Collection, type Document } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { type ClaimedDelivery, type EventEnvelope, type LeaseReference } from '@relaybus/core';
import { at, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { LANE_COUNT } from '../src/lanes.js';
import {
  LEASE_MS,
  error,
  type TestStore,
  client,
  useReplicaSet,
  createStore,
  append,
  claim,
  eventsInLanes,
  recordCommands,
} from './store-support.js';

useReplicaSet();

describe('completion without a transaction', () => {
  /** Two deliveries in different lanes, claimed by worker-1 at different times so only the first lease expires at LEASE_MS. */
  async function twoLeases(): Promise<
    TestStore & { early: ClaimedDelivery; late: ClaimedDelivery }
  > {
    const created = await createStore();
    const [first, second] = eventsInLanes([20, 21]) as [EventEnvelope, EventEnvelope];
    await append(created.store, [{ event: first, handlers: ['h'] }]);
    const [early] = await claim(created.store, ['h'], 'worker-1', at(0));
    await append(created.store, [{ event: second, handlers: ['h'] }], at(1_000));
    const [late] = await claim(created.store, ['h'], 'worker-1', at(1_000));
    expect([early!.event.id, late!.event.id]).toEqual([first.id, second.id]);
    return { ...created, early: early!, late: late! };
  }

  /** Runs `takeover` right before the first `method` call on the `suffix` collection. */
  function before(
    method: 'find' | 'deleteMany',
    suffix: string,
    takeover: () => Promise<unknown>,
  ): void {
    const original = Collection.prototype[method] as (...args: unknown[]) => unknown;
    let done = false;
    vi.spyOn(Collection.prototype, method).mockImplementation(function (
      this: Collection,
      ...args: unknown[]
    ) {
      if (done || !this.collectionName.endsWith(suffix)) {
        return original.apply(this, args);
      }
      done = true;
      if (method === 'find') {
        // find returns a cursor synchronously: run the takeover before the cursor is read.
        const cursor = original.apply(this, args) as { toArray(): Promise<unknown[]> };
        const toArray = cursor.toArray.bind(cursor);
        cursor.toArray = async () => {
          await takeover();
          return toArray();
        };
        return cursor;
      }
      return takeover().then(() => original.apply(this, args));
    } as never);
  }

  it('reports a lease lost when the lane is taken over between reading the deliveries and the lanes', async () => {
    const { store, database, early, late } = await twoLeases();
    const peer = new MongoEventStore({ client, database });
    let taken: ClaimedDelivery[] = [];
    before('find', '_lanes', async () => {
      taken = await claim(peer, ['h'], 'worker-2', at(LEASE_MS));
    });
    expect(await store.completeMany([early, late], at(LEASE_MS + 1))).toEqual([false, true]);
    vi.restoreAllMocks();
    expect(taken.map((delivery) => [delivery.deliveryId, delivery.attempt])).toEqual([
      [early.deliveryId, 2],
    ]);
    expect(await peer.complete(taken[0]!, at(LEASE_MS + 2))).toBe(true);
  });

  it('deletes nothing of a lane taken over after it was read, and reports no success it cannot prove', async () => {
    const { store, database, deliveries, early, late } = await twoLeases();
    const peer = new MongoEventStore({ client, database });
    let taken: ClaimedDelivery[] = [];
    before('deleteMany', '_deliveries', async () => {
      taken = await claim(peer, ['h'], 'worker-2', at(LEASE_MS));
    });
    // The other lease's delivery is deleted, but one delete of the batch matched nothing, and
    // which one cannot be told apart from a delivery the new claim completed: none is reported.
    expect(await store.completeMany([early, late], at(LEASE_MS + 1))).toEqual([false, false]);
    vi.restoreAllMocks();
    expect(taken.map((delivery) => [delivery.deliveryId, delivery.attempt])).toEqual([
      [early.deliveryId, 2],
    ]);
    expect(await deliveries.findOne({ _id: early.deliveryId })).toMatchObject({ attempts: 1 });
    expect(await deliveries.findOne({ _id: late.deliveryId })).toBeNull();
    expect(await peer.complete(taken[0]!, at(LEASE_MS + 2))).toBe(true);
  });
});

describe('unissued lease tokens', () => {
  it.each(['not-a-token', '999:1', '256:0', '3:x', '3:', '-1:2', '7:1234567890123456', ''])(
    'are refused by every operation without a database call: %j',
    async (leaseToken) => {
      const { store } = await createStore();
      await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
      const [real] = await claim(store, ['h'], 'worker-1');
      const lease: LeaseReference = { deliveryId: real!.deliveryId, leaseToken };
      const outcomes: boolean[] = [];
      const commands = await recordCommands(async () => {
        outcomes.push(
          await store.extendLease(lease, at(LEASE_MS * 2), at(1)),
          await store.complete(lease, at(1)),
          await store.reschedule(lease, at(5), error, at(1)),
          await store.bury(lease, error, at(1)),
          await store.release(lease, at(1)),
          ...(await store.completeMany([lease, lease], at(1))),
        );
      });
      expect(outcomes).toEqual([false, false, false, false, false, false, false]);
      expect(commands).toEqual([]);
      expect(await store.completeMany([lease, real!], at(1))).toEqual([false, true]);
    },
  );

  it('refuses a well-formed token of another epoch, another lane or another claim', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [real] = await claim(store, ['h'], 'worker-1');
    const [lane, epoch, claims] = real!.leaseToken.split(':').map(Number) as [
      number,
      number,
      number,
    ];
    const forged = [
      { deliveryId: real!.deliveryId, leaseToken: `${lane}:${epoch + 1}:${claims}` },
      { deliveryId: real!.deliveryId, leaseToken: `${(lane + 1) % LANE_COUNT}:${epoch}:${claims}` },
      { deliveryId: real!.deliveryId, leaseToken: `${lane}:${epoch}:${claims + 1}` },
    ];
    for (const lease of forged) {
      expect(await store.extendLease(lease, at(LEASE_MS * 2), at(1))).toBe(false);
      expect(await store.reschedule(lease, at(5), error, at(1))).toBe(false);
      expect(await store.bury(lease, error, at(1))).toBe(false);
      expect(await store.release(lease, at(1))).toBe(false);
    }
    expect(await store.completeMany([...forged, real!], at(1))).toEqual([
      false,
      false,
      false,
      true,
    ]);
  });
});

describe('claiming behind a clogged lane', () => {
  it('finds unrelated deliveries behind a long backlog of one blocked key and parks the backlog', async () => {
    const { store, deliveries } = await createStore();
    await append(
      store,
      Array.from({ length: 300 }, () => ({
        event: createEnvelope({ key: 'hot' }),
        handlers: ['h'],
      })),
    );
    await append(
      store,
      [
        ...Array.from({ length: 5 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
        ...Array.from({ length: 5 }, (_, index) => ({
          event: createEnvelope({ key: `other-${index}` }),
          handlers: ['h'],
        })),
      ],
      at(10),
    );
    const claimed = await claim(store, ['h'], 'worker-1', at(100), 20);
    expect(claimed).toHaveLength(11);
    expect(claimed.filter((delivery) => delivery.event.key === 'hot')).toHaveLength(1);
    expect(claimed[0]!.event.key).toBe('hot');
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(299);
    // The hot lane belongs to worker-1, so nobody else sees its backlog meanwhile.
    expect(await claim(store, ['h'], 'worker-2', at(101), 20)).toEqual([]);
    const head = claimed[0]!;
    expect(await store.complete(head, at(102))).toBe(true);
    const [next] = await claim(store, ['h'], 'worker-1', at(103), 20);
    expect(next!.event.key).toBe('hot');
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(298);
  });
});

describe('requeued deliveries behind a running successor', () => {
  it('answers every later requeued delivery of the key from what the claim already learned', async () => {
    const { store } = await createStore();
    const events = [1, 2, 3].map(() => createEnvelope({ key: 'k' }));
    for (const [index, event] of events.entries()) {
      // Ids in sequence order, so the claim meets the requeued deliveries lowest first.
      await store.transaction((session) =>
        store.append(
          session,
          [{ event, deliveries: [{ id: `delivery-${index}`, handler: 'h', availableAt: at(0) }] }],
          at(0),
        ),
      );
    }
    const [first] = await claim(store, ['h'], 'worker-1', at(0));
    await store.bury(first!, error, at(1));
    const [second] = await claim(store, ['h'], 'worker-1', at(2));
    await store.bury(second!, error, at(3));
    const [third] = await claim(store, ['h'], 'worker-1', at(4));
    expect(third!.deliveryId).toBe('delivery-2');
    expect(await store.requeue(['delivery-0', 'delivery-1'], at(5))).toBe(2);
    let claimed: ClaimedDelivery[] = [];
    const commands = await recordCommands(async () => {
      claimed = await claim(store, ['h'], 'worker-1', at(6));
    });
    expect(claimed).toEqual([]);
    // One predecessor lookup for the first requeued delivery; the second is answered without one.
    const lookups = commands.filter(
      (command) =>
        command.commandName === 'find' &&
        (command.command['filter'] as Document | undefined)?.['sequence'] !== undefined,
    );
    expect(lookups).toHaveLength(1);
    await store.complete(third!, at(7));
    expect((await claim(store, ['h'], 'worker-1', at(8))).map((d) => d.deliveryId)).toEqual([
      'delivery-0',
    ]);
  });
});
