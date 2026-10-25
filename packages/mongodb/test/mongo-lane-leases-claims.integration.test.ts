import { Collection } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { type ClaimedDelivery, type EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { laneOf } from '../src/lanes.js';
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
  sortById,
} from './store-support.js';

useReplicaSet();

describe('claiming does not write delivery documents', () => {
  it('leaves delivery documents untouched through claim and renewal, writes lane documents, and deletes on completion', async () => {
    const { store, deliveries, lanes } = await createStore();
    await append(
      store,
      Array.from({ length: 20 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const deliveriesBefore = sortById(await deliveries.find().toArray());
    const lanesBefore = await lanes.find().sort({ _id: 1 }).toArray();

    const claimed = await claim(store, ['h'], 'worker-1', at(0), 20);
    expect(claimed).toHaveLength(20);
    expect(await store.extendLease(claimed[0]!, at(LEASE_MS * 2), at(1))).toBe(true);

    expect(sortById(await deliveries.find().toArray())).toEqual(deliveriesBefore);
    const lanesAfterClaim = await lanes.find().sort({ _id: 1 }).toArray();
    expect(lanesAfterClaim).not.toEqual(lanesBefore);
    const owned = lanesAfterClaim.filter((lane) => lane['owner'] === 'worker-1');
    expect(owned.flatMap((lane) => lane['inFlight'] as string[]).sort()).toEqual(
      claimed.map((delivery) => delivery.deliveryId).sort(),
    );
    expect(lanesAfterClaim.filter((lane) => lane['owner'] !== null)).toEqual(owned);

    // A completion deletes the delivery documents and never writes a lane document.
    expect(await store.completeMany(claimed, at(2))).toEqual(claimed.map(() => true));
    expect(await deliveries.countDocuments()).toBe(0);
    expect(await lanes.find().sort({ _id: 1 }).toArray()).toEqual(lanesAfterClaim);
  });

  it('completes a batch of leases from many lanes with four commands and no transaction', async () => {
    const { store } = await createStore();
    await append(
      store,
      Array.from({ length: 60 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const claimed = await claim(store, ['h'], 'worker-1', at(0), 60);
    expect(
      new Set(claimed.map((delivery) => delivery.leaseToken.split(':')[0])).size,
    ).toBeGreaterThan(10);
    let outcome: boolean[] = [];
    const commands = await recordCommands(async () => {
      outcome = await store.completeMany(claimed, at(1));
    });
    expect(outcome).toEqual(claimed.map(() => true));
    // Read the deliveries, read the lanes, record the events' completion, delete: whatever the batch size.
    expect(commands.map((command) => command.commandName)).toEqual([
      'find',
      'find',
      'update',
      'delete',
    ]);
  });

  it('completes a batch with its history records in one transaction with a fixed number of commands', async () => {
    const { store } = await createStore({ completionHistory: true });
    await append(
      store,
      Array.from({ length: 60 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const claimed = await claim(store, ['h'], 'worker-1', at(0), 60);
    expect(
      new Set(claimed.map((delivery) => delivery.leaseToken.split(':')[0])).size,
    ).toBeGreaterThan(10);
    let outcome: boolean[] = [];
    const commands = await recordCommands(async () => {
      outcome = await store.completeMany(claimed, at(1));
    });
    expect(outcome).toEqual(claimed.map(() => true));
    const names = commands.map((command) => command.commandName);
    expect(names.filter((name) => name === 'commitTransaction')).toHaveLength(1);
    expect(names.filter((name) => name === 'delete')).toHaveLength(1);
    expect(names.filter((name) => name === 'insert')).toHaveLength(1);
    // The one update records the events' completion, outside the transaction.
    expect(names.filter((name) => name === 'update')).toHaveLength(1);
    expect(names.length).toBeLessThanOrEqual(7);
  });
});

describe('lane leases', () => {
  async function sameLaneTwoHandlers(): Promise<TestStore> {
    const created = await createStore();
    await append(created.store, [
      { event: createEnvelope({ key: 'shared' }), handlers: ['h1', 'h2'] },
    ]);
    return created;
  }

  it('keeps the deliveries of a lane away from other owners while the lease is live', async () => {
    const { store } = await sameLaneTwoHandlers();
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    expect(await claim(store, ['h2'], 'worker-2', at(LEASE_MS - 1))).toEqual([]);
    expect(await claim(store, ['h2'], 'worker-1', at(1))).toHaveLength(1);
  });

  it('gives the lane to another owner once the lease expired and fences the old epoch', async () => {
    const { store, lanes } = await sameLaneTwoHandlers();
    const [first] = await claim(store, ['h1'], 'worker-1');
    const [second] = await claim(store, ['h2'], 'worker-2', at(LEASE_MS));
    expect(second).toMatchObject({ handler: 'h2', attempt: 1 });
    const [firstLane, firstEpoch] = first!.leaseToken.split(':');
    const [secondLane, secondEpoch] = second!.leaseToken.split(':');
    expect(secondLane).toBe(firstLane);
    expect(Number(secondLane)).toBe(laneOf('shared', 'unused'));
    expect(Number(secondEpoch)).toBe(Number(firstEpoch) + 1);
    expect(await store.complete(first!, at(LEASE_MS + 1))).toBe(false);
    expect(await store.extendLease(first!, at(LEASE_MS * 3), at(LEASE_MS + 1))).toBe(false);
    expect(await lanes.findOne({ _id: Number(firstLane) })).toMatchObject({
      owner: 'worker-2',
      epoch: Number(secondEpoch),
      inFlight: [second!.deliveryId],
      leaseExpiresAt: at(LEASE_MS * 2),
    });
  });

  it('charges a lost attempt only to deliveries that were still running', async () => {
    const { store, deliveries } = await createStore();
    await append(store, [{ event: createEnvelope({ key: 'k' }), handlers: ['a', 'b', 'c'] }]);
    const [running, retrying, finished] = (await claim(store, ['a', 'b', 'c'], 'worker-1')).sort(
      (left, right) => left.handler.localeCompare(right.handler),
    );
    await store.reschedule(retrying!, at(1_000), error, at(1));
    await store.complete(finished!, at(2));
    const before = await deliveries.findOne({ _id: running!.deliveryId });
    expect(before).toMatchObject({ attempts: 0 });
    const taken = await claim(store, ['a', 'b', 'c'], 'worker-2', at(LEASE_MS + 1_000));
    expect(
      Object.fromEntries(taken.map((delivery) => [delivery.handler, delivery.attempt])),
    ).toEqual({ a: 2, b: 2 });
    expect(await deliveries.findOne({ _id: running!.deliveryId })).toMatchObject({
      attempts: 1,
    });
    expect(await deliveries.findOne({ _id: retrying!.deliveryId })).toMatchObject({
      attempts: 1,
    });
  });

  it('lets a completion that committed first win: the takeover waits for it and finds the delivery gone', async () => {
    const { store, database } = await sameLaneTwoHandlers();
    const [stale] = await claim(store, ['h1'], 'worker-1');
    const peer = new MongoEventStore({ client, database });
    let takeover: Promise<ClaimedDelivery[]> | undefined;
    await store.transaction(async (session) => {
      expect(await store.complete(stale!, at(LEASE_MS + 1), session)).toBe(true);
      takeover ??= claim(peer, ['h1', 'h2'], 'worker-2', at(LEASE_MS));
      await new Promise((resolve) => setTimeout(resolve, 200));
    });
    const taken = await takeover!;
    expect(taken.map((delivery) => [delivery.handler, delivery.attempt])).toEqual([['h2', 1]]);
    expect(await store.listDeliveries({})).toEqual([
      expect.objectContaining({ handler: 'h2', status: 'processing', leaseOwner: 'worker-2' }),
    ]);
  });

  it('makes a completion that started before a takeover conflict, retry and report the lease as lost', async () => {
    const { store, database, deliveries } = await sameLaneTwoHandlers();
    const [stale] = await claim(store, ['h1'], 'worker-1');
    const peer = new MongoEventStore({ client, database });
    const outcomes: boolean[] = [];
    let taken: ClaimedDelivery[] = [];
    let runs = 0;
    await store.transaction(async (session) => {
      runs += 1;
      // Opens the transaction's snapshot before the takeover commits.
      await deliveries.findOne({}, { session });
      if (runs === 1) {
        taken = await claim(peer, ['h1', 'h2'], 'worker-2', at(LEASE_MS));
      }
      outcomes.push(await store.complete(stale!, at(LEASE_MS + 1), session));
    });
    // The first run saw the old epoch, but its delete hit the delivery the takeover had charged.
    expect(runs).toBe(2);
    expect(outcomes).toEqual([false]);
    expect(taken.map((delivery) => [delivery.handler, delivery.attempt]).sort()).toEqual([
      ['h1', 2],
      ['h2', 1],
    ]);
    expect(await deliveries.findOne({ _id: stale!.deliveryId })).toMatchObject({
      status: 'pending',
      attempts: 1,
    });
    const retaken = taken.find((delivery) => delivery.handler === 'h1')!;
    expect(await peer.complete(retaken, at(LEASE_MS + 2))).toBe(true);
  });

  it('lets a takeover proceed and redelivers when the completing transaction rolls back', async () => {
    const { store, database } = await sameLaneTwoHandlers();
    const [stale] = await claim(store, ['h1'], 'worker-1');
    const peer = new MongoEventStore({ client, database });
    await expect(
      store.transaction(async (session) => {
        await store.complete(stale!, at(LEASE_MS + 1), session);
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    const taken = await claim(peer, ['h1', 'h2'], 'worker-2', at(LEASE_MS + 2));
    expect(taken.map((delivery) => [delivery.handler, delivery.attempt]).sort()).toEqual([
      ['h1', 2],
      ['h2', 1],
    ]);
  });

  it('hands lanes back when a claim finds nothing to run', async () => {
    const { store, lanes } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1');
    await store.complete(delivery!, at(1));
    expect(await claim(store, ['h'], 'worker-1', at(2))).toEqual([]);
    expect(await lanes.countDocuments({ owner: { $ne: null } })).toBe(0);
    // Freeing keeps the epoch: a later owner still gets a new one.
    const [again] = await (async () => {
      await append(store, [{ event: createEnvelope(), handlers: ['h'] }], at(3));
      return claim(store, ['h'], 'worker-2', at(3));
    })();
    expect(again!.leaseToken).not.toBe(delivery!.leaseToken);
  });

  it('frees a lane whose deliveries all went back to pending', async () => {
    const { store, lanes } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1');
    await store.release(delivery!, at(1));
    expect(await claim(store, ['other'], 'worker-1', at(2))).toEqual([]);
    expect(await lanes.countDocuments({ owner: { $ne: null } })).toBe(0);
    expect(await claim(store, ['h'], 'worker-2', at(3))).toHaveLength(1);
  });

  it('shows running deliveries as processing with the lease of their lane', async () => {
    const { store } = await createStore();
    await append(store, [
      { event: createEnvelope(), handlers: ['running'] },
      { event: createEnvelope(), handlers: ['waiting'] },
    ]);
    await claim(store, ['running'], 'worker-1', at(5));
    const processing = await store.listDeliveries({ status: 'processing' });
    expect(processing).toHaveLength(1);
    expect(processing[0]).toMatchObject({
      handler: 'running',
      attempts: 1,
      leaseOwner: 'worker-1',
    });
    expect(processing[0]!.leaseExpiresAt).toEqual(at(5 + LEASE_MS));
    const pending = await store.listDeliveries({ status: 'pending' });
    expect(pending.map((record) => [record.handler, record.attempts, record.leaseOwner])).toEqual([
      ['waiting', 0, null],
    ]);
    expect(await store.listDeliveries({ status: 'constructor' as never })).toEqual([]);
    expect(await store.listDeliveries({ status: 'unknown' as never })).toEqual([]);
  });

  it('drops the picks of a lane that changed hands before the claim was recorded', async () => {
    const { store, lanes } = await createStore();
    const [kept, lost] = eventsInLanes([3, 4]) as [EventEnvelope, EventEnvelope];
    await append(store, [
      { event: kept, handlers: ['h'] },
      { event: lost, handlers: ['h'] },
    ]);
    const original = Collection.prototype.bulkWrite;
    let intercepted = false;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      if (!intercepted && this.collectionName.endsWith('_lanes')) {
        intercepted = true;
        // What a takeover between the claim's reads and its record looks like.
        await lanes.updateOne({ _id: 4 }, { $inc: { epoch: 1 } });
      }
      return original.apply(this, args);
    });
    const claimed = await claim(store, ['h'], 'worker-1');
    vi.restoreAllMocks();
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([kept.id]);
    expect(await store.statistics(at(1))).toMatchObject({ processing: 1, pending: 1 });
    // The lane still belongs to this owner under the newer epoch, which the next claim uses.
    const [retried] = await claim(store, ['h'], 'worker-1', at(1));
    expect(retried).toMatchObject({ attempt: 1 });
    expect(retried!.event.id).toBe(lost.id);
  });

  it('skips a lane another claimer took first and does not take it later in the same claim', async () => {
    const { store } = await createStore();
    const [first, second] = eventsInLanes([10, 11]) as [EventEnvelope, EventEnvelope];
    await store.transaction((session) =>
      store.append(
        session,
        [
          { event: first, deliveries: [createDelivery('h', at(0))] },
          { event: second, deliveries: [createDelivery('h', at(1))] },
        ],
        at(0),
      ),
    );
    vi.spyOn(Collection.prototype, 'findOneAndUpdate').mockImplementationOnce(
      (async () => null) as never,
    );
    const claimed = await claim(store, ['h'], 'worker-1', at(5));
    vi.restoreAllMocks();
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([second.id]);
    expect((await claim(store, ['h'], 'worker-1', at(6))).map((d) => d.event.id)).toEqual([
      first.id,
    ]);
  });
});
