import { Collection, MongoError, type Document } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { type ClaimedDelivery } from '@relaybus/core';
import { at, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { laneOf } from '../src/lanes.js';

import {
  LEASE_MS,
  error,
  type TestStore,
  client,
  useReplicaSet,
  append,
  claim,
  intercept,
  createStore as createSharedStore,
} from './store-support.js';

useReplicaSet();

const createStore = (completionHistory = false) => createSharedStore({ completionHistory });

describe('reconciliation after a failed claim keeps every live claim', () => {
  /** Lets the next record write of the lanes reach the database, then reports a failure. */
  function failNextRecord(): Error {
    const failure = new Error('connection reset after the write');
    const original = Collection.prototype.bulkWrite;
    let failed = false;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      const result = await original.apply(this, args);
      if (!failed && this.collectionName.endsWith('_lanes')) {
        failed = true;
        throw failure;
      }
      return result;
    });
    return failure;
  }

  /** One key, so both handlers' deliveries share the lane `worker-1` holds. */
  async function sharedLane(): Promise<TestStore> {
    const created = await createStore();
    await append(created.store, [{ event: createEnvelope({ key: 'k' }), handlers: ['h1', 'h2'] }]);
    return created;
  }

  it('does not forget a newer claim when a stale lease of the delivery is settled', async () => {
    const { store, lanes } = await sharedLane();
    const [stale] = await claim(store, ['h1'], 'worker-1', at(0));
    await store.release(stale!, at(1));
    const [current] = await claim(store, ['h1'], 'worker-1', at(2));
    expect(await store.complete(stale!, at(3))).toBe(false);
    const failure = failNextRecord();
    await expect(claim(store, ['h2'], 'worker-1', at(4))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await lanes.findOne({ _id: laneOf('k', 'unused') })).toMatchObject({
      inFlight: [current!.deliveryId],
    });
    expect(await claim(store, ['h1'], 'worker-1', at(5))).toEqual([]);
    expect(await store.complete(current!, at(6))).toBe(true);
  });

  it('keeps a claim whose completion failed, so the retried completion is still its own', async () => {
    const { store, database } = await sharedLane();
    const [running] = await claim(store, ['h1'], 'worker-1', at(0));
    const lost = new Error('connection lost before the delete');
    intercept('deleteMany', '_deliveries', async () => {
      throw lost;
    });
    await expect(store.completeMany([running!], at(1))).rejects.toBe(lost);
    vi.restoreAllMocks();
    const failure = failNextRecord();
    await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await claim(store, ['h1'], 'worker-1', at(3))).toEqual([]);
    expect(await store.completeMany([running!], at(4))).toEqual([true]);
    expect(await new MongoEventStore({ client, database }).statistics(at(5))).toMatchObject({
      processing: 0,
    });
  });

  it('keeps a transactional completion registered while its transaction is retried', async () => {
    const { store } = await sharedLane();
    const [running] = await claim(store, ['h1'], 'worker-1', at(0));
    const outcomes: boolean[] = [];
    await store.transaction(async (session) => {
      outcomes.push(await store.complete(running!, at(1), session));
      if (outcomes.length === 1) {
        // A failed claim of the same owner reconciles while the completion is not committed.
        const failure = failNextRecord();
        await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toBe(failure);
        vi.restoreAllMocks();
        const retry = new MongoError('write conflict');
        retry.addErrorLabel('TransientTransactionError');
        throw retry;
      }
    });
    expect(outcomes).toEqual([true, true]);
    expect(await store.statistics(at(3))).toMatchObject({ processing: 0, pending: 1 });
  });
});

describe('reconciliation trusts only registrations of the current claim', () => {
  function failNextRecordOf(owner: string): Error {
    const failure = new Error('connection reset after the write');
    const original = Collection.prototype.bulkWrite;
    let failed = false;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      const result = await original.apply(this, args);
      const recordsFor = JSON.stringify(args[0]).includes(`"owner":"${owner}"`);
      if (!failed && this.collectionName.endsWith('_lanes') && recordsFor) {
        failed = true;
        throw failure;
      }
      return result;
    });
    return failure;
  }

  it('does not keep a delivery listed for a registration of an earlier claim', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    // Handed out and never settled: its lease expires.
    await claim(store, ['h'], 'worker-1', at(0));
    const failure = failNextRecordOf('worker-1');
    await expect(claim(store, ['h'], 'worker-1', at(LEASE_MS))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await lanes.findOne({ _id: laneOf(null, event.id) })).toMatchObject({ inFlight: [] });
    const [again] = await claim(store, ['h'], 'worker-1', at(LEASE_MS + 1));
    expect(again).toMatchObject({ attempt: 2 });
  });

  it('does not invalidate the lease of a worker that took the lane over during the reconciliation', async () => {
    const { store, database } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const peer = new MongoEventStore({ client, database });
    let taken: ClaimedDelivery[] = [];
    let tookOver = false;
    const failure = failNextRecordOf('worker-1');
    intercept(
      'find',
      '_lanes',
      async () => {
        if (!tookOver) {
          tookOver = true;
          taken = await claim(peer, ['h'], 'worker-2', at(LEASE_MS));
        }
      },
      // The reconciliation reads the owner's lanes whatever their expiry; the takeover comes after.
      (args) => {
        const filter = args[0] as Document;
        return filter['holder'] !== undefined && filter['leaseExpiresAt'] === undefined;
      },
      true,
    );
    await expect(claim(store, ['h'], 'worker-1', at(0))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(taken).toHaveLength(1);
    expect(await peer.complete(taken[0]!, at(LEASE_MS + 1))).toBe(true);
  });

  it('keeps the newer registration when an older claim of the delivery returns after it', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope({ key: 'k' }), handlers: ['h1', 'h2'] }]);
    let newer: ClaimedDelivery[] = [];
    let overtaken = false;
    // While worker-1's claim is between recording and returning, its lease expires and
    // worker-2 (another owner on the same store) takes the lane over and gets the delivery.
    intercept(
      'find',
      '_deliveries',
      async () => {
        if (!overtaken) {
          overtaken = true;
          newer = await claim(store, ['h1'], 'worker-2', at(LEASE_MS));
        }
      },
      (args) => (args[0] as Document)['$or'] !== undefined,
    );
    const [older] = await claim(store, ['h1'], 'worker-1', at(0));
    vi.restoreAllMocks();
    expect(newer).toHaveLength(1);
    expect(older!.deliveryId).toBe(newer[0]!.deliveryId);
    expect(await store.complete(older!, at(LEASE_MS + 1))).toBe(false);
    const failure = failNextRecordOf('worker-2');
    await expect(claim(store, ['h2'], 'worker-2', at(LEASE_MS + 2))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await store.complete(newer[0]!, at(LEASE_MS + 3))).toBe(true);
  });
});

describe('settling a completion made inside the caller transaction', () => {
  it('is settled by the next claim of the owner once the completion committed', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope({ key: 'k' });
    await append(store, [{ event, handlers: ['h1', 'h2'] }]);
    const [first] = await claim(store, ['h1'], 'worker-1', at(0));
    await store.transaction(async (session) => {
      expect(await store.complete(first!, at(1), session)).toBe(true);
    });
    // The next claim finds the completed delivery gone, settles it and prunes it from the lane.
    const [second] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(await lanes.findOne({ _id: laneOf('k', 'unused') })).toMatchObject({
      inFlight: [second!.deliveryId],
    });
    expect(await store.complete(second!, at(3))).toBe(true);
  });

  it('lets a failed claim prune a completed delivery that is still listed', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope({ key: 'k' });
    await append(store, [{ event, handlers: ['h1', 'h2'] }]);
    const [first] = await claim(store, ['h1'], 'worker-1', at(0));
    await store.transaction(async (session) => {
      expect(await store.complete(first!, at(1), session)).toBe(true);
    });
    const failure = new Error('connection reset after the write');
    const original = Collection.prototype.bulkWrite;
    let failed = false;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      // Fails before the record reaches the database, so the completed delivery stays listed.
      if (!failed && this.collectionName.endsWith('_lanes')) {
        failed = true;
        throw failure;
      }
      return original.apply(this, args);
    });
    await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await lanes.findOne({ _id: laneOf('k', 'unused') })).toMatchObject({ inFlight: [] });
    const [second] = await claim(store, ['h2'], 'worker-1', at(3));
    expect(second).toMatchObject({ handler: 'h2', attempt: 1 });
  });
});

describe('a reconciliation that failed', () => {
  it('is retried before the next claim of the owner, so the orphaned delivery is not stranded', async () => {
    const { store, lanes } = await createStore();
    await append(store, [{ event: createEnvelope({ key: 'k' }), handlers: ['h1', 'h2'] }]);
    const [live] = await claim(store, ['h1'], 'worker-1', at(0));
    const failure = new Error('connection reset after the write');
    const original = Collection.prototype.bulkWrite;
    let recordFailed = false;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      const result = await original.apply(this, args);
      if (!recordFailed && this.collectionName.endsWith('_lanes')) {
        recordFailed = true;
        throw failure;
      }
      return result;
    });
    let reconciliationFailed = false;
    intercept(
      'find',
      '_lanes',
      async () => {
        if (!reconciliationFailed) {
          reconciliationFailed = true;
          throw new Error('connection lost during the reconciliation');
        }
      },
      (args) => {
        const filter = args[0] as Document;
        return filter['holder'] !== undefined && filter['leaseExpiresAt'] === undefined;
      },
    );
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toBe(failure);
    vi.restoreAllMocks();
    // The record of the failed claim is still in the lane: the orphan is listed beside the live delivery.
    expect((await lanes.findOne({ _id: laneOf('k', 'unused') }))!['inFlight']).toHaveLength(2);
    const [orphan] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(orphan).toMatchObject({ handler: 'h2', attempt: 1 });
    expect(await store.complete(live!, at(3))).toBe(true);
    expect(await store.complete(orphan!, at(3))).toBe(true);
  });
});

describe('settling deliveries found gone', () => {
  it('forgets only registrations of the claiming owner and of the lane epoch it read', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope({ key: 'k' }), handlers: ['h1', 'h2'] }]);
    const [first] = await claim(store, ['h1'], 'worker-1', at(0));
    let newer: ClaimedDelivery[] = [];
    let buried = false;
    let reclaimed = false;
    const originalFind = Collection.prototype.find as (...args: unknown[]) => unknown;
    vi.spyOn(Collection.prototype, 'find').mockImplementation(function (
      this: Collection,
      ...args: unknown[]
    ) {
      const cursor = originalFind.apply(this, args) as { toArray(): Promise<unknown[]> };
      const filter = (args[0] ?? {}) as Document;
      const readsHeldLanes =
        this.collectionName.endsWith('_lanes') && filter['leaseExpiresAt'] !== undefined;
      const readsRunning =
        this.collectionName.endsWith('_deliveries') &&
        filter['status'] === 'pending' &&
        filter['laneId'] === undefined;
      const toArray = cursor.toArray.bind(cursor);
      cursor.toArray = async () => {
        const documents = await toArray();
        if (readsHeldLanes && !buried) {
          // worker-1's next claim read its lanes: the delivery is buried before it reads what runs.
          buried = true;
          expect(await store.bury(first!, error, at(1))).toBe(true);
        } else if (readsRunning && buried && !reclaimed) {
          // That claim saw the delivery gone: it is requeued, and worker-2 takes the lane and claims it.
          reclaimed = true;
          expect(await store.requeue([first!.deliveryId], at(LEASE_MS))).toBe(1);
          newer = await claim(store, ['h1'], 'worker-2', at(LEASE_MS));
        }
        return documents;
      };
      return cursor;
    } as never);
    await claim(store, ['h2'], 'worker-1', at(2));
    vi.restoreAllMocks();
    expect(newer.map((delivery) => delivery.deliveryId)).toEqual([first!.deliveryId]);
    // worker-2's next claim fails after recording: its reconciliation must keep its live delivery.
    const failure = new Error('connection reset after the write');
    const original = Collection.prototype.bulkWrite;
    let failed = false;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      const result = await original.apply(this, args);
      if (!failed && this.collectionName.endsWith('_lanes')) {
        failed = true;
        throw failure;
      }
      return result;
    });
    await expect(claim(store, ['h2'], 'worker-2', at(LEASE_MS + 1))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await store.complete(newer[0]!, at(LEASE_MS + 2))).toBe(true);
  });
});

describe('a record write that arrives after the claim failed', () => {
  it('changes nothing once the reconciliation ran, so nothing is stranded', async () => {
    const { store } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const failure = new Error('socket timeout while the write is still running');
    const original = Collection.prototype.bulkWrite;
    let delayed: { collection: Collection; args: Parameters<Collection['bulkWrite']> } | undefined;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      if (delayed === undefined && this.collectionName.endsWith('_lanes')) {
        delayed = { collection: this, args };
        throw failure;
      }
      return original.apply(this, args);
    });
    await expect(claim(store, ['h'], 'worker-1', at(0))).rejects.toBe(failure);
    vi.restoreAllMocks();
    // The server executes the record only now, after the claim failed and reconciled.
    await original.apply(delayed!.collection, delayed!.args);
    const [again] = await claim(store, ['h'], 'worker-1', at(1));
    expect(again).toMatchObject({ attempt: 1 });
    expect(again!.event.id).toBe(event.id);
  });
});

describe('giving back an overtaken pick late', () => {
  it('does not take a newer claim of the delivery out of the lane', async () => {
    const { store } = await createStore();
    const [first, second] = [createEnvelope({ key: 'k' }), createEnvelope({ key: 'k' })];
    await append(store, [{ event: first, handlers: ['h'] }]);
    await append(store, [{ event: second, handlers: ['h'] }]);
    const [head] = await claim(store, ['h'], 'worker-1', at(0));
    await store.bury(head!, error, at(1));
    // A requeue of the head lands between the claim's predecessor check and its record, so the
    // claim gives the successor back; that pull times out but still reaches the server later.
    const originalFindOne = Collection.prototype.findOne as (
      ...args: unknown[]
    ) => Promise<unknown>;
    let requeued = false;
    vi.spyOn(Collection.prototype, 'findOne').mockImplementation(async function (
      this: Collection,
      ...args: unknown[]
    ) {
      const result = await originalFindOne.apply(this, args);
      const filter = (args[0] ?? {}) as { sequence?: { $lt?: number } };
      if (!requeued && filter.sequence?.$lt !== undefined) {
        requeued = true;
        await store.requeue([head!.deliveryId], at(2));
      }
      return result;
    } as never);
    const failure = new Error('socket timeout while the pull is still running');
    const originalUpdateOne = Collection.prototype.updateOne;
    let delayed: { collection: Collection; args: Parameters<Collection['updateOne']> } | undefined;
    vi.spyOn(Collection.prototype, 'updateOne').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['updateOne']>
    ) {
      const [filter, update] = args as [Document, Document];
      if (
        delayed === undefined &&
        this.collectionName.endsWith('_lanes') &&
        update['$pull'] !== undefined &&
        filter['inFlight'] === undefined
      ) {
        delayed = { collection: this, args };
        throw failure;
      }
      return originalUpdateOne.apply(this, args);
    } as never);
    await expect(claim(store, ['h'], 'worker-1', at(3))).rejects.toBe(failure);
    vi.restoreAllMocks();
    // The requeued head runs and completes, then the successor is claimed again in the same epoch.
    const [requeuedHead] = await claim(store, ['h'], 'worker-1', at(4));
    expect(requeuedHead!.event.id).toBe(first.id);
    expect(await store.complete(requeuedHead!, at(5))).toBe(true);
    const [successor] = await claim(store, ['h'], 'worker-1', at(6));
    expect(successor!.event.id).toBe(second.id);
    // Only now does the server execute the old pull.
    await originalUpdateOne.apply(delayed!.collection, delayed!.args);
    expect(await claim(store, ['h'], 'worker-1', at(7))).toEqual([]);
    expect(await store.complete(successor!, at(8))).toBe(true);
  });
});
