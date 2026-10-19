import { Collection, MongoError, type Document, type MongoClient } from 'mongodb';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import type { ClaimedDelivery, EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { laneOf } from '../src/lanes.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

const LEASE_MS = 30_000;
const DAY = 86_400_000;
const error = { name: 'Error', message: 'x' };

/** A raw stored document: ids are strings (lanes: numbers), never ObjectIds. */
type RawDocument = { _id: string | number } & Document;

let replicaSet: MongoReplicaSet;
let client: MongoClient;

beforeAll(async () => {
  replicaSet = await startReplicaSet();
  client = await replicaSet.connect();
});

afterAll(async () => {
  await client.close();
  await replicaSet.stop();
});

afterEach(() => {
  vi.restoreAllMocks();
});

interface TestStore {
  readonly store: MongoEventStore;
  readonly database: string;
  readonly deliveries: Collection<RawDocument>;
  readonly lanes: Collection<RawDocument>;
  readonly events: Collection<RawDocument>;
}

async function createStore(completionHistory = false): Promise<TestStore> {
  const database = uniqueDatabaseName();
  const store = new MongoEventStore({ client, database, completionHistory });
  await store.setup();
  const db = client.db(database);
  return {
    store,
    database,
    deliveries: db.collection('relaybus_deliveries'),
    lanes: db.collection('relaybus_lanes'),
    events: db.collection('relaybus_events'),
  };
}

async function append(
  store: MongoEventStore,
  entries: Array<{ event: EventEnvelope; handlers: string[] }>,
  now: Date = at(0),
): Promise<void> {
  await store.transaction((session) =>
    store.append(
      session,
      entries.map(({ event, handlers }) => ({
        event,
        deliveries: handlers.map((handler) => createDelivery(handler)),
      })),
      now,
    ),
  );
}

function claim(
  store: MongoEventStore,
  handlers: string[],
  owner: string,
  now: Date = at(0),
  limit = 100,
): Promise<ClaimedDelivery[]> {
  return store.claim({
    handlers,
    limit,
    leaseOwner: owner,
    leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
    now,
  });
}

/**
 * Wraps `method` on the collections whose name ends with `suffix`: `before`
 * runs ahead of the original call for every call that `matches` accepts.
 */
function intercept<M extends 'find' | 'deleteMany' | 'bulkWrite'>(
  method: M,
  suffix: string,
  before: (args: unknown[]) => Promise<void>,
  matches: (args: unknown[]) => boolean = () => true,
  afterRead = false,
): void {
  const original = Collection.prototype[method] as (...args: unknown[]) => unknown;
  vi.spyOn(Collection.prototype, method).mockImplementation(function (
    this: Collection,
    ...args: unknown[]
  ) {
    if (!this.collectionName.endsWith(suffix) || !matches(args)) {
      return original.apply(this, args);
    }
    if (method === 'find') {
      // find returns a cursor synchronously: run `before` when the cursor is read.
      const cursor = original.apply(this, args) as { toArray(): Promise<unknown[]> };
      const toArray = cursor.toArray.bind(cursor);
      cursor.toArray = async () => {
        if (afterRead) {
          // Runs `before` once the documents were read, ahead of whatever the caller does next.
          const documents = await toArray();
          await before(args);
          return documents;
        }
        await before(args);
        return toArray();
      };
      return cursor;
    }
    return before(args).then(() => original.apply(this, args));
  } as never);
}

describe('A: a lease names the claim it was issued for', () => {
  it('puts the claim generation into the token and moves it on with every release, retry and takeover', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [first] = await claim(store, ['h'], 'worker-1', at(0));
    expect(first!.leaseToken).toMatch(/^\d+:\d+:0$/);
    await store.release(first!, at(1));
    const [second] = await claim(store, ['h'], 'worker-1', at(2));
    expect(second!.leaseToken.split(':').slice(0, 2)).toEqual(
      first!.leaseToken.split(':').slice(0, 2),
    );
    expect(second!.leaseToken.split(':')[2]).toBe('1');
    await store.reschedule(second!, at(3), error, at(3));
    const [third] = await claim(store, ['h'], 'worker-1', at(3));
    expect(third!.leaseToken.split(':')[2]).toBe('2');
    const [fourth] = await claim(store, ['h'], 'worker-2', at(3 + LEASE_MS));
    expect(fourth!.leaseToken.split(':')[2]).toBe('3');
    for (const stale of [first!, second!, third!]) {
      expect(await store.extendLease(stale, at(LEASE_MS * 4), at(4 + LEASE_MS))).toBe(false);
      expect(await store.complete(stale, at(4 + LEASE_MS))).toBe(false);
    }
    expect(await store.complete(fourth!, at(5 + LEASE_MS))).toBe(true);
  });

  it('does not let a stale transactional completion delete the newer attempt', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [stale] = await claim(store, ['h'], 'worker-1', at(0));
    await store.release(stale!, at(1));
    const [current] = await claim(store, ['h'], 'worker-1', at(2));
    await store.transaction(async (session) => {
      expect(await store.complete(stale!, at(3), session)).toBe(false);
    });
    expect(await store.statistics(at(3))).toMatchObject({ processing: 1 });
    expect(await store.complete(current!, at(4))).toBe(true);
  });

  it('treats a delivery stored without a claim generation as generation 0', async () => {
    const { store, deliveries } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    await deliveries.updateMany({}, { $unset: { claims: '' } });
    const [delivery] = await claim(store, ['h'], 'worker-1');
    expect(delivery!.leaseToken).toMatch(/:0$/);
    expect(await store.extendLease(delivery!, at(LEASE_MS * 2), at(1))).toBe(true);
    expect(await store.complete(delivery!, at(2))).toBe(true);
  });
});

describe('D: completion without a transaction against a concurrent release', () => {
  it('does not delete a delivery that was released between the fence read and the delete', async () => {
    const { store, deliveries } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1');
    let released = false;
    intercept('deleteMany', '_deliveries', async () => {
      if (!released) {
        released = true;
        expect(await store.release(delivery!, at(1))).toBe(true);
      }
    });
    expect(await store.completeMany([delivery!], at(2))).toEqual([false]);
    vi.restoreAllMocks();
    expect(await deliveries.findOne({ _id: delivery!.deliveryId })).toMatchObject({
      status: 'pending',
      attempts: 0,
    });
    const [again] = await claim(store, ['h'], 'worker-1', at(3));
    expect(again).toMatchObject({ deliveryId: delivery!.deliveryId, attempt: 1 });
  });
});

describe('E: overlapping claims of one owner on one store', () => {
  it('never hands the same delivery to two overlapping claims of the same owner', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    // Hold the lane, then put the delivery back, so both claims read it from a held lane.
    const [held] = await claim(store, ['h'], 'worker-1', at(0));
    await store.release(held!, at(1));
    let candidateReads = 0;
    let secondRead!: () => void;
    const bothRead = new Promise<void>((resolve) => {
      secondRead = resolve;
    });
    intercept(
      'find',
      '_deliveries',
      async () => {
        candidateReads += 1;
        if (candidateReads === 2) {
          secondRead();
        }
      },
      (args) => (args[0] as Document)['laneId'] !== undefined,
    );
    let recording = 0;
    intercept('bulkWrite', '_lanes', async () => {
      recording += 1;
      if (recording === 1) {
        // Without serialization the second claim reads the same in-flight list meanwhile.
        await Promise.race([bothRead, new Promise((resolve) => setTimeout(resolve, 300))]);
      }
    });
    const [first, second] = await Promise.all([
      claim(store, ['h'], 'worker-1', at(2)),
      claim(store, ['h'], 'worker-1', at(2)),
    ]);
    vi.restoreAllMocks();
    expect([...first, ...second].map((delivery) => delivery.deliveryId)).toEqual([
      held!.deliveryId,
    ]);
  });

  it('keeps serving claims of the owner after one of them failed', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const failure = new Error('connection lost');
    let failed = false;
    intercept('find', '_events', async () => {
      if (!failed) {
        failed = true;
        throw failure;
      }
    });
    const outcomes = await Promise.allSettled([
      claim(store, ['h'], 'worker-1', at(0)),
      claim(store, ['h'], 'worker-1', at(0)),
    ]);
    vi.restoreAllMocks();
    expect(outcomes[0]).toEqual({ status: 'rejected', reason: failure });
    expect(outcomes[1]).toMatchObject({ status: 'fulfilled', value: [expect.anything()] });
  });
});

describe('J: charging a lost attempt once', () => {
  it('charges the attempt exactly once when clearing the lost list fails after the charge', async () => {
    const { store, deliveries, lanes } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const [lost] = await claim(store, ['h'], 'worker-1', at(0));
    const failure = new Error('connection lost');
    let failed = false;
    intercept(
      'bulkWrite',
      '_lanes',
      async () => {
        if (!failed) {
          failed = true;
          throw failure;
        }
      },
      // The write that clears the lost list.
      (args) => JSON.stringify(args[0]).includes('"lost":[]'),
    );
    await expect(claim(store, ['h'], 'worker-2', at(LEASE_MS))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await deliveries.findOne({ _id: lost!.deliveryId })).toMatchObject({ attempts: 1 });
    const [again] = await claim(store, ['h'], 'worker-2', at(LEASE_MS + 1));
    expect(again).toMatchObject({ deliveryId: lost!.deliveryId, attempt: 2 });
    expect(await deliveries.findOne({ _id: lost!.deliveryId })).toMatchObject({ attempts: 1 });
    expect(await lanes.findOne({ _id: laneOf(null, event.id) })).toMatchObject({ lost: [] });
  });
});

describe('I: retention counts from the last completion', () => {
  it('keeps an event whose other delivery completed recently when the dead letters are purged', async () => {
    const { store, events } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['done', 'failed'] }]);
    const [failed] = await claim(store, ['failed'], 'worker-1', at(0));
    await store.bury(failed!, error, at(1));
    const [done] = await claim(store, ['done'], 'worker-1', at(30 * DAY));
    expect(await store.complete(done!, at(30 * DAY))).toBe(true);
    expect(await store.purge({ deadBefore: at(29 * DAY) })).toEqual({
      deletedDeliveries: 1,
      deletedEvents: 0,
    });
    expect(await events.findOne({ _id: event.id })).toMatchObject({ completedAt: at(30 * DAY) });
    expect(await store.purge({ completedBefore: at(31 * DAY) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 1,
    });
  });

  it('records the latest completion of an event, also from a transactional completion', async () => {
    const { store, events } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['a', 'b'] }]);
    const claimed = await claim(store, ['a', 'b'], 'worker-1');
    const [a, b] = claimed.sort((left, right) => left.handler.localeCompare(right.handler));
    await store.transaction(async (session) => {
      expect(await store.complete(b!, at(20), session)).toBe(true);
    });
    expect(await store.complete(a!, at(10))).toBe(true);
    expect(await events.findOne({ _id: event.id })).toMatchObject({ completedAt: at(20) });
  });

  it('applies the same rule with a completion history', async () => {
    const { store, events } = await createStore(true);
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['a'] }]);
    const [delivery] = await claim(store, ['a'], 'worker-1');
    expect(await store.completeMany([delivery!], at(5))).toEqual([true]);
    expect(await events.findOne({ _id: event.id })).toMatchObject({ completedAt: at(5) });
  });
});

describe('M1: a claim that fails while or after recording strands nothing', () => {
  /** Lets the first record write of the lanes reach the database, then reports a failure. */
  function recordThenFail(failure: Error): void {
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
  }

  it('takes the picks of a failed claim out of the lane even when their record was written', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const failure = new Error('connection reset after the write');
    recordThenFail(failure);
    await expect(claim(store, ['h'], 'worker-1')).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await lanes.findOne({ _id: laneOf(null, event.id) })).toMatchObject({ inFlight: [] });
    expect(await store.statistics(at(1))).toMatchObject({ processing: 0, pending: 1 });
    const [again] = await claim(store, ['h'], 'worker-1', at(1));
    expect(again).toMatchObject({ attempt: 1 });
    expect(again!.event.id).toBe(event.id);
  });

  it('keeps the deliveries it handed out and has not settled listed after a failed claim', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope({ key: 'k' });
    await append(store, [{ event, handlers: ['h1', 'h2'] }]);
    const [running] = await claim(store, ['h1'], 'worker-1');
    const failure = new Error('connection reset after the write');
    recordThenFail(failure);
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await lanes.findOne({ _id: laneOf('k', 'unused') })).toMatchObject({
      inFlight: [running!.deliveryId],
    });
    expect(await store.complete(running!, at(2))).toBe(true);
    const [next] = await claim(store, ['h2'], 'worker-1', at(3));
    expect(next).toMatchObject({ handler: 'h2', attempt: 1 });
  });
});

describe('M2: a delayed charge of an older lost claim', () => {
  it('does nothing once a newer lost claim of the delivery was charged', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const [first] = await claim(store, ['h'], 'worker-1', at(0));
    await claim(store, ['h'], 'worker-2', at(LEASE_MS));
    const [current] = await claim(store, ['h'], 'worker-3', at(LEASE_MS * 2));
    expect(current).toMatchObject({ attempt: 3 });
    const lane = laneOf(null, event.id);
    // A holder that paused before charging the first loss charges it now.
    await lanes.updateOne(
      { _id: lane },
      {
        $set: { lost: [{ id: first!.deliveryId, epoch: Number(first!.leaseToken.split(':')[1]) }] },
      },
    );
    expect(await claim(store, ['h'], 'worker-3', at(LEASE_MS * 2 + 1))).toEqual([]);
    expect(await lanes.findOne({ _id: lane })).toMatchObject({ lost: [] });
    expect(await store.complete(current!, at(LEASE_MS * 2 + 2))).toBe(true);
  });
});

describe('M4: renewal with a stale lease', () => {
  it('leaves the lane of the newer claim untouched', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const [stale] = await claim(store, ['h'], 'worker-1', at(0));
    await store.release(stale!, at(1));
    await claim(store, ['h'], 'worker-1', at(2));
    const before = await lanes.findOne({ _id: laneOf(null, event.id) });
    expect(await store.extendLease(stale!, at(LEASE_MS * 10), at(3))).toBe(false);
    expect(await lanes.findOne({ _id: laneOf(null, event.id) })).toEqual(before);
  });
});

describe('completion without a transaction never claims a deletion it cannot prove', () => {
  it('reports false when the delivery vanished because the claim that took it over completed it', async () => {
    const { store, database } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [stale] = await claim(store, ['h'], 'worker-1', at(0));
    const peer = new MongoEventStore({ client, database });
    let interleaved = false;
    intercept('deleteMany', '_deliveries', async () => {
      if (!interleaved) {
        interleaved = true;
        const [taken] = await claim(peer, ['h'], 'worker-2', at(LEASE_MS));
        expect(await peer.complete(taken!, at(LEASE_MS + 1))).toBe(true);
      }
    });
    expect(await store.completeMany([stale!], at(LEASE_MS + 2))).toEqual([false]);
  });
});

describe('M3: a transactional completion after appending to its own event', () => {
  it('records the completion time inside the caller transaction instead of waiting for it', async () => {
    const { store, events } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1');
    await store.transaction(async (session) => {
      await store.append(session, [{ event, deliveries: [createDelivery('late')] }], at(1));
      expect(await store.complete(delivery!, at(2), session)).toBe(true);
    });
    expect(await events.findOne({ _id: event.id })).toMatchObject({
      completedAt: at(2),
      handlers: ['h', 'late'],
    });
  }, 20_000);
});

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
