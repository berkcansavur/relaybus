import { Collection, type CommandStartedEvent, type Document, type MongoClient } from 'mongodb';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import {
  createUuidV7,
  type ClaimedDelivery,
  type EventEnvelope,
  type LeaseReference,
} from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore, type MongoEventStoreOptions } from '../src/index.js';
import { LANE_COUNT, laneOf } from '../src/lanes.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

const LEASE_MS = 30_000;
const error = { name: 'Error', message: 'x' };

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

/** A raw stored document: ids are strings (lanes: numbers), never ObjectIds. */
type RawDocument = { _id: string | number } & Document;

interface TestStore {
  readonly store: MongoEventStore;
  readonly database: string;
  readonly deliveries: Collection<RawDocument>;
  readonly lanes: Collection<RawDocument>;
  readonly events: Collection<RawDocument>;
}

function collections(database: string): Omit<TestStore, 'store' | 'database'> {
  const db = client.db(database);
  return {
    deliveries: db.collection('relaybus_deliveries'),
    lanes: db.collection('relaybus_lanes'),
    events: db.collection('relaybus_events'),
  };
}

async function createStore(
  options: Partial<Omit<MongoEventStoreOptions, 'client'>> = {},
): Promise<TestStore> {
  const database = options.database ?? uniqueDatabaseName();
  const store = new MongoEventStore({ client, ...options, database });
  await store.setup();
  return { store, database, ...collections(database) };
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

/** One unkeyed event in each of the given lanes. */
function eventsInLanes(lanes: readonly number[]): EventEnvelope[] {
  return lanes.map((lane) => {
    for (;;) {
      const event = createEnvelope();
      if (laneOf(null, event.id) === lane) {
        return event;
      }
    }
  });
}

/** Records the commands the shared client sends while `work` runs. */
async function recordCommands(work: () => Promise<unknown>): Promise<CommandStartedEvent[]> {
  const started: CommandStartedEvent[] = [];
  const onStarted = (event: CommandStartedEvent): void => void started.push(event);
  client.on('commandStarted', onStarted);
  try {
    await work();
  } finally {
    client.off('commandStarted', onStarted);
  }
  return started;
}

function sortById(documents: Document[]): Document[] {
  return [...documents].sort((left, right) => String(left['_id']).localeCompare(right['_id']));
}

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

  it('completes a batch of leases from many lanes in one transaction with a fixed number of commands', async () => {
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
    const names = commands.map((command) => command.commandName);
    expect(names.filter((name) => name === 'commitTransaction')).toHaveLength(1);
    expect(names.filter((name) => name === 'delete')).toHaveLength(1);
    expect(names.filter((name) => name === 'update')).toHaveLength(0);
    expect(names.length).toBeLessThanOrEqual(5);
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

describe('failures during a claim', () => {
  const failure = new Error('connection lost');

  /** A keyed head claimed by worker-1 at 0, and its follower. */
  async function headAndFollower(): Promise<
    TestStore & { head: ClaimedDelivery; follower: EventEnvelope }
  > {
    const created = await createStore();
    const [first, second] = [createEnvelope({ key: 'k' }), createEnvelope({ key: 'k' })];
    await append(created.store, [{ event: first, handlers: ['h'] }]);
    await append(created.store, [{ event: second, handlers: ['h'] }]);
    const [head] = await claim(created.store, ['h'], 'worker-1', at(0));
    expect(head!.event.id).toBe(first.id);
    return { ...created, head: head!, follower: second };
  }

  /** Fails the first call of `method` on the `suffix` collection that `matches` accepts. */
  function failOnce<M extends 'find' | 'findOne' | 'updateMany' | 'bulkWrite'>(
    method: M,
    suffix: string,
    matches: (args: unknown[]) => boolean = () => true,
  ): void {
    const original = Collection.prototype[method] as (...args: unknown[]) => unknown;
    let failed = false;
    vi.spyOn(Collection.prototype, method).mockImplementation(function (
      this: Collection,
      ...args: unknown[]
    ) {
      if (!failed && this.collectionName.endsWith(suffix) && matches(args)) {
        failed = true;
        throw failure;
      }
      return original.apply(this, args);
    } as never);
  }

  it('charges the lost attempt exactly once when the claim fails right after taking the lane', async () => {
    const { store, deliveries, lanes, head, follower } = await headAndFollower();
    failOnce('updateMany', '_deliveries');
    await expect(claim(store, ['h'], 'worker-2', at(LEASE_MS))).rejects.toBe(failure);
    vi.restoreAllMocks();
    const lane = await lanes.findOne({ _id: laneOf('k', 'unused') });
    expect(lane).toMatchObject({ owner: 'worker-2', inFlight: [], lost: [head.deliveryId] });
    expect(await deliveries.findOne({ _id: head.deliveryId })).toMatchObject({ attempts: 0 });
    expect(await store.statistics(at(LEASE_MS))).toMatchObject({ processing: 0, pending: 2 });

    const [retried] = await claim(store, ['h'], 'worker-2', at(LEASE_MS + 1));
    expect(retried).toMatchObject({ deliveryId: head.deliveryId, attempt: 2 });
    expect(await lanes.findOne({ _id: laneOf('k', 'unused') })).toMatchObject({ lost: [] });
    expect(await store.complete(head, at(LEASE_MS + 2))).toBe(false);
    expect(await store.complete(retried!, at(LEASE_MS + 2))).toBe(true);
    const [next] = await claim(store, ['h'], 'worker-2', at(LEASE_MS + 3));
    expect(next).toMatchObject({ attempt: 1 });
    expect(next!.event.id).toBe(follower.id);
  });

  it('runs the lost delivery again, charged once, when the claim fails after charging', async () => {
    const { store, deliveries, head } = await headAndFollower();
    failOnce('find', '_deliveries', (args) => (args[0] as Document)['laneId'] !== undefined);
    await expect(claim(store, ['h'], 'worker-2', at(LEASE_MS))).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await deliveries.findOne({ _id: head.deliveryId })).toMatchObject({ attempts: 1 });
    expect(await store.statistics(at(LEASE_MS))).toMatchObject({ processing: 0 });
    const [retried] = await claim(store, ['h'], 'worker-2', at(LEASE_MS + 1));
    expect(retried).toMatchObject({ deliveryId: head.deliveryId, attempt: 2 });
    expect(await deliveries.findOne({ _id: head.deliveryId })).toMatchObject({ attempts: 1 });
  });

  it('records nothing when reading the events fails, so the batch can be claimed again at once', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    failOnce('find', '_events');
    await expect(claim(store, ['h'], 'worker-1')).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await lanes.findOne({ _id: laneOf(null, event.id) })).toMatchObject({
      owner: 'worker-1',
      inFlight: [],
    });
    const [again] = await claim(store, ['h'], 'worker-1', at(1));
    expect(again).toMatchObject({ attempt: 1 });
    expect(again!.event.id).toBe(event.id);
  });

  it('gives the recorded batch back when the claim fails after recording it', async () => {
    const { store } = await createStore();
    await append(store, [
      { event: createEnvelope({ key: 'k' }), handlers: ['h'] },
      { event: createEnvelope(), handlers: ['h'] },
    ]);
    let lookups = 0;
    failOnce('findOne', '_deliveries', (args) => {
      const filter = args[0] as Document;
      if (filter['sequence'] === undefined) {
        return false;
      }
      lookups += 1;
      // The first lookup is the claim's own check; the second runs after recording.
      return lookups === 2;
    });
    await expect(claim(store, ['h'], 'worker-1')).rejects.toBe(failure);
    vi.restoreAllMocks();
    expect(await store.statistics(at(1))).toMatchObject({ processing: 0, pending: 2 });
    const again = await claim(store, ['h'], 'worker-1', at(1));
    expect(again.map((delivery) => delivery.attempt)).toEqual([1, 1]);
  });

  it('still returns the batch when parking fails, and reports it', async () => {
    const warnings: string[] = [];
    const logger = {
      log: () => {},
      warn: (message: string) => void warnings.push(message),
      error: () => {},
    };
    const { store, deliveries } = await createStore({ logger });
    const events = Array.from({ length: 41 }, () => createEnvelope({ key: 'hot' }));
    await append(store, [{ event: events[0]!, handlers: ['h'] }]);
    await append(
      store,
      events.slice(1).map((event) => ({ event, handlers: ['h'] })),
    );
    const unrelated = createEnvelope();
    await append(store, [{ event: unrelated, handlers: ['h'] }], at(1));
    failOnce('updateMany', '_deliveries', (args) => {
      const update = args[1] as { $set?: { lane?: string } };
      return update.$set?.lane === 'parked';
    });
    const claimed = await claim(store, ['h'], 'worker-1');
    vi.restoreAllMocks();
    expect(claimed.map((delivery) => delivery.event.id).sort()).toEqual(
      [events[0]!.id, unrelated.id].sort(),
    );
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(0);
    expect(warnings).toEqual([
      expect.stringMatching(
        /^\[MongoEventStore\]\(parkWaiting\) Parking waiting deliveries failed, a later claim parks them \| count=40 \| error=Error: connection lost$/,
      ),
    ]);
    expect(await claim(store, ['h'], 'worker-1', at(2))).toEqual([]);
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(40);
  });

  it('does not revive a lane that was handed back while the claim was recording', async () => {
    const { store, lanes } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const original = Collection.prototype.bulkWrite;
    let intercepted = false;
    vi.spyOn(Collection.prototype, 'bulkWrite').mockImplementation(async function (
      this: Collection,
      ...args: Parameters<Collection['bulkWrite']>
    ) {
      if (!intercepted && this.collectionName.endsWith('_lanes')) {
        intercepted = true;
        await store.relinquish('worker-1');
      }
      return original.apply(this, args);
    });
    expect(await claim(store, ['h'], 'worker-1')).toEqual([]);
    vi.restoreAllMocks();
    expect(await lanes.findOne({ _id: laneOf(null, event.id) })).toMatchObject({
      owner: null,
      holder: null,
      inFlight: [],
    });
    expect(await claim(store, ['h'], 'worker-2', at(1))).toEqual([
      expect.objectContaining({ attempt: 1, deliveryId: expect.any(String) }),
    ]);
  });

  it('does not let a restarted process with the same owner name adopt the lanes of its predecessor', async () => {
    const { store, database } = await createStore();
    await append(store, [{ event: createEnvelope({ key: 'k' }), handlers: ['h1', 'h2'] }]);
    const [lost] = await claim(store, ['h1'], 'fixed-owner');
    const restarted = new MongoEventStore({ client, database });
    // The predecessor's lane is still leased: the new instance neither sees nor extends it.
    expect(await claim(restarted, ['h1', 'h2'], 'fixed-owner', at(1))).toEqual([]);
    await restarted.relinquish('fixed-owner');
    expect(await claim(restarted, ['h1', 'h2'], 'fixed-owner', at(LEASE_MS - 1))).toEqual([]);
    const taken = await claim(restarted, ['h1', 'h2'], 'fixed-owner', at(LEASE_MS));
    expect(taken.map((delivery) => [delivery.handler, delivery.attempt]).sort()).toEqual([
      ['h1', 2],
      ['h2', 1],
    ]);
    expect(await store.complete(lost!, at(LEASE_MS + 1))).toBe(false);
  });
});

describe('free lanes with nothing to run', () => {
  /** A key whose head retries far in the future, with `followers` due deliveries behind it. */
  async function delayedHead(followers: number): Promise<TestStore> {
    const created = await createStore();
    const events = Array.from({ length: followers + 1 }, () => createEnvelope({ key: 'k' }));
    await append(created.store, [{ event: events[0]!, handlers: ['h'] }]);
    await append(
      created.store,
      events.slice(1).map((event) => ({ event, handlers: ['h'] })),
    );
    const [head] = await claim(created.store, ['h'], 'worker-1', at(0), 1);
    await created.store.reschedule(head!, at(10 ** 9), error, at(1));
    return created;
  }

  it('leaves a lane alone whose due deliveries all wait behind a delayed head', async () => {
    const { store, lanes } = await delayedHead(5);
    const lane = laneOf('k', 'unused');
    const before = await lanes.findOne({ _id: lane });
    for (const owner of ['worker-2', 'worker-3']) {
      expect(await claim(store, ['h'], owner, at(LEASE_MS * 2))).toEqual([]);
    }
    expect(await lanes.findOne({ _id: lane })).toEqual(before);
  });

  it('takes such a lane once when its backlog is long enough to park, and then leaves it alone', async () => {
    const { store, lanes, deliveries } = await delayedHead(40);
    const lane = laneOf('k', 'unused');
    const before = await lanes.findOne({ _id: lane });
    expect(await claim(store, ['h'], 'worker-2', at(LEASE_MS * 2))).toEqual([]);
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(40);
    const parked = await lanes.findOne({ _id: lane });
    expect(parked).toMatchObject({ epoch: before!['epoch'] + 1, owner: null });
    expect(await claim(store, ['h'], 'worker-3', at(LEASE_MS * 3))).toEqual([]);
    expect(await lanes.findOne({ _id: lane })).toEqual(parked);
  });
});

describe('derived counts and untrusted lease input', () => {
  it('never reports a negative pending count', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    await claim(store, ['h'], 'worker-1');
    const original = Collection.prototype.countDocuments;
    // What a completion between the status count and the running count looks like.
    vi.spyOn(Collection.prototype, 'countDocuments').mockImplementation(function (
      this: Collection,
      ...args: Parameters<Collection['countDocuments']>
    ) {
      return (args[0] as Document | undefined)?.['status'] === 'pending'
        ? Promise.resolve(0)
        : original.apply(this, args);
    } as never);
    expect(await store.statistics(at(1))).toMatchObject({ pending: 0, processing: 1 });
  });

  it('treats a delivery id that is not a string as plain text', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [real] = await claim(store, ['h'], 'worker-1');
    const lease = {
      deliveryId: { $ne: 'nothing' } as unknown as string,
      leaseToken: real!.leaseToken,
    };
    expect(await store.extendLease(lease, at(LEASE_MS * 2), at(1))).toBe(false);
    expect(await store.reschedule(lease, at(5), error, at(1))).toBe(false);
    expect(await store.release(lease, at(1))).toBe(false);
    expect(await store.bury(lease, error, at(1))).toBe(false);
    expect(await store.complete(lease, at(1))).toBe(false);
    expect(await store.complete(real!, at(1))).toBe(true);
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

  it('refuses a well-formed token of another epoch or another lane', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [real] = await claim(store, ['h'], 'worker-1');
    const [lane, epoch] = real!.leaseToken.split(':').map(Number) as [number, number];
    const forged = [
      { deliveryId: real!.deliveryId, leaseToken: `${lane}:${epoch + 1}` },
      { deliveryId: real!.deliveryId, leaseToken: `${(lane + 1) % LANE_COUNT}:${epoch}` },
    ];
    for (const lease of forged) {
      expect(await store.extendLease(lease, at(LEASE_MS * 2), at(1))).toBe(false);
      expect(await store.reschedule(lease, at(5), error, at(1))).toBe(false);
      expect(await store.bury(lease, error, at(1))).toBe(false);
      expect(await store.release(lease, at(1))).toBe(false);
    }
    expect(await store.completeMany([...forged, real!], at(1))).toEqual([false, false, true]);
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

describe('upgrade from 0.1.0', () => {
  const LEGACY_INDEXES = [
    { key: { eventId: 1, handler: 1 }, name: 'event_handler', unique: true },
    {
      key: { handler: 1, dueAt: 1, _id: 1 },
      name: 'claim',
      partialFilterExpression: { lane: 'ready' },
    },
    {
      key: { handler: 1, key: 1, dueAt: 1 },
      name: 'running',
      partialFilterExpression: { status: 'processing', key: { $exists: true } },
    },
  ];

  function legacyEvent(event: EventEnvelope, sequence: number | null): RawDocument {
    return {
      _id: event.id,
      type: event.type,
      source: event.source,
      key: event.key,
      sequence,
      payload: event.payload,
      headers: event.headers,
      correlationId: null,
      causationId: null,
      occurredAt: event.occurredAt,
      revision: 0,
    };
  }

  async function createLegacyDatabase(): Promise<string> {
    const database = uniqueDatabaseName();
    const db = client.db(database);
    for (const name of ['relaybus_events', 'relaybus_deliveries', 'relaybus_key_sequences']) {
      await db.createCollection(name);
    }
    await db.collection('relaybus_deliveries').createIndexes(LEGACY_INDEXES);
    return database;
  }

  it('converts the documents in place, keeps every delivery and can run again', async () => {
    const database = await createLegacyDatabase();
    const { deliveries, events, lanes } = collections(database);
    const keyed = [createEnvelope({ key: 'order-1' }), createEnvelope({ key: 'order-1' })];
    const unkeyed = [createEnvelope(), createEnvelope(), createEnvelope()];
    const silent = createEnvelope({ occurredAt: at(-5) });
    await events.insertMany([
      legacyEvent(keyed[0]!, 1),
      legacyEvent(keyed[1]!, 2),
      ...unkeyed.map((event) => legacyEvent(event, null)),
      legacyEvent(silent, null),
    ]);
    const base = (event: EventEnvelope, sequence: number | null) => ({
      _id: createUuidV7(),
      eventId: event.id,
      eventType: event.type,
      handler: 'h',
      ...(sequence === null ? {} : { key: event.key, sequence }),
      availableAt: at(0),
      createdAt: at(-1),
      updatedAt: at(-1),
    });
    const processing = {
      ...base(keyed[0]!, 1),
      status: 'processing',
      lane: 'ready',
      attempts: 2,
      dueAt: at(LEASE_MS),
      leaseOwner: 'old-worker',
      leaseToken: createUuidV7(),
      leaseExpiresAt: at(LEASE_MS),
    };
    const follower = {
      ...base(keyed[1]!, 2),
      status: 'pending',
      lane: 'ready',
      attempts: 0,
      dueAt: at(0),
    };
    const completed = {
      ...base(unkeyed[0]!, null),
      status: 'completed',
      attempts: 1,
      completedAt: at(1),
    };
    const dead = { ...base(unkeyed[1]!, null), status: 'dead', attempts: 5 };
    const retrying = {
      ...base(unkeyed[2]!, null),
      status: 'pending',
      lane: 'ready',
      attempts: 1,
      dueAt: at(0),
    };
    await deliveries.insertMany([processing, follower, completed, dead, retrying]);

    const store = new MongoEventStore({ client, database });
    await store.setup();
    await store.setup();

    const indexNames = (await deliveries.indexes()).map((index) => index.name).sort();
    expect(indexNames).not.toContain('running');
    expect((await deliveries.indexes()).find((index) => index.name === 'claim')!.key).toEqual({
      laneId: 1,
      dueAt: 1,
      _id: 1,
      handler: 1,
    });
    expect(await lanes.countDocuments()).toBe(LANE_COUNT);
    expect(
      await deliveries.countDocuments({
        $or: [
          { leaseOwner: { $exists: true } },
          { leaseToken: { $exists: true } },
          { leaseExpiresAt: { $exists: true } },
          { laneId: { $exists: false } },
        ],
      }),
    ).toBe(0);
    expect(await deliveries.findOne({ _id: processing._id })).toMatchObject({
      status: 'pending',
      lane: 'ready',
      attempts: 2,
      dueAt: at(0),
      laneId: laneOf('order-1', 'unused'),
    });
    expect(await deliveries.findOne({ _id: dead._id })).toMatchObject({
      laneId: laneOf(null, unkeyed[1]!.id),
    });
    // Every event remembers its handlers and when it was created, so a completed handler never runs again.
    expect(await events.findOne({ _id: unkeyed[0]!.id })).toMatchObject({
      handlers: ['h'],
      createdAt: at(-1),
    });
    expect(await events.findOne({ _id: silent.id })).toMatchObject({
      handlers: [],
      createdAt: at(-5),
    });
    const republished = await store.transaction((session) =>
      store.append(
        session,
        [{ event: unkeyed[0]!, deliveries: [createDelivery('h'), createDelivery('other')] }],
        at(LEASE_MS),
      ),
    );
    expect(republished).toMatchObject({
      duplicateEventIds: [unkeyed[0]!.id],
      appendedDeliveryCount: 1,
    });

    expect(await store.statistics(at(LEASE_MS * 2))).toMatchObject({
      pending: 4,
      processing: 0,
      completed: 1,
      dead: 1,
    });
    const claimed = await claim(store, ['h', 'other'], 'worker-1', at(LEASE_MS * 2));
    // The delivery that was processing keeps the attempt 0.1.0 already charged to it.
    expect(claimed.map((delivery) => [delivery.deliveryId, delivery.attempt])).toEqual(
      expect.arrayContaining([
        [processing._id, 3],
        [retrying._id, 2],
      ]),
    );
    expect(claimed).toHaveLength(3);
    expect(claimed.map((delivery) => delivery.deliveryId)).not.toContain(follower._id);
    expect(await store.completeMany(claimed, at(LEASE_MS * 2 + 1))).toEqual(
      claimed.map(() => true),
    );
    const [next] = await claim(store, ['h'], 'worker-1', at(LEASE_MS * 2 + 2));
    expect(next!.deliveryId).toBe(follower._id);
    // Legacy completed documents are purged as before, and so is the event nobody subscribed to.
    const purged = await store.purge({ completedBefore: at(LEASE_MS * 3) });
    expect(purged.deletedDeliveries).toBe(1);
    expect(await events.findOne({ _id: silent.id })).toBeNull();
  });

  it('converts large collections in batches and skips the conversion once the lanes exist', async () => {
    const database = await createLegacyDatabase();
    const { deliveries, events } = collections(database);
    const legacy = Array.from({ length: 1_001 }, () => createEnvelope());
    await events.insertMany(legacy.map((event) => legacyEvent(event, null)));
    await deliveries.insertMany(
      legacy.map((event) => ({
        _id: createUuidV7(),
        eventId: event.id,
        eventType: event.type,
        handler: 'h',
        status: 'pending',
        lane: 'ready',
        attempts: 0,
        availableAt: at(0),
        dueAt: at(0),
        createdAt: at(0),
        updatedAt: at(0),
      })),
    );
    const store = new MongoEventStore({ client, database });
    await store.setup();
    expect(await deliveries.countDocuments({ laneId: { $exists: true } })).toBe(1_001);
    expect(await events.countDocuments({ handlers: ['h'] })).toBe(1_001);

    // A later setup() does not look at the documents again.
    const marker = legacy[0]!.id;
    await deliveries.updateOne({ eventId: marker }, { $unset: { laneId: '' } });
    await store.setup();
    expect(await deliveries.countDocuments({ laneId: { $exists: false } })).toBe(1);
  });
});
