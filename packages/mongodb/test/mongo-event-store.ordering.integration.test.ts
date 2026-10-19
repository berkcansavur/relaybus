import { Collection, type CommandSucceededEvent, type Document, type MongoClient } from 'mongodb';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import type { ClaimedDelivery, EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { laneOf } from '../src/lanes.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

const LEASE_MS = 30_000;

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
  readonly deliveries: Collection<Document>;
}

async function createStore(completionHistory = false): Promise<TestStore> {
  const database = uniqueDatabaseName();
  const store = new MongoEventStore({ client, database, completionHistory });
  await store.setup();
  return { store, deliveries: client.db(database).collection('relaybus_deliveries') };
}

async function publish(
  store: MongoEventStore,
  events: readonly EventEnvelope[],
  handlers: readonly string[] = ['h'],
  availableAt: Date = at(0),
): Promise<void> {
  await store.transaction((session) =>
    store.append(
      session,
      events.map((event) => ({
        event,
        deliveries: handlers.map((handler) => createDelivery(handler, availableAt)),
      })),
      at(0),
    ),
  );
}

function claimAt(
  store: MongoEventStore,
  nowMs: number,
  handlers: readonly string[] = ['h'],
  limit = 10,
): Promise<ClaimedDelivery[]> {
  return store.claim({
    handlers,
    limit,
    leaseOwner: 'worker',
    leaseExpiresAt: at(nowMs + LEASE_MS),
    now: at(nowMs),
  });
}

function keyedEvents(key: string, count: number): EventEnvelope[] {
  return Array.from({ length: count }, () => createEnvelope({ key }));
}

/**
 * Runs `inject` right after the first predecessor lookup of a claim returned,
 * which is the moment between "nothing precedes this delivery" and its lease.
 */
function afterFirstPredecessorLookup(inject: () => Promise<unknown>): void {
  const original = Collection.prototype.findOne as (...args: unknown[]) => Promise<unknown>;
  let pending = true;
  vi.spyOn(Collection.prototype, 'findOne').mockImplementation(async function (
    this: Collection,
    ...args: unknown[]
  ) {
    const result = await original.apply(this, args);
    const filter = args[0] as { sequence?: { $lt?: number } };
    if (pending && filter.sequence?.$lt !== undefined) {
      pending = false;
      await inject();
    }
    return result;
  } as never);
}

describe('requeued dead letters', () => {
  it('lets only the earlier delivery run when a requeue lands between the checks and the lease', async () => {
    const { store, deliveries } = await createStore();
    const [first, second] = keyedEvents('k', 2) as [EventEnvelope, EventEnvelope];
    await publish(store, [first, second]);
    const [head] = await claimAt(store, 0);
    await store.bury(head!, { name: 'Error', message: 'x' }, at(1));
    const before = await deliveries.find({ eventId: second.id }).next();

    afterFirstPredecessorLookup(() => store.requeue([head!.deliveryId], at(2)));
    expect(await claimAt(store, 3)).toEqual([]);
    vi.restoreAllMocks();

    expect(await deliveries.find({ eventId: second.id }).next()).toEqual(before);
    const [requeued] = await claimAt(store, 4);
    expect(requeued!.event.id).toBe(first.id);
    expect(await claimAt(store, 5)).toEqual([]);
    await store.complete(requeued!, at(6));
    const [successor] = await claimAt(store, 7);
    expect(successor).toMatchObject({ attempt: 1 });
    expect(successor!.event.id).toBe(second.id);
  });

  it('fences a crashed worker and gives its delivery back when a requeue lands during the takeover', async () => {
    const { store, deliveries } = await createStore();
    const [first, second] = keyedEvents('k', 2) as [EventEnvelope, EventEnvelope];
    await publish(store, [first, second]);
    const [head] = await claimAt(store, 0);
    await store.bury(head!, { name: 'Error', message: 'x' }, at(1));
    const [crashed] = await claimAt(store, 2);
    expect(crashed!.event.id).toBe(second.id);
    const before = await deliveries.find({ eventId: second.id }).next();

    afterFirstPredecessorLookup(() => store.requeue([head!.deliveryId], at(3 + LEASE_MS)));
    expect(await claimAt(store, 3 + LEASE_MS)).toEqual([]);
    vi.restoreAllMocks();

    // The takeover charged the crashed worker's lost attempt and fenced its lease.
    expect(await deliveries.find({ eventId: second.id }).next()).toEqual({
      ...before,
      attempts: 1,
      claims: 1,
      lostAt: expect.any(Number),
    });
    expect(await store.complete(crashed!, at(4 + LEASE_MS))).toBe(false);
    const [requeued] = await claimAt(store, 5 + LEASE_MS);
    expect(requeued!.event.id).toBe(first.id);
    expect(await claimAt(store, 6 + LEASE_MS)).toEqual([]);
    await store.complete(requeued!, at(7 + LEASE_MS));
    const [successor] = await claimAt(store, 8 + LEASE_MS);
    expect(successor).toMatchObject({ attempt: 2 });
    expect(successor!.event.id).toBe(second.id);
  });

  it('holds every requeued delivery back while a later one of the key runs', async () => {
    const { store } = await createStore();
    const events = keyedEvents('k', 3);
    await publish(store, events);
    const [first] = await claimAt(store, 0);
    await store.bury(first!, { name: 'Error', message: 'x' }, at(1));
    const [second] = await claimAt(store, 2);
    await store.bury(second!, { name: 'Error', message: 'x' }, at(3));
    const [third] = await claimAt(store, 4);
    expect(third!.event.id).toBe(events[2]!.id);
    expect(await store.requeue([first!.deliveryId, second!.deliveryId], at(5))).toBe(2);

    expect(await claimAt(store, 6)).toEqual([]);
    await store.complete(third!, at(7));
    const released = await claimAt(store, 8);
    expect(released.map((delivery) => delivery.event.id)).toEqual([events[0]!.id]);
  });

  it('moves on when another claimer takes the delivery first', async () => {
    const { store } = await createStore();
    const keyed = createEnvelope({ key: 'k' });
    // Deliveries of one lane are taken together: keep the two apart.
    let unkeyed = createEnvelope();
    while (laneOf(null, unkeyed.id) === laneOf('k', keyed.id)) {
      unkeyed = createEnvelope();
    }
    await publish(store, [keyed]);
    await publish(store, [unkeyed], ['h'], at(1));
    vi.spyOn(Collection.prototype, 'findOneAndUpdate').mockImplementationOnce(
      (async () => null) as never,
    );

    const claimed = await claimAt(store, 5);
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([unkeyed.id]);
    vi.restoreAllMocks();
    expect((await claimAt(store, 6)).map((delivery) => delivery.event.id)).toEqual([keyed.id]);
  });

  it('never runs two deliveries of a key at once while dead letters are requeued at random', async () => {
    const { store } = await createStore(true);
    const keys = Array.from({ length: 5 }, (_, index) => `key-${index}`);
    const eventsPerKey = 12;
    const total = keys.length * eventsPerKey;
    const inFlight = new Set<string>();
    const violations: string[] = [];
    const completed = new Set<string>();
    let buried = 0;

    const publishers = keys.map(async (key) => {
      for (let index = 0; index < eventsPerKey; index += 1) {
        await publish(store, [createEnvelope({ key })]);
      }
    });

    async function handle(delivery: ClaimedDelivery): Promise<void> {
      const slot = `${delivery.handler}/${delivery.event.key}`;
      if (inFlight.has(slot)) {
        violations.push(`two in flight for ${slot}`);
      }
      inFlight.add(slot);
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 5));
      inFlight.delete(slot);
      if (buried < 25 && Math.random() < 0.2) {
        buried += 1;
        expect(await store.bury(delivery, { name: 'Error', message: 'dead' }, at(0))).toBe(true);
        return;
      }
      expect(await store.complete(delivery, at(0))).toBe(true);
      if (completed.has(delivery.deliveryId)) {
        violations.push(`completed twice ${delivery.deliveryId}`);
      }
      completed.add(delivery.deliveryId);
    }

    async function worker(index: number): Promise<void> {
      const deadline = Date.now() + 90_000;
      while (completed.size < total && Date.now() < deadline) {
        const claimed = await store.claim({
          handlers: ['h'],
          limit: 3,
          leaseOwner: `worker-${index}`,
          leaseExpiresAt: at(10 ** 9),
          now: at(0),
        });
        await Promise.all(claimed.map(handle));
        if (claimed.length === 0) {
          await new Promise((resolve) => setTimeout(resolve, 3));
        }
      }
    }

    async function operator(): Promise<void> {
      const deadline = Date.now() + 90_000;
      while (completed.size < total && Date.now() < deadline) {
        const dead = await store.listDeliveries({ status: 'dead' });
        const chosen = dead.filter(() => Math.random() < 0.6).map((record) => record.id);
        await store.requeue(chosen, at(0));
        await new Promise((resolve) => setTimeout(resolve, 2));
      }
    }

    await Promise.all([
      ...publishers,
      operator(),
      ...Array.from({ length: 8 }, (_, i) => worker(i)),
    ]);
    expect(violations).toEqual([]);
    expect(completed.size).toBe(total);
    expect(await store.statistics(at(0))).toMatchObject({ completed: total, dead: 0 });
  });
});

describe('parking of blocked backlogs', () => {
  function countDocumentsRead(): { stop(): number } {
    let total = 0;
    const onSucceeded = (event: CommandSucceededEvent): void => {
      if (event.commandName === 'find' || event.commandName === 'getMore') {
        const cursor = (
          event.reply as { cursor?: { firstBatch?: unknown[]; nextBatch?: unknown[] } }
        ).cursor;
        total += (cursor?.firstBatch ?? cursor?.nextBatch ?? []).length;
      }
    };
    client.on('commandSucceeded', onSucceeded);
    return {
      stop: () => {
        client.off('commandSucceeded', onSucceeded);
        return total;
      },
    };
  }

  /** A running head with `followers` deliveries of its key waiting, parked by one claim. */
  async function parkBacklog(
    followers: number,
    completionHistory = false,
  ): Promise<TestStore & { events: EventEnvelope[]; head: ClaimedDelivery }> {
    const created = await createStore(completionHistory);
    const events = keyedEvents('hot', followers + 1);
    await publish(created.store, events.slice(0, 1));
    await publish(created.store, events.slice(1), ['h'], at(1));
    const [head] = await claimAt(created.store, 0, ['h'], 1);
    expect(head!.event.id).toBe(events[0]!.id);
    expect(await claimAt(created.store, 1)).toEqual([]);
    return { ...created, events, head: head! };
  }

  it('parks a long queue behind a running delivery so that claims stop reading it', async () => {
    const { store, deliveries, head } = await parkBacklog(60);
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(60);

    const reading = countDocumentsRead();
    expect(await claimAt(store, 2)).toEqual([]);
    expect(reading.stop()).toBeLessThanOrEqual(5);
    expect(await store.statistics(at(2))).toEqual({
      pending: 60,
      processing: 1,
      completed: 0,
      dead: 0,
      oldestDueAt: at(1),
    });
    expect(await store.extendLease(head, at(2 * LEASE_MS), at(3))).toBe(true);
  });

  it('releases the parked queue in publish order, one delivery at a time', async () => {
    const { store, deliveries, events, head } = await parkBacklog(40, true);
    expect(await store.complete(head, at(2))).toBe(true);
    for (const expected of events.slice(1)) {
      const claimed = await claimAt(store, 3);
      expect(claimed.map((delivery) => delivery.event.id)).toEqual([expected.id]);
      expect(await store.complete(claimed[0]!, at(4))).toBe(true);
    }
    expect(await claimAt(store, 5)).toEqual([]);
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(0);
    expect(await store.statistics(at(5))).toMatchObject({ completed: 41, pending: 0 });
  });

  it('moves the next parked delivery on when its predecessor is buried and requeued', async () => {
    const { store, events, head } = await parkBacklog(40);
    await store.bury(head, { name: 'Error', message: 'x' }, at(2));
    const [next] = await claimAt(store, 3);
    expect(next!.event.id).toBe(events[1]!.id);

    expect(await store.requeue([head.deliveryId], at(4))).toBe(1);
    expect(await claimAt(store, 5)).toEqual([]);
    await store.complete(next!, at(6));
    const [again] = await claimAt(store, 7);
    expect(again!.event.id).toBe(events[0]!.id);
    expect(await claimAt(store, 8)).toEqual([]);
    await store.complete(again!, at(9));
    const [following] = await claimAt(store, 10);
    expect(following!.event.id).toBe(events[2]!.id);
  });

  it('never strands a key whose predecessor finished before its backlog was parked', async () => {
    const { store, deliveries } = await createStore();
    const firstKey = keyedEvents('first', 3);
    const secondKey = keyedEvents('second', 3);
    const otherHandler = keyedEvents('first', 2);
    const delayed = keyedEvents('delayed', 2);
    await publish(store, [...firstKey, ...secondKey], ['h']);
    await publish(store, otherHandler, ['g']);
    await publish(store, delayed, ['h'], at(10 ** 6));
    const heads = await claimAt(store, 0, ['h', 'g'], 5);
    expect(heads).toHaveLength(3);
    for (const head of heads) {
      await store.complete(head, at(1));
    }
    // What a crash between "predecessor finished" and "backlog parked" leaves behind.
    await deliveries.updateMany(
      { status: 'pending', eventId: { $ne: delayed[0]!.id } },
      { $set: { lane: 'parked' } },
    );

    const released = await claimAt(store, 2, ['h', 'g', 'unused']);
    expect(released.map((delivery) => `${delivery.handler}/${delivery.event.key}`).sort()).toEqual([
      'g/first',
      'h/first',
      'h/second',
    ]);
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(3);
  });

  it('keeps short queues in the claim index', async () => {
    const { store, deliveries } = await parkBacklog(5);
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(0);
    expect(await claimAt(store, 2)).toEqual([]);
  });

  it('never parks a running delivery that waits behind a requeued one', async () => {
    const { store, deliveries } = await createStore();
    const [first, second] = keyedEvents('k', 2) as [EventEnvelope, EventEnvelope];
    await publish(store, [first, second]);
    const [head] = await claimAt(store, 0);
    await store.bury(head!, { name: 'Error', message: 'x' }, at(1));
    const [crashed] = await claimAt(store, 2);
    await store.requeue([head!.deliveryId], at(5));

    const claimed = await claimAt(store, 3 + LEASE_MS);
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([first.id]);
    // The taken-over delivery went back to pending with its lost attempt counted, and stays ready.
    expect(await deliveries.find({ _id: crashed!.deliveryId as never }).next()).toMatchObject({
      status: 'pending',
      lane: 'ready',
      attempts: 1,
    });
  });
});
