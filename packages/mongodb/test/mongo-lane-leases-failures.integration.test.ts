import { Collection, type Document } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { type ClaimedDelivery, type EventEnvelope } from '@relaybus/core';
import { at, createEnvelope } from '@relaybus/testing';
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
} from './store-support.js';

useReplicaSet();

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
    // The charge of the lost attempts is one bulk write on the deliveries.
    failOnce('bulkWrite', '_deliveries');
    await expect(claim(store, ['h'], 'worker-2', at(LEASE_MS))).rejects.toBe(failure);
    vi.restoreAllMocks();
    const lane = await lanes.findOne({ _id: laneOf('k', 'unused') });
    expect(lane).toMatchObject({
      owner: 'worker-2',
      inFlight: [],
      lost: [{ id: head.deliveryId, epoch: Number(head.leaseToken.split(':')[1]) }],
    });
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
    // The predecessor check that runs after recording reads all picks with one `$or` query.
    failOnce('find', '_deliveries', (args) => (args[0] as Document)['$or'] !== undefined);
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
        /^\[MongoOrdering\]\(parkWaiting\) Parking waiting deliveries failed, a later claim parks them \| count=40 \| error=Error: connection lost$/,
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

  it('takes a lane whose due deliveries all wait behind a delayed head once, then leaves it alone', async () => {
    const { store, lanes, deliveries } = await delayedHead(5);
    const lane = laneOf('k', 'unused');
    const before = await lanes.findOne({ _id: lane });
    expect(await claim(store, ['h'], 'worker-2', at(LEASE_MS * 2))).toEqual([]);
    // The claim that handed the lane back parked its waiting deliveries.
    expect(await deliveries.countDocuments({ lane: 'parked' })).toBe(5);
    const handedBack = await lanes.findOne({ _id: lane });
    expect(handedBack).toMatchObject({ epoch: before!['epoch'] + 1, owner: null });
    for (const owner of ['worker-3', 'worker-4']) {
      expect(await claim(store, ['h'], owner, at(LEASE_MS * 3))).toEqual([]);
    }
    expect(await lanes.findOne({ _id: lane })).toEqual(handedBack);
    // Nothing is lost: once the head is due and done, the followers run in order.
    const [head] = await claim(store, ['h'], 'worker-3', at(10 ** 9));
    expect(await store.complete(head!, at(10 ** 9))).toBe(true);
    for (let index = 0; index < 5; index += 1) {
      const [follower] = await claim(store, ['h'], 'worker-3', at(10 ** 9 + 1 + index));
      expect(follower).toBeDefined();
      expect(await store.complete(follower!, at(10 ** 9 + 1 + index))).toBe(true);
    }
    expect(await store.statistics(at(10 ** 9 + 10))).toMatchObject({ pending: 0, processing: 0 });
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
