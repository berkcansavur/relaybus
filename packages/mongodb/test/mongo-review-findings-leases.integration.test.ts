import { Collection, type Document } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { laneOf } from '../src/lanes.js';

import {
  LEASE_MS,
  error,
  client,
  useReplicaSet,
  append,
  claim,
  intercept,
  createStore as createSharedStore,
} from './store-support.js';

useReplicaSet();

const createStore = (completionHistory = false) => createSharedStore({ completionHistory });

const DAY = 86_400_000;

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
