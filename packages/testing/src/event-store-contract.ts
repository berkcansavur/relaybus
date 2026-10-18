import type {
  AppendEntry,
  ClaimedDelivery,
  EventEnvelope,
  EventStore,
  NewDelivery,
} from '@relaybus/core';
import { createUuidV7 } from '@relaybus/core';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

export interface EventStoreContractOptions<TTransaction> {
  /** Creates an empty, set-up store. Called before every test. */
  readonly createStore: () => Promise<EventStore<TTransaction>>;
  /** Releases the store. Called after every test. */
  readonly destroyStore?: (store: EventStore<TTransaction>) => Promise<void>;
  /**
   * Creates a second store instance on the same database with its own
   * connections, simulating another process. Enables cross-instance tests.
   * Stores without shared state across instances (in-memory) omit it.
   */
  readonly createPeerStore?: (
    primary: EventStore<TTransaction>,
  ) => Promise<EventStore<TTransaction>>;
}

interface Deferred {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
}

function createDeferred(): Deferred {
  let resolve!: () => void;
  const promise = new Promise<void>((onResolve) => {
    resolve = onResolve;
  });
  return { promise, resolve };
}

const BASE_TIME = Date.parse('2026-01-01T00:00:00.000Z');
const LEASE_MS = 30_000;

export function at(offsetMs: number): Date {
  return new Date(BASE_TIME + offsetMs);
}

export function createEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: createUuidV7(),
    type: 'contract.event.v1',
    source: 'contract-suite',
    key: null,
    payload: { value: 1 },
    headers: {},
    correlationId: null,
    causationId: null,
    occurredAt: at(0),
    ...overrides,
  };
}

export function createDelivery(handler: string, availableAt: Date = at(0)): NewDelivery {
  return { id: createUuidV7(), handler, availableAt };
}

/**
 * Behaviour every {@link EventStore} must have. Call it inside a test file of
 * the store package:
 *
 * ```ts
 * eventStoreContract('PostgresEventStore', { createStore: async () => ... });
 * ```
 */
export function eventStoreContract<TTransaction>(
  name: string,
  options: EventStoreContractOptions<TTransaction>,
): void {
  describe(`${name} satisfies the EventStore contract`, () => {
    let store: EventStore<TTransaction>;

    beforeEach(async () => {
      store = await options.createStore();
    });

    afterEach(async () => {
      await options.destroyStore?.(store);
    });

    async function append(entries: AppendEntry[], now: Date = at(0)) {
      return store.transaction((transaction) => store.append(transaction, entries, now));
    }

    async function claim(
      handlers: string[],
      now: Date = at(0),
      limit = 100,
      owner = 'worker-1',
    ): Promise<ClaimedDelivery[]> {
      return store.claim({
        handlers,
        limit,
        leaseOwner: owner,
        leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
        now,
      });
    }

    describe('setup', () => {
      it('can run more than once', async () => {
        await store.setup?.();
        await store.setup?.();
        expect(await store.statistics(at(0))).toMatchObject({ pending: 0 });
      });
    });

    describe('append', () => {
      it('stores events and deliveries when the transaction commits', async () => {
        const event = createEnvelope();
        const result = await append([
          { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
        ]);
        expect(result).toEqual({
          appendedEventIds: [event.id],
          duplicateEventIds: [],
          appendedDeliveryCount: 2,
        });
        const records = await store.listDeliveries({ eventId: event.id });
        expect(records.map((record) => record.handler).sort()).toEqual(['handler-a', 'handler-b']);
        expect(
          records.every((record) => record.status === 'pending' && record.attempts === 0),
        ).toBe(true);
      });

      it('stores nothing when the transaction rolls back', async () => {
        const event = createEnvelope();
        const failure = new Error('rollback');
        await expect(
          store.transaction(async (transaction) => {
            await store.append(
              transaction,
              [{ event, deliveries: [createDelivery('handler-a')] }],
              at(0),
            );
            throw failure;
          }),
        ).rejects.toBe(failure);
        expect(await store.listDeliveries({})).toEqual([]);
        expect(await claim(['handler-a'])).toEqual([]);
      });

      it('returns the value of the transaction work', async () => {
        await expect(store.transaction(async () => 'result')).resolves.toBe('result');
      });

      it('round-trips the envelope exactly as JSON would', async () => {
        const event = createEnvelope({
          key: 'order-42',
          payload: { text: 'çğıöşü 🚀', nested: { list: [1, 'two', null, true] }, amount: 12.5 },
          headers: { traceparent: '00-abc-def-01', tenant: 'acme' },
          correlationId: createUuidV7(),
          causationId: createUuidV7(),
          occurredAt: new Date('2026-03-04T05:06:07.089Z'),
        });
        await append([{ event, deliveries: [createDelivery('handler-a')] }]);
        const [claimed] = await claim(['handler-a']);
        expect(claimed!.event).toEqual(event);
        expect(claimed!.event.occurredAt).toBeInstanceOf(Date);
      });

      it('skips an event id that already exists but adds its missing deliveries', async () => {
        const event = createEnvelope();
        await append([{ event, deliveries: [createDelivery('handler-a')] }]);
        const result = await append([
          { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
        ]);
        expect(result).toEqual({
          appendedEventIds: [],
          duplicateEventIds: [event.id],
          appendedDeliveryCount: 1,
        });
        const records = await store.listDeliveries({ eventId: event.id });
        expect(records.map((record) => record.handler).sort()).toEqual(['handler-a', 'handler-b']);
      });

      it('deduplicates repeated events and handlers inside one call', async () => {
        const event = createEnvelope();
        const result = await append([
          { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-a')] },
          { event, deliveries: [createDelivery('handler-a')] },
        ]);
        expect(result.appendedEventIds).toEqual([event.id]);
        expect(result.duplicateEventIds).toEqual([event.id]);
        expect(result.appendedDeliveryCount).toBe(1);
        expect(await store.listDeliveries({})).toHaveLength(1);
      });

      it('keeps events written by concurrent transactions apart', async () => {
        const events = Array.from({ length: 20 }, () => createEnvelope());
        await Promise.all(
          events.map((event) => append([{ event, deliveries: [createDelivery('handler-a')] }])),
        );
        expect(await store.listDeliveries({ limit: 1_000 })).toHaveLength(20);
      });
    });

    describe('claim', () => {
      it('leases due deliveries of the requested handlers and counts the attempt', async () => {
        const event = createEnvelope();
        await append([
          { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
        ]);
        const claimed = await claim(['handler-a'], at(1_000));
        expect(claimed).toHaveLength(1);
        expect(claimed[0]).toMatchObject({
          handler: 'handler-a',
          attempt: 1,
          leaseExpiresAt: at(1_000 + LEASE_MS),
        });
        expect(claimed[0]!.leaseToken).toEqual(expect.any(String));
        const [record] = await store.listDeliveries({ handler: 'handler-a' });
        expect(record).toMatchObject({ status: 'processing', attempts: 1, leaseOwner: 'worker-1' });
        expect(record!.leaseExpiresAt).toEqual(at(1_000 + LEASE_MS));
      });

      it('ignores deliveries that are not due yet', async () => {
        await append([
          { event: createEnvelope(), deliveries: [createDelivery('handler-a', at(5_000))] },
        ]);
        expect(await claim(['handler-a'], at(4_999))).toEqual([]);
        expect(await claim(['handler-a'], at(5_000))).toHaveLength(1);
      });

      it('returns nothing for an empty handler list', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        expect(await claim([])).toEqual([]);
      });

      it('respects the limit and claims the earliest due first', async () => {
        const late = createEnvelope();
        const early = createEnvelope();
        const middle = createEnvelope();
        await append([
          { event: late, deliveries: [createDelivery('handler-a', at(300))] },
          { event: early, deliveries: [createDelivery('handler-a', at(100))] },
          { event: middle, deliveries: [createDelivery('handler-a', at(200))] },
        ]);
        const first = await claim(['handler-a'], at(1_000), 2);
        expect(first.map((delivery) => delivery.event.id)).toEqual([early.id, middle.id]);
        const second = await claim(['handler-a'], at(1_000), 2);
        expect(second.map((delivery) => delivery.event.id)).toEqual([late.id]);
      });

      it('breaks ties between equally due deliveries by delivery id', async () => {
        const ids = [createUuidV7(), createUuidV7(), createUuidV7()].sort();
        await append([
          {
            event: createEnvelope(),
            deliveries: [{ id: ids[2]!, handler: 'handler-a', availableAt: at(0) }],
          },
          {
            event: createEnvelope(),
            deliveries: [{ id: ids[0]!, handler: 'handler-a', availableAt: at(0) }],
          },
          {
            event: createEnvelope(),
            deliveries: [{ id: ids[1]!, handler: 'handler-a', availableAt: at(0) }],
          },
        ]);
        const claimed = await claim(['handler-a'], at(0), 3);
        expect(claimed.map((delivery) => delivery.deliveryId)).toEqual(ids);
      });

      it('does not hand out a leased delivery until its lease expires', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [first] = await claim(['handler-a'], at(0));
        expect(await claim(['handler-a'], at(LEASE_MS - 1), 100, 'worker-2')).toEqual([]);
        const [second] = await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
        expect(second).toMatchObject({ deliveryId: first!.deliveryId, attempt: 2 });
        expect(second!.leaseToken).not.toBe(first!.leaseToken);
      });

      it('never gives the same delivery to two concurrent claimers', async () => {
        const entries = Array.from({ length: 40 }, () => ({
          event: createEnvelope(),
          deliveries: [createDelivery('handler-a')],
        }));
        await append(entries);
        const batches = await Promise.all(
          Array.from({ length: 8 }, (_, index) =>
            claim(['handler-a'], at(0), 10, `worker-${index}`),
          ),
        );
        const claimedIds = batches.flat().map((delivery) => delivery.deliveryId);
        expect(new Set(claimedIds).size).toBe(claimedIds.length);
        expect(claimedIds).toHaveLength(40);
      });
    });

    describe('fencing', () => {
      it('completes a delivery only with its current lease', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [stale] = await claim(['handler-a'], at(0));
        const [current] = await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
        expect(await store.complete(stale!, at(LEASE_MS + 1))).toBe(false);
        expect(await store.extendLease(stale!, at(LEASE_MS * 3), at(LEASE_MS + 1))).toBe(false);
        expect(
          await store.reschedule(stale!, at(0), { name: 'Error', message: 'x' }, at(LEASE_MS + 1)),
        ).toBe(false);
        expect(await store.bury(stale!, { name: 'Error', message: 'x' }, at(LEASE_MS + 1))).toBe(
          false,
        );
        expect(await store.complete(current!, at(LEASE_MS + 2))).toBe(true);
        const [record] = await store.listDeliveries({});
        expect(record).toMatchObject({
          status: 'completed',
          leaseOwner: null,
          leaseExpiresAt: null,
        });
        expect(record!.completedAt).toEqual(at(LEASE_MS + 2));
      });

      it('refuses to complete twice', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a']);
        expect(await store.complete(delivery!, at(1))).toBe(true);
        expect(await store.complete(delivery!, at(2))).toBe(false);
      });

      it('refuses leases of unknown deliveries', async () => {
        const lease = { deliveryId: createUuidV7(), leaseToken: createUuidV7() };
        expect(await store.complete(lease, at(0))).toBe(false);
        expect(await store.extendLease(lease, at(1), at(0))).toBe(false);
      });

      it('extends a held lease', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a']);
        expect(await store.extendLease(delivery!, at(LEASE_MS * 2), at(10))).toBe(true);
        expect(await claim(['handler-a'], at(LEASE_MS + 1), 100, 'worker-2')).toEqual([]);
        const [record] = await store.listDeliveries({});
        expect(record!.leaseExpiresAt).toEqual(at(LEASE_MS * 2));
      });

      it('completes inside a transaction together with it', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a']);
        await expect(
          store.transaction(async (transaction) => {
            expect(await store.complete(delivery!, at(1), transaction)).toBe(true);
            throw new Error('rollback');
          }),
        ).rejects.toThrow('rollback');
        expect((await store.listDeliveries({}))[0]!.status).toBe('processing');
        await store.transaction(async (transaction) => {
          expect(await store.complete(delivery!, at(2), transaction)).toBe(true);
        });
        expect((await store.listDeliveries({}))[0]!.status).toBe('completed');
      });

      it('refuses a transactional completion with a lost lease', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [stale] = await claim(['handler-a'], at(0));
        await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
        await store.transaction(async (transaction) => {
          expect(await store.complete(stale!, at(LEASE_MS + 1), transaction)).toBe(false);
        });
        expect((await store.listDeliveries({}))[0]!.status).toBe('processing');
      });
    });

    describe('retries and dead letters', () => {
      it('reschedules a delivery with its error', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a']);
        const error = { name: 'TypeError', message: 'boom', stack: 'TypeError: boom\n    at x' };
        expect(await store.reschedule(delivery!, at(10_000), error, at(5))).toBe(true);
        const [record] = await store.listDeliveries({});
        expect(record).toMatchObject({
          status: 'pending',
          attempts: 1,
          lastError: error,
          leaseOwner: null,
        });
        expect(record!.availableAt).toEqual(at(10_000));
        expect(await claim(['handler-a'], at(9_999))).toEqual([]);
        const [retry] = await claim(['handler-a'], at(10_000));
        expect(retry!.attempt).toBe(2);
      });

      it('buries a delivery so it is never claimed again', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a']);
        const error = { name: 'Error', message: 'fatal' };
        expect(await store.bury(delivery!, error, at(5))).toBe(true);
        expect(await claim(['handler-a'], at(LEASE_MS * 10))).toEqual([]);
        const [record] = await store.listDeliveries({ status: 'dead' });
        expect(record).toMatchObject({ status: 'dead', lastError: error });
        expect(record!.updatedAt).toEqual(at(5));
      });

      it('requeues only dead deliveries, with a fresh attempt budget', async () => {
        const event = createEnvelope();
        await append([
          { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
        ]);
        const [dead] = await claim(['handler-a']);
        await store.bury(dead!, { name: 'Error', message: 'fatal' }, at(1));
        const [pending] = await store.listDeliveries({ handler: 'handler-b' });
        const moved = await store.requeue(
          [dead!.deliveryId, dead!.deliveryId, pending!.id, createUuidV7()],
          at(100),
        );
        expect(moved).toBe(1);
        const [record] = await store.listDeliveries({ handler: 'handler-a' });
        expect(record).toMatchObject({ status: 'pending', attempts: 0 });
        expect(record!.availableAt).toEqual(at(100));
        const [again] = await claim(['handler-a'], at(100));
        expect(again!.attempt).toBe(1);
      });
    });

    describe('ordering keys', () => {
      async function appendKeyed(key: string, count: number, handlers = ['handler-a']) {
        const events = Array.from({ length: count }, () => createEnvelope({ key }));
        for (const event of events) {
          await append([{ event, deliveries: handlers.map((handler) => createDelivery(handler)) }]);
        }
        return events;
      }

      it('hands out one delivery per handler and key, in publish order', async () => {
        const [first, second, third] = await appendKeyed('order-1', 3);
        const batch1 = await claim(['handler-a']);
        expect(batch1.map((delivery) => delivery.event.id)).toEqual([first!.id]);
        expect(await claim(['handler-a'])).toEqual([]);
        await store.complete(batch1[0]!, at(1));
        const batch2 = await claim(['handler-a']);
        expect(batch2.map((delivery) => delivery.event.id)).toEqual([second!.id]);
        await store.complete(batch2[0]!, at(2));
        expect((await claim(['handler-a'])).map((delivery) => delivery.event.id)).toEqual([
          third!.id,
        ]);
      });

      it('keeps later events waiting while the head is retrying', async () => {
        const [first, second] = await appendKeyed('order-1', 2);
        const [head] = await claim(['handler-a']);
        await store.reschedule(head!, at(60_000), { name: 'Error', message: 'retry' }, at(1));
        expect(await claim(['handler-a'], at(30_000))).toEqual([]);
        const [retried] = await claim(['handler-a'], at(60_000));
        expect(retried!.event.id).toBe(first!.id);
        await store.complete(retried!, at(60_001));
        expect((await claim(['handler-a'], at(60_002)))[0]!.event.id).toBe(second!.id);
      });

      it('releases the key when the head is dead-lettered', async () => {
        const [, second] = await appendKeyed('order-1', 2);
        const [head] = await claim(['handler-a']);
        await store.bury(head!, { name: 'Error', message: 'fatal' }, at(1));
        expect((await claim(['handler-a']))[0]!.event.id).toBe(second!.id);
      });

      it('orders each handler independently', async () => {
        const [first, second] = await appendKeyed('order-1', 2, ['handler-a', 'handler-b']);
        const [fromA] = await claim(['handler-a']);
        await store.complete(fromA!, at(1));
        const [nextA] = await claim(['handler-a']);
        expect(nextA!.event.id).toBe(second!.id);
        const [fromB] = await claim(['handler-b']);
        expect(fromB!.event.id).toBe(first!.id);
      });

      it('runs different keys and unkeyed events in parallel', async () => {
        await appendKeyed('order-1', 2);
        await appendKeyed('order-2', 2);
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        expect(await claim(['handler-a'])).toHaveLength(3);
      });

      it('orders concurrent publishers of the same key by commit', async () => {
        const events = Array.from({ length: 10 }, (_, index) =>
          createEnvelope({ key: 'shared', payload: { index } }),
        );
        await Promise.all(
          events.map((event) => append([{ event, deliveries: [createDelivery('handler-a')] }])),
        );
        const seen: string[] = [];
        for (let round = 0; round < 10; round += 1) {
          const batch = await claim(['handler-a']);
          expect(batch).toHaveLength(1);
          seen.push(batch[0]!.event.id);
          await store.complete(batch[0]!, at(round + 1));
        }
        expect(new Set(seen).size).toBe(10);
      });
    });

    describe('queries', () => {
      it('filters deliveries and applies the limit in creation order', async () => {
        const first = createEnvelope({ type: 'contract.first.v1' });
        const second = createEnvelope({ type: 'contract.second.v1' });
        await append([{ event: first, deliveries: [createDelivery('handler-a')] }], at(1));
        await append(
          [
            {
              event: second,
              deliveries: [createDelivery('handler-a'), createDelivery('handler-b')],
            },
          ],
          at(2),
        );
        expect(await store.listDeliveries({ eventType: 'contract.second.v1' })).toHaveLength(2);
        expect(await store.listDeliveries({ handler: 'handler-b' })).toHaveLength(1);
        expect(
          await store.listDeliveries({ status: 'pending', handler: 'handler-a' }),
        ).toHaveLength(2);
        expect(await store.listDeliveries({ status: 'dead' })).toEqual([]);
        const limited = await store.listDeliveries({ limit: 1 });
        expect(limited).toHaveLength(1);
        expect(limited[0]!.event.id).toBe(first.id);
        expect(limited[0]!.createdAt).toEqual(at(1));
      });

      it('reports counts and the oldest due delivery', async () => {
        await append([
          { event: createEnvelope(), deliveries: [createDelivery('handler-a', at(100))] },
          { event: createEnvelope(), deliveries: [createDelivery('handler-a', at(200))] },
          { event: createEnvelope(), deliveries: [createDelivery('handler-b', at(50_000))] },
          { event: createEnvelope(), deliveries: [createDelivery('handler-c', at(0))] },
        ]);
        const [processing] = await claim(['handler-c'], at(0));
        const [dead] = await claim(['handler-a'], at(100), 1);
        await store.bury(dead!, { name: 'Error', message: 'x' }, at(101));
        expect(await store.statistics(at(1_000))).toEqual({
          pending: 2,
          processing: 1,
          completed: 0,
          dead: 1,
          oldestDueAt: at(200),
        });
        expect((await store.statistics(at(LEASE_MS))).oldestDueAt).toEqual(at(200));
        await store.complete(processing!, at(1));
        expect(await store.statistics(at(0))).toMatchObject({ completed: 1, oldestDueAt: null });
      });

      it('counts an expired lease as due', async () => {
        await append([
          { event: createEnvelope(), deliveries: [createDelivery('handler-a', at(LEASE_MS * 5))] },
        ]);
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-b')] }]);
        await claim(['handler-b'], at(0));
        expect((await store.statistics(at(LEASE_MS + 1))).oldestDueAt).toEqual(at(LEASE_MS));
      });
    });

    describe('purge', () => {
      it('deletes old completed and dead deliveries and their orphaned events', async () => {
        const done = createEnvelope();
        const failed = createEnvelope();
        const shared = createEnvelope();
        await append([
          { event: done, deliveries: [createDelivery('handler-a')] },
          { event: failed, deliveries: [createDelivery('handler-b')] },
          { event: shared, deliveries: [createDelivery('handler-c'), createDelivery('handler-d')] },
        ]);
        const [doneDelivery] = await claim(['handler-a']);
        await store.complete(doneDelivery!, at(10));
        const [failedDelivery] = await claim(['handler-b']);
        await store.bury(failedDelivery!, { name: 'Error', message: 'x' }, at(10));
        const [sharedDelivery] = await claim(['handler-c']);
        await store.complete(sharedDelivery!, at(10));

        expect(await store.purge({})).toEqual({ deletedDeliveries: 0, deletedEvents: 0 });
        expect(await store.purge({ completedBefore: at(10) })).toEqual({
          deletedDeliveries: 0,
          deletedEvents: 0,
        });
        expect(await store.purge({ completedBefore: at(11) })).toEqual({
          deletedDeliveries: 2,
          deletedEvents: 1,
        });
        expect(await store.purge({ deadBefore: at(11) })).toEqual({
          deletedDeliveries: 1,
          deletedEvents: 1,
        });
        const remaining = await store.listDeliveries({});
        expect(remaining.map((record) => record.handler)).toEqual(['handler-d']);
        expect(remaining[0]!.event.id).toBe(shared.id);
      });

      it('lets a purged event id be appended again', async () => {
        const event = createEnvelope();
        await append([{ event, deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a']);
        await store.complete(delivery!, at(1));
        await store.purge({ completedBefore: at(2) });
        const result = await append([{ event, deliveries: [createDelivery('handler-a')] }]);
        expect(result.appendedEventIds).toEqual([event.id]);
      });
    });

    describe('release', () => {
      it('gives a leased delivery back without counting the attempt', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a'], at(0));
        expect(await store.release(delivery!, at(500))).toBe(true);
        const [record] = await store.listDeliveries({});
        expect(record).toMatchObject({
          status: 'pending',
          attempts: 0,
          leaseOwner: null,
          leaseExpiresAt: null,
        });
        expect(record!.availableAt).toEqual(at(500));
        expect(record!.updatedAt).toEqual(at(500));
        expect(await store.release(delivery!, at(501))).toBe(false);
        const [again] = await claim(['handler-a'], at(500));
        expect(again!.attempt).toBe(1);
      });
    });

    describe('transactions in flight', () => {
      it('hides uncommitted appends from every reader', async () => {
        const inside = createDeferred();
        const finish = createDeferred();
        const event = createEnvelope();
        const open = store.transaction(async (transaction) => {
          await store.append(
            transaction,
            [{ event, deliveries: [createDelivery('handler-a')] }],
            at(0),
          );
          inside.resolve();
          await finish.promise;
        });
        await inside.promise;
        expect(await claim(['handler-a'])).toEqual([]);
        expect(await store.listDeliveries({})).toEqual([]);
        expect((await store.statistics(at(0))).pending).toBe(0);
        finish.resolve();
        await open;
        expect(await claim(['handler-a'])).toHaveLength(1);
      });

      it('orders same-key events by commit, not by first write', async () => {
        const first = createEnvelope({ key: 'race' });
        const second = createEnvelope({ key: 'race' });
        const firstWritten = createDeferred();
        const releaseFirst = createDeferred();
        const firstTransaction = store.transaction(async (transaction) => {
          await store.append(
            transaction,
            [{ event: first, deliveries: [createDelivery('handler-a')] }],
            at(0),
          );
          firstWritten.resolve();
          await releaseFirst.promise;
        });
        await firstWritten.promise;
        const secondTransaction = store.transaction((transaction) =>
          store.append(
            transaction,
            [{ event: second, deliveries: [createDelivery('handler-a')] }],
            at(0),
          ),
        );
        await new Promise((resolve) => setTimeout(resolve, 50));
        releaseFirst.resolve();
        await firstTransaction;
        await secondTransaction;
        const [head] = await claim(['handler-a']);
        expect(head!.event.id).toBe(first.id);
        await store.complete(head!, at(1));
        expect((await claim(['handler-a']))[0]!.event.id).toBe(second.id);
      });

      it('stores an event id written by two concurrent transactions once', async () => {
        const event = createEnvelope({ key: 'same' });
        const results = await Promise.all([
          append([{ event, deliveries: [createDelivery('handler-a')] }]),
          append([
            { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
          ]),
        ]);
        const appended = results.flatMap((result) => result.appendedEventIds);
        expect(appended).toEqual([event.id]);
        const records = await store.listDeliveries({});
        expect(records.map((record) => record.handler).sort()).toEqual(['handler-a', 'handler-b']);
      });
    });

    describe('ordering keys under load', () => {
      it('does not let a blocked key starve unrelated deliveries', async () => {
        const blocked = Array.from({ length: 120 }, () => ({
          event: createEnvelope({ key: 'hot' }),
          deliveries: [createDelivery('handler-a', at(0))],
        }));
        await append(blocked);
        const unrelated = Array.from({ length: 5 }, () => ({
          event: createEnvelope(),
          deliveries: [createDelivery('handler-a', at(10))],
        }));
        await append(unrelated);
        const claimed = await claim(['handler-a'], at(100), 10);
        expect(claimed).toHaveLength(6);
        expect(claimed.filter((delivery) => delivery.event.key === 'hot')).toHaveLength(1);
      });

      it('keeps successors blocked behind an expired, delayed or requeued head', async () => {
        const head = createEnvelope({ key: 'k' });
        const next = createEnvelope({ key: 'k' });
        await append([{ event: head, deliveries: [createDelivery('handler-a')] }]);
        await append([{ event: next, deliveries: [createDelivery('handler-a')] }]);
        const [claimedHead] = await claim(['handler-a'], at(0));
        const reclaimed = await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
        expect(reclaimed.map((delivery) => delivery.event.id)).toEqual([head.id]);
        expect(
          await store.reschedule(
            reclaimed[0]!,
            at(LEASE_MS * 10),
            { name: 'Error', message: 'x' },
            at(LEASE_MS),
          ),
        ).toBe(true);
        expect(await claim(['handler-a'], at(LEASE_MS * 5))).toEqual([]);
        const [delayedHead] = await claim(['handler-a'], at(LEASE_MS * 10));
        await store.bury(delayedHead!, { name: 'Error', message: 'x' }, at(LEASE_MS * 10));
        expect(await store.requeue([delayedHead!.deliveryId], at(LEASE_MS * 11))).toBe(1);
        const [requeuedHead] = await claim(['handler-a'], at(LEASE_MS * 11));
        expect(requeuedHead!.event.id).toBe(head.id);
        expect(await claim(['handler-a'], at(LEASE_MS * 11))).toEqual([]);
        expect(claimedHead).toBeDefined();
      });
    });

    describe('requeued keyed dead letters', () => {
      it('never runs a requeued dead letter alongside its running successor', async () => {
        const first = createEnvelope({ key: 'k' });
        const second = createEnvelope({ key: 'k' });
        await append([{ event: first, deliveries: [createDelivery('handler-a')] }]);
        await append([{ event: second, deliveries: [createDelivery('handler-a')] }]);
        const [head] = await claim(['handler-a'], at(0));
        await store.bury(head!, { name: 'Error', message: 'x' }, at(1));
        const [successor] = await claim(['handler-a'], at(2));
        expect(successor!.event.id).toBe(second.id);
        expect(await store.requeue([head!.deliveryId], at(3))).toBe(1);
        expect(await claim(['handler-a'], at(4))).toEqual([]);
        await store.complete(successor!, at(5));
        const [requeued] = await claim(['handler-a'], at(6));
        expect(requeued!.event.id).toBe(first.id);
      });

      it('recovers when the running successor crashes after the requeue', async () => {
        const first = createEnvelope({ key: 'k' });
        const second = createEnvelope({ key: 'k' });
        await append([{ event: first, deliveries: [createDelivery('handler-a')] }]);
        await append([{ event: second, deliveries: [createDelivery('handler-a')] }]);
        const [head] = await claim(['handler-a'], at(0));
        await store.bury(head!, { name: 'Error', message: 'x' }, at(1));
        await claim(['handler-a'], at(2));
        await store.requeue([head!.deliveryId], at(3));
        const recovered = await claim(['handler-a'], at(2 + LEASE_MS));
        expect(recovered.map((delivery) => delivery.event.id)).toEqual([first.id]);
        await store.complete(recovered[0]!, at(3 + LEASE_MS));
        const [retried] = await claim(['handler-a'], at(4 + LEASE_MS));
        expect(retried).toMatchObject({ attempt: 2 });
        expect(retried!.event.id).toBe(second.id);
      });
    });

    describe('lease semantics', () => {
      it('confirms a repeated renewal with identical values', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a'], at(0));
        expect(await store.extendLease(delivery!, at(LEASE_MS * 2), at(10))).toBe(true);
        expect(await store.extendLease(delivery!, at(LEASE_MS * 2), at(10))).toBe(true);
      });

      it('accepts an expired lease that nobody reclaimed', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [delivery] = await claim(['handler-a'], at(0));
        expect(await store.extendLease(delivery!, at(LEASE_MS * 3), at(LEASE_MS * 2))).toBe(true);
        expect(await store.complete(delivery!, at(LEASE_MS * 2))).toBe(true);
      });

      it('rejects every lease operation once the delivery left processing', async () => {
        const error = { name: 'Error', message: 'x' };
        await append([
          {
            event: createEnvelope(),
            deliveries: [createDelivery('a'), createDelivery('b'), createDelivery('c')],
          },
        ]);
        const [completed] = await claim(['a']);
        await store.complete(completed!, at(1));
        const [buried] = await claim(['b']);
        await store.bury(buried!, error, at(1));
        const [rescheduled] = await claim(['c']);
        await store.reschedule(rescheduled!, at(100), error, at(1));
        for (const lease of [completed!, buried!, rescheduled!]) {
          expect(await store.extendLease(lease, at(LEASE_MS), at(2))).toBe(false);
          expect(await store.reschedule(lease, at(3), error, at(2))).toBe(false);
          expect(await store.bury(lease, error, at(2))).toBe(false);
          expect(await store.release(lease, at(2))).toBe(false);
          expect(await store.complete(lease, at(2))).toBe(false);
        }
      });

      it('rejects made-up leases on pending deliveries and requeues only dead ones', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('a')] }]);
        const [pending] = await store.listDeliveries({});
        const fake = { deliveryId: pending!.id, leaseToken: createUuidV7() };
        expect(await store.reschedule(fake, at(1), { name: 'Error', message: 'x' }, at(1))).toBe(
          false,
        );
        expect(await store.bury(fake, { name: 'Error', message: 'x' }, at(1))).toBe(false);
        const [processing] = await claim(['a']);
        expect(await store.requeue([processing!.deliveryId], at(1))).toBe(0);
        expect(await store.purge({ completedBefore: at(1e9), deadBefore: at(1e9) })).toEqual({
          deletedDeliveries: 0,
          deletedEvents: 0,
        });
        expect(await store.listDeliveries({})).toHaveLength(1);
      });
    });

    describe('data fidelity', () => {
      it.each([null, 0, 42.5, 'text', [1, { two: 2 }], { deep: { list: [] } }, true])(
        'round-trips the payload %j',
        async (payload) => {
          const event = createEnvelope({ payload });
          await append([{ event, deliveries: [createDelivery('handler-a')] }]);
          const [claimed] = await claim(['handler-a']);
          expect(claimed!.event.payload).toEqual(payload);
        },
      );

      it('accepts long identifiers and non-UUID event ids', async () => {
        const handler = 'h'.repeat(255);
        const event = createEnvelope({
          id: `order-42:placed:${'x'.repeat(200)}`,
          key: 'k'.repeat(255),
          type: 't'.repeat(255),
        });
        await append([{ event, deliveries: [createDelivery(handler)] }]);
        const [claimed] = await claim([handler]);
        expect(claimed!.event).toEqual(event);
        expect(claimed!.handler).toBe(handler);
      });

      it('lists at most 100 deliveries by default', async () => {
        await append(
          Array.from({ length: 101 }, () => ({
            event: createEnvelope(),
            deliveries: [createDelivery('handler-a')],
          })),
        );
        expect(await store.listDeliveries({})).toHaveLength(100);
      });

      it('hands out copies that callers cannot use to change stored data', async () => {
        const event = createEnvelope({ payload: { amount: 1 }, headers: { a: '1' } });
        await append([{ event, deliveries: [createDelivery('handler-a')] }]);
        const [claimed] = await claim(['handler-a']);
        (claimed!.event.payload as { amount: number }).amount = 999;
        (claimed!.event.headers as Record<string, string>)['a'] = 'changed';
        const [listed] = await store.listDeliveries({});
        expect(listed!.event.payload).toEqual({ amount: 1 });
        expect(listed!.event.headers).toEqual({ a: '1' });
      });
    });

    describe.skipIf(options.createPeerStore === undefined)('across store instances', () => {
      let peer: EventStore<TTransaction>;

      beforeEach(async () => {
        peer = await options.createPeerStore!(store);
      });

      afterEach(async () => {
        await options.destroyStore?.(peer);
      });

      it('never gives one delivery to claimers in different processes', async () => {
        await append(
          Array.from({ length: 60 }, (_, index) => ({
            event: createEnvelope({ key: index % 3 === 0 ? `key-${index % 9}` : null }),
            deliveries: [createDelivery('handler-a')],
          })),
        );
        const request = (owner: string) => ({
          handlers: ['handler-a'],
          limit: 7,
          leaseOwner: owner,
          leaseExpiresAt: at(LEASE_MS),
          now: at(0),
        });
        const batches = await Promise.all([
          ...Array.from({ length: 5 }, (_, index) => store.claim(request(`primary-${index}`))),
          ...Array.from({ length: 5 }, (_, index) => peer.claim(request(`peer-${index}`))),
        ]);
        const claimed = batches.flat();
        expect(new Set(claimed.map((delivery) => delivery.deliveryId)).size).toBe(claimed.length);
        const keys = claimed.map((delivery) => delivery.event.key).filter((key) => key !== null);
        expect(new Set(keys).size).toBe(keys.length);
      });

      it('fences a lease taken over by another process', async () => {
        await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
        const [stale] = await claim(['handler-a'], at(0));
        const [current] = await peer.claim({
          handlers: ['handler-a'],
          limit: 1,
          leaseOwner: 'peer',
          leaseExpiresAt: at(LEASE_MS * 2),
          now: at(LEASE_MS),
        });
        expect(await store.complete(stale!, at(LEASE_MS + 1))).toBe(false);
        expect(await peer.complete(current!, at(LEASE_MS + 1))).toBe(true);
      });
    });
  });
}
