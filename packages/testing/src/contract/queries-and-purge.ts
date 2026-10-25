import type { EventStore } from '@relaybus/core';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  at,
  createEnvelope,
  createDelivery,
  createDeferred,
  LEASE_MS,
} from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/** Queries, purge, release and appends of transactions still in flight. */
export function maintenanceContract<TTransaction>(suite: ContractSuite<TTransaction>): void {
  const { append, claim, keepsCompleted } = suite;
  let store!: EventStore<TTransaction>;

  beforeEach(() => {
    store = suite.store();
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
      expect(await store.listDeliveries({ status: 'pending', handler: 'handler-a' })).toHaveLength(
        2,
      );
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
      expect(await store.statistics(at(0))).toMatchObject({
        completed: keepsCompleted ? 1 : 0,
        processing: 0,
        oldestDueAt: null,
      });
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
    async function appendDoneFailedShared() {
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
      return { shared };
    }

    it.runIf(keepsCompleted)(
      'deletes old completed and dead deliveries and their orphaned events',
      async () => {
        const { shared } = await appendDoneFailedShared();
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
      },
    );

    it.runIf(!keepsCompleted)(
      'deletes old dead deliveries and the events no delivery references any more',
      async () => {
        const { shared } = await appendDoneFailedShared();
        // Completed deliveries are already gone: only their event is left behind.
        expect(await store.purge({})).toEqual({ deletedDeliveries: 0, deletedEvents: 0 });
        expect(await store.purge({ completedBefore: at(0) })).toEqual({
          deletedDeliveries: 0,
          deletedEvents: 0,
        });
        expect(await store.purge({ completedBefore: at(11) })).toEqual({
          deletedDeliveries: 0,
          deletedEvents: 1,
        });
        expect(await store.purge({ deadBefore: at(11) })).toEqual({
          deletedDeliveries: 1,
          deletedEvents: 1,
        });
        const remaining = await store.listDeliveries({});
        expect(remaining.map((record) => record.handler)).toEqual(['handler-d']);
        expect(remaining[0]!.event.id).toBe(shared.id);
      },
    );

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
        append([{ event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] }]),
      ]);
      const appended = results.flatMap((result) => result.appendedEventIds);
      expect(appended).toEqual([event.id]);
      const records = await store.listDeliveries({});
      expect(records.map((record) => record.handler).sort()).toEqual(['handler-a', 'handler-b']);
    });
  });
}
