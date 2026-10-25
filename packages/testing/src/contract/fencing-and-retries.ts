import { createUuidV7, type EventStore } from '@relaybus/core';
import { beforeEach, describe, expect, it } from 'vitest';
import { at, createEnvelope, createDelivery, LEASE_MS } from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/** Fencing by lease token, retries, dead letters and ordering keys. */
export function fencingContract<TTransaction>(suite: ContractSuite<TTransaction>): void {
  const { append, claim, keepsCompleted } = suite;
  let store!: EventStore<TTransaction>;

  beforeEach(() => {
    store = suite.store();
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
      if (!keepsCompleted) {
        expect(await store.listDeliveries({})).toEqual([]);
        return;
      }
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
      if (keepsCompleted) {
        expect((await store.listDeliveries({}))[0]!.status).toBe('completed');
      } else {
        expect(await store.listDeliveries({})).toEqual([]);
      }
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
}
