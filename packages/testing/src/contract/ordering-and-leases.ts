import { createUuidV7, type EventStore } from '@relaybus/core';
import { beforeEach, describe, expect, it } from 'vitest';
import { at, createEnvelope, createDelivery, LEASE_MS } from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/** Ordering under load, requeued keyed dead letters, lease semantics and data fidelity. */
export function orderingAndLeasesContract<TTransaction>(suite: ContractSuite<TTransaction>): void {
  const { append, claim } = suite;
  let store!: EventStore<TTransaction>;

  beforeEach(() => {
    store = suite.store();
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
}
