import type { EventStore } from '@relaybus/core';
import { beforeEach, describe, expect, it } from 'vitest';
import { at, createEnvelope, createDelivery, LEASE_MS } from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/** Completion, stale leases of one delivery and retention of the deduplication record. */
export function completionContract<TTransaction>(suite: ContractSuite<TTransaction>): void {
  const { append, claim, completeMany, keepsCompleted } = suite;
  let store!: EventStore<TTransaction>;

  beforeEach(() => {
    store = suite.store();
  });

  describe('completion', () => {
    it('removes a completed delivery from every listing', async () => {
      const event = createEnvelope();
      await append([
        { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
      ]);
      const [delivery] = await claim(['handler-a']);
      expect(await store.complete(delivery!, at(5))).toBe(true);
      const records = await store.listDeliveries({ eventId: event.id });
      if (keepsCompleted) {
        expect(records.map((record) => [record.handler, record.status]).sort()).toEqual([
          ['handler-a', 'completed'],
          ['handler-b', 'pending'],
        ]);
        const [completed] = await store.listDeliveries({ status: 'completed' });
        expect(completed).toMatchObject({ handler: 'handler-a', attempts: 1 });
        expect(completed!.completedAt).toEqual(at(5));
      } else {
        expect(records.map((record) => [record.handler, record.status])).toEqual([
          ['handler-b', 'pending'],
        ]);
        expect(await store.listDeliveries({ status: 'completed' })).toEqual([]);
      }
      expect(await claim(['handler-a'], at(LEASE_MS * 10))).toEqual([]);
      expect(await store.statistics(at(0))).toMatchObject({
        pending: 1,
        processing: 0,
        completed: keepsCompleted ? 1 : 0,
      });
    });

    it('does not create the delivery of a handler that already completed again', async () => {
      const event = createEnvelope();
      await append([{ event, deliveries: [createDelivery('handler-a')] }]);
      const [delivery] = await claim(['handler-a']);
      await store.complete(delivery!, at(1));
      // A broker redelivery or a publish retry carries the same event id: handler-a must not run twice.
      const result = await append([
        { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
      ]);
      expect(result).toEqual({
        appendedEventIds: [],
        duplicateEventIds: [event.id],
        appendedDeliveryCount: 1,
      });
      expect(await claim(['handler-a'], at(LEASE_MS * 10))).toEqual([]);
      expect(await claim(['handler-b'], at(LEASE_MS * 10))).toHaveLength(1);
      const again = await append([{ event, deliveries: [createDelivery('handler-a')] }]);
      expect(again.appendedDeliveryCount).toBe(0);
    });

    it('completes many leases at once and reports each one', async () => {
      await append(
        Array.from({ length: 5 }, () => ({
          event: createEnvelope(),
          deliveries: [createDelivery('handler-a')],
        })),
      );
      const claimed = await claim(['handler-a']);
      expect(claimed).toHaveLength(5);
      expect(await completeMany(store, [], at(1))).toEqual([]);
      expect(await completeMany(store, claimed.slice(0, 3), at(1))).toEqual([true, true, true]);
      expect(await store.statistics(at(2))).toMatchObject({ processing: 2, pending: 0 });
      expect(await completeMany(store, claimed, at(2))).toEqual([false, false, false, true, true]);
      expect(await store.statistics(at(3))).toMatchObject({ processing: 0, pending: 0 });
    });

    it('completes a lease listed twice only once', async () => {
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
      const [delivery] = await claim(['handler-a']);
      expect(await completeMany(store, [delivery!, delivery!], at(1))).toEqual([true, false]);
    });

    it('fences every lease of a batch on its own', async () => {
      await append(
        Array.from({ length: 3 }, () => ({
          event: createEnvelope(),
          deliveries: [createDelivery('handler-a')],
        })),
      );
      const stale = await claim(['handler-a'], at(0));
      const current = await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
      expect(current).toHaveLength(3);
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }], at(1));
      const [fresh] = await claim(['handler-a'], at(LEASE_MS + 1), 100, 'worker-2');
      expect(
        await completeMany(store, [stale[0]!, fresh!, stale[1]!, current[2]!], at(LEASE_MS + 2)),
      ).toEqual([false, true, false, true]);
      expect(await store.statistics(at(LEASE_MS + 3))).toMatchObject({ processing: 2 });
    });

    it('does not complete a lease of a delivery that went back to pending', async () => {
      const error = { name: 'Error', message: 'x' };
      await append([
        { event: createEnvelope(), deliveries: [createDelivery('a'), createDelivery('b')] },
      ]);
      const [retrying] = await claim(['a']);
      const [released] = await claim(['b']);
      await store.reschedule(retrying!, at(1_000), error, at(1));
      await store.release(released!, at(1));
      expect(await completeMany(store, [retrying!, released!], at(2))).toEqual([false, false]);
      expect(await store.statistics(at(2))).toMatchObject({ pending: 2, processing: 0 });
    });
  });

  describe('stale leases of one delivery', () => {
    const error = { name: 'Error', message: 'x' };

    it('refuses the lease of an attempt that was released when the same owner claims it again', async () => {
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
      const [stale] = await claim(['handler-a'], at(0), 100, 'worker-1');
      expect(await store.release(stale!, at(1))).toBe(true);
      const [current] = await claim(['handler-a'], at(2), 100, 'worker-1');
      expect(current!.deliveryId).toBe(stale!.deliveryId);
      expect(current!.leaseToken).not.toBe(stale!.leaseToken);
      expect(await store.complete(stale!, at(3))).toBe(false);
      expect(await store.bury(stale!, error, at(3))).toBe(false);
      expect(await store.reschedule(stale!, at(9), error, at(3))).toBe(false);
      expect(await store.release(stale!, at(3))).toBe(false);
      expect(await store.extendLease(stale!, at(LEASE_MS * 3), at(3))).toBe(false);
      expect(await store.complete(current!, at(4))).toBe(true);
    });

    it('refuses the lease of an attempt that was rescheduled when the same owner claims it again', async () => {
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
      const [stale] = await claim(['handler-a'], at(0), 100, 'worker-1');
      expect(await store.reschedule(stale!, at(10), error, at(1))).toBe(true);
      const [current] = await claim(['handler-a'], at(10), 100, 'worker-1');
      expect(current).toMatchObject({ deliveryId: stale!.deliveryId, attempt: 2 });
      expect(await store.complete(stale!, at(11))).toBe(false);
      expect(await store.bury(stale!, error, at(11))).toBe(false);
      expect(await completeMany(store, [stale!], at(11))).toEqual([false]);
      expect(await store.complete(current!, at(12))).toBe(true);
    });

    it('matches the results of completeMany to the lease, not to the delivery id', async () => {
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
      const [stale] = await claim(['handler-a'], at(0), 100, 'worker-1');
      await store.release(stale!, at(1));
      const [current] = await claim(['handler-a'], at(2), 100, 'worker-1');
      expect(await completeMany(store, [stale!, current!], at(3))).toEqual([false, true]);
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-b')] }]);
      const [staleB] = await claim(['handler-b'], at(4), 100, 'worker-1');
      await store.release(staleB!, at(5));
      const [currentB] = await claim(['handler-b'], at(6), 100, 'worker-1');
      expect(await completeMany(store, [currentB!, staleB!], at(7))).toEqual([true, false]);
    });
  });

  describe('retention of the deduplication record', () => {
    const DAY = 86_400_000;

    it('keeps an event for the retention window after its last delivery completed, however old it is', async () => {
      const event = createEnvelope();
      await append([{ event, deliveries: [createDelivery('handler-a')] }]);
      const [delivery] = await claim(['handler-a'], at(30 * DAY));
      expect(await store.complete(delivery!, at(30 * DAY + 1))).toBe(true);
      // The event is 30 days old but completed just now: a 29 day completed retention must not take it.
      expect(await store.purge({ completedBefore: at(29 * DAY) })).toMatchObject({
        deletedEvents: 0,
      });
      const again = await append(
        [{ event, deliveries: [createDelivery('handler-a')] }],
        at(30 * DAY + 2),
      );
      expect(again.appendedDeliveryCount).toBe(0);
      expect(await store.purge({ completedBefore: at(31 * DAY) })).toMatchObject({
        deletedEvents: 1,
      });
      const afterwards = await append(
        [{ event, deliveries: [createDelivery('handler-a')] }],
        at(31 * DAY),
      );
      expect(afterwards.appendedEventIds).toEqual([event.id]);
    });

    it('keeps the later completion time of an event when completions are recorded out of order', async () => {
      const event = createEnvelope();
      await append([
        { event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] },
      ]);
      const claimed = await claim(['handler-a', 'handler-b']);
      expect(claimed).toHaveLength(2);
      const [first, second] = claimed.sort((left, right) =>
        left.handler.localeCompare(right.handler),
      );
      expect(await store.complete(first!, at(200))).toBe(true);
      // A worker with a slow clock records an earlier time afterwards: it must not move the event back.
      expect(await store.complete(second!, at(100))).toBe(true);
      expect(await store.purge({ completedBefore: at(150) })).toMatchObject({ deletedEvents: 0 });
      const again = await append(
        [{ event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] }],
        at(201),
      );
      expect(again.appendedDeliveryCount).toBe(0);
    });

    it('remembers every handler of an event appended twice in one transaction', async () => {
      const event = createEnvelope();
      const result = await store.transaction(async (transaction) => {
        await store.append(
          transaction,
          [{ event, deliveries: [createDelivery('handler-a')] }],
          at(0),
        );
        return store.append(
          transaction,
          [{ event, deliveries: [createDelivery('handler-b')] }],
          at(0),
        );
      });
      expect(result).toMatchObject({ duplicateEventIds: [event.id], appendedDeliveryCount: 1 });
      const claimed = await claim(['handler-a', 'handler-b']);
      expect(claimed).toHaveLength(2);
      for (const delivery of claimed) {
        expect(await store.complete(delivery, at(1))).toBe(true);
      }
      const again = await append(
        [{ event, deliveries: [createDelivery('handler-a'), createDelivery('handler-b')] }],
        at(2),
      );
      expect(again).toMatchObject({ duplicateEventIds: [event.id], appendedDeliveryCount: 0 });
    });

    it('does not let the dead-letter cutoff remove events whose deliveries completed', async () => {
      const event = createEnvelope();
      await append([{ event, deliveries: [createDelivery('handler-a')] }]);
      const [delivery] = await claim(['handler-a']);
      await store.complete(delivery!, at(1));
      expect(await store.purge({ deadBefore: at(100 * DAY) })).toMatchObject({
        deletedEvents: 0,
      });
      const again = await append([{ event, deliveries: [createDelivery('handler-a')] }], at(2));
      expect(again.appendedDeliveryCount).toBe(0);
    });
  });
}
