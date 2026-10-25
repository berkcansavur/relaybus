import { createUuidV7, type ClaimedDelivery, type EventStore } from '@relaybus/core';
import { beforeEach, describe, expect, it } from 'vitest';
import { at, createEnvelope, createDelivery, LEASE_MS } from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/** Setup, append and claim. */
export function appendAndClaimContract<TTransaction>(suite: ContractSuite<TTransaction>): void {
  const { append, claim } = suite;
  let store!: EventStore<TTransaction>;

  beforeEach(() => {
    store = suite.store();
  });

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
      expect(records.every((record) => record.status === 'pending' && record.attempts === 0)).toBe(
        true,
      );
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
      // A store may lease groups of deliveries to one claimer at a time, so a claimer that
      // keeps claiming is what drains the workload; whatever nobody holds is left for a late one.
      const batches = await Promise.all(
        Array.from({ length: 8 }, async (_, index) => {
          const own: ClaimedDelivery[] = [];
          for (;;) {
            const batch = await claim(['handler-a'], at(0), 10, `worker-${index}`);
            if (batch.length === 0) {
              return own;
            }
            own.push(...batch);
          }
        }),
      );
      const leftovers = await claim(['handler-a'], at(0), 100, 'worker-late');
      const claimedIds = [...batches.flat(), ...leftovers].map((delivery) => delivery.deliveryId);
      expect(new Set(claimedIds).size).toBe(claimedIds.length);
      expect(claimedIds).toHaveLength(40);
    });
  });
}
