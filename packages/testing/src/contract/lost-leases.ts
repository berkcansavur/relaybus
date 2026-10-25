import type { EventStore } from '@relaybus/core';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { at, createEnvelope, createDelivery, LEASE_MS } from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/** Lost leases, relinquish and stores across instances. */
export function lostLeasesContract<TTransaction>(suite: ContractSuite<TTransaction>): void {
  const { append, claim, completeMany, options } = suite;
  let store!: EventStore<TTransaction>;

  beforeEach(() => {
    store = suite.store();
  });

  describe('lost leases', () => {
    it('hands a delivery of a crashed worker to the next claimer with the attempt counted', async () => {
      await append(
        Array.from({ length: 4 }, () => ({
          event: createEnvelope(),
          deliveries: [createDelivery('handler-a')],
        })),
      );
      const first = await claim(['handler-a'], at(0), 100, 'worker-1');
      expect(first.map((delivery) => delivery.attempt)).toEqual([1, 1, 1, 1]);
      expect(await claim(['handler-a'], at(LEASE_MS - 1), 100, 'worker-2')).toEqual([]);
      const second = await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
      expect(second.map((delivery) => delivery.deliveryId).sort()).toEqual(
        first.map((delivery) => delivery.deliveryId).sort(),
      );
      expect(second.map((delivery) => delivery.attempt)).toEqual([2, 2, 2, 2]);
      for (const stale of first) {
        expect(await store.complete(stale, at(LEASE_MS + 1))).toBe(false);
      }
      expect(await completeMany(store, first, at(LEASE_MS + 1))).toEqual([
        false,
        false,
        false,
        false,
      ]);
      expect(await completeMany(store, second, at(LEASE_MS + 2))).toEqual([true, true, true, true]);
    });

    it('keeps counting attempts of a poison delivery until it is dead-lettered', async () => {
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
      for (let crash = 0; crash < 4; crash += 1) {
        const [delivery] = await claim(['handler-a'], at(crash * LEASE_MS), 100, `worker-${crash}`);
        expect(delivery!.attempt).toBe(crash + 1);
      }
      const [last] = await claim(['handler-a'], at(4 * LEASE_MS), 100, 'worker-4');
      expect(last!.attempt).toBe(5);
      expect(await store.bury(last!, { name: 'Error', message: 'poison' }, at(4 * LEASE_MS))).toBe(
        true,
      );
      const [dead] = await store.listDeliveries({ status: 'dead' });
      expect(dead).toMatchObject({ attempts: 5, lastError: { message: 'poison' } });
      expect(await claim(['handler-a'], at(100 * LEASE_MS))).toEqual([]);
    });

    it('does not count an attempt that was reported as failed twice', async () => {
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
      const [delivery] = await claim(['handler-a'], at(0), 100, 'worker-1');
      await store.reschedule(delivery!, at(10), { name: 'Error', message: 'x' }, at(1));
      const [retry] = await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
      expect(retry!.attempt).toBe(2);
      const [afterCrash] = await claim(['handler-a'], at(LEASE_MS * 2), 100, 'worker-3');
      expect(afterCrash!.attempt).toBe(3);
    });

    it('keeps the order of a key across a takeover', async () => {
      const [head, next] = await (async () => {
        const events = [createEnvelope({ key: 'k' }), createEnvelope({ key: 'k' })];
        for (const event of events) {
          await append([{ event, deliveries: [createDelivery('handler-a')] }]);
        }
        return events;
      })();
      const [stale] = await claim(['handler-a'], at(0), 100, 'worker-1');
      expect(stale!.event.id).toBe(head!.id);
      const taken = await claim(['handler-a'], at(LEASE_MS), 100, 'worker-2');
      expect(taken.map((delivery) => delivery.event.id)).toEqual([head!.id]);
      expect(await store.complete(stale!, at(LEASE_MS + 1))).toBe(false);
      expect(await store.complete(taken[0]!, at(LEASE_MS + 2))).toBe(true);
      const [following] = await claim(['handler-a'], at(LEASE_MS + 3), 100, 'worker-2');
      expect(following!.event.id).toBe(next!.id);
    });

    it('lets every worker see only deliveries nobody else holds, and none twice', async () => {
      await append(
        Array.from({ length: 30 }, () => ({
          event: createEnvelope(),
          deliveries: [createDelivery('handler-a')],
        })),
      );
      const seen: string[] = [];
      for (let round = 0; round < 40; round += 1) {
        const owners = ['worker-1', 'worker-2', 'worker-3'];
        const batches = await Promise.all(
          owners.map((owner) => claim(['handler-a'], at(round), 4, owner)),
        );
        for (const batch of batches) {
          seen.push(...batch.map((delivery) => delivery.deliveryId));
          expect(await completeMany(store, batch, at(round))).toEqual(batch.map(() => true));
        }
      }
      expect(seen).toHaveLength(30);
      expect(new Set(seen).size).toBe(30);
    });
  });

  describe('relinquish', () => {
    it('hands idle leases to other workers at once and keeps leases with running work', async (context) => {
      if (store.relinquish === undefined) {
        return context.skip();
      }
      await append([
        { event: createEnvelope(), deliveries: [createDelivery('handler-a')] },
        { event: createEnvelope(), deliveries: [createDelivery('handler-a')] },
      ]);
      const claimed = await claim(['handler-a'], at(0), 100, 'worker-1');
      expect(claimed).toHaveLength(2);
      await store.relinquish('worker-1', at(1));
      expect(await store.complete(claimed[0]!, at(2))).toBe(true);
      expect(await store.complete(claimed[1]!, at(2))).toBe(true);
      await store.relinquish('worker-1', at(3));
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }], at(4));
      expect(await claim(['handler-a'], at(4), 100, 'worker-2')).toHaveLength(1);
    });

    it('does not give away a delivery that is still running', async (context) => {
      if (store.relinquish === undefined) {
        return context.skip();
      }
      await append([{ event: createEnvelope(), deliveries: [createDelivery('handler-a')] }]);
      const [running] = await claim(['handler-a'], at(0), 100, 'worker-1');
      await store.relinquish('worker-1', at(1));
      expect(await claim(['handler-a'], at(2), 100, 'worker-2')).toEqual([]);
      expect(await store.complete(running!, at(3))).toBe(true);
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

    it('completes batches in either process and fences the one that lost its lease', async () => {
      await append(
        Array.from({ length: 6 }, () => ({
          event: createEnvelope(),
          deliveries: [createDelivery('handler-a')],
        })),
      );
      const request = (owner: string, now: Date) => ({
        handlers: ['handler-a'],
        limit: 100,
        leaseOwner: owner,
        leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
        now,
      });
      const stale = await store.claim(request('primary', at(0)));
      const current = await peer.claim(request('peer', at(LEASE_MS)));
      expect(current).toHaveLength(6);
      expect(await completeMany(store, stale, at(LEASE_MS + 1))).toEqual(stale.map(() => false));
      expect(await completeMany(peer, current, at(LEASE_MS + 1))).toEqual(current.map(() => true));
      expect(await store.statistics(at(LEASE_MS + 2))).toMatchObject({
        pending: 0,
        processing: 0,
      });
    });
  });
}
