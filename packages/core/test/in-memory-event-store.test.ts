import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConfigurationError, InMemoryEventStore, InMemoryTransaction } from '@relaybus/core';
import { at, createDelivery, createEnvelope, eventStoreContract } from '@relaybus/testing';

eventStoreContract('InMemoryEventStore', {
  createStore: async () => new InMemoryEventStore(),
  // One process holds the only copy of the data, so the "peer" is the same instance.
  createPeerStore: async (primary) => primary,
});

eventStoreContract('InMemoryEventStore with completion history', {
  createStore: async () => new InMemoryEventStore({ retainCompleted: true }),
  createPeerStore: async (primary) => primary,
  keepsCompletedRecords: true,
});

// A store without the optional port methods must pass the contract too (the suite completes one by one).
eventStoreContract('InMemoryEventStore without completeMany', {
  createStore: async () => {
    const store = new InMemoryEventStore();
    Object.defineProperty(store, 'completeMany', { value: undefined });
    return store;
  },
  createPeerStore: async (primary) => primary,
});

describe('InMemoryEventStore', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it('refuses to start in production unless allowed', () => {
    vi.stubEnv('NODE_ENV', 'production');
    expect(() => new InMemoryEventStore()).toThrow(ConfigurationError);
    expect(() => new InMemoryEventStore({ allowInProduction: true })).not.toThrow();
  });

  it('uses the injected identifier generator for lease tokens', async () => {
    const store = new InMemoryEventStore({ generateIdentifier: () => 'token-1' });
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event: createEnvelope(), deliveries: [createDelivery('a')] }],
        at(0),
      ),
    );
    const [claimed] = await store.claim({
      handlers: ['a'],
      limit: 1,
      leaseOwner: 'owner',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    expect(claimed!.leaseToken).toBe('token-1');
  });

  it('applies a transactional completion only if the lease is still held at commit', async () => {
    const store = new InMemoryEventStore();
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event: createEnvelope(), deliveries: [createDelivery('a')] }],
        at(0),
      ),
    );
    const request = {
      handlers: ['a'],
      limit: 1,
      leaseOwner: 'owner',
      now: at(0),
      leaseExpiresAt: at(10),
    };
    const [stale] = await store.claim(request);
    await store.transaction(async (transaction) => {
      expect(await store.complete(stale!, at(1), transaction)).toBe(true);
      await store.claim({ ...request, now: at(10), leaseExpiresAt: at(20) });
    });
    expect((await store.listDeliveries({}))[0]!.status).toBe('processing');
  });
});

describe('InMemoryEventStore locks', () => {
  it('lets one transaction write the same key twice', async () => {
    const store = new InMemoryEventStore();
    await store.transaction(async (transaction) => {
      await store.append(
        transaction,
        [{ event: createEnvelope({ key: 'k' }), deliveries: [createDelivery('a')] }],
        at(0),
      );
      await store.append(
        transaction,
        [{ event: createEnvelope({ key: 'k' }), deliveries: [createDelivery('a')] }],
        at(0),
      );
    });
    expect(await store.listDeliveries({})).toHaveLength(2);
  });

  it('releases locks when a transaction rolls back', async () => {
    const store = new InMemoryEventStore();
    const event = createEnvelope({ key: 'k' });
    await expect(
      store.transaction(async (transaction) => {
        await store.append(transaction, [{ event, deliveries: [createDelivery('a')] }], at(0));
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    const result = await store.transaction((transaction) =>
      store.append(transaction, [{ event, deliveries: [createDelivery('a')] }], at(0)),
    );
    expect(result.appendedEventIds).toEqual([event.id]);
  });
});

describe('InMemoryTransaction', () => {
  it('rejects writes after it finished', () => {
    const committed = new InMemoryTransaction();
    committed.commit();
    expect(committed.state).toBe('committed');
    expect(() => committed.enqueue(() => undefined)).toThrow(/committed/);
    const rolledBack = new InMemoryTransaction();
    rolledBack.rollback();
    expect(rolledBack.state).toBe('rolled-back');
    expect(() => rolledBack.enqueue(() => undefined)).toThrow(/rolled-back/);
  });
});

describe('InMemoryEventStore completion', () => {
  const lease = (deliveryId: string, leaseToken: string) => ({ deliveryId, leaseToken });

  async function claimOne(store: InMemoryEventStore) {
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event: createEnvelope(), deliveries: [createDelivery('a')] }],
        at(0),
      ),
    );
    const [claimed] = await store.claim({
      handlers: ['a'],
      limit: 1,
      leaseOwner: 'w',
      leaseExpiresAt: at(1_000),
      now: at(0),
    });
    return claimed!;
  }

  it('deletes a completed delivery unless a history is kept', async () => {
    const store = new InMemoryEventStore();
    const claimed = await claimOne(store);
    expect(await store.complete(claimed, at(1))).toBe(true);
    expect(await store.listDeliveries({})).toEqual([]);
    expect(await store.statistics(at(2))).toMatchObject({ completed: 0, pending: 0 });
  });

  it('keeps a completed record when asked to', async () => {
    const store = new InMemoryEventStore({ retainCompleted: true });
    const claimed = await claimOne(store);
    expect(await store.completeMany([claimed, lease('missing', 'x')], at(1))).toEqual([
      true,
      false,
    ]);
    expect(await store.listDeliveries({ status: 'completed' })).toHaveLength(1);
  });

  it('removes events no delivery references once they are older than the cutoff', async () => {
    const store = new InMemoryEventStore();
    const claimed = await claimOne(store);
    await store.complete(claimed, at(1));
    await store.transaction((transaction) =>
      store.append(transaction, [{ event: createEnvelope(), deliveries: [] }], at(5_000)),
    );
    expect(await store.purge({})).toEqual({ deletedDeliveries: 0, deletedEvents: 0 });
    expect(await store.purge({ completedBefore: at(2_000), deadBefore: at(9_000) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 1,
    });
    expect(await store.purge({ completedBefore: at(9_000) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 1,
    });
  });
});

describe('InMemoryEventStore append that fails to serialize', () => {
  it('lets the caller append the corrected event under the same id in the same transaction', async () => {
    const store = new InMemoryEventStore();
    const event = createEnvelope();
    const broken = { ...event, payload: { amount: 1n } } as unknown as typeof event;
    const result = await store.transaction(async (transaction) => {
      await expect(
        store.append(transaction, [{ event: broken, deliveries: [createDelivery('h')] }], at(0)),
      ).rejects.toThrow();
      return store.append(transaction, [{ event, deliveries: [createDelivery('h')] }], at(0));
    });
    expect(result).toMatchObject({ appendedEventIds: [event.id], appendedDeliveryCount: 1 });
    expect(await store.statistics(at(0))).toMatchObject({ pending: 1 });
  });
});

describe('InMemoryEventStore purge next to an open append', () => {
  it('keeps an event that an open transaction is adding a handler to', async () => {
    const store = new InMemoryEventStore();
    const event = createEnvelope();
    await store.transaction((transaction) =>
      store.append(transaction, [{ event, deliveries: [] }], at(0)),
    );
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const appending = store.transaction(async (transaction) => {
      await store.append(transaction, [{ event, deliveries: [createDelivery('a')] }], at(1));
      await gate;
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    // The event is an old orphan, but a transaction that has not committed yet is about to attach a delivery.
    expect(await store.purge({ completedBefore: at(1_000) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 0,
    });
    release();
    await appending;
    expect(await store.listDeliveries({ eventId: event.id })).toHaveLength(1);
  });
});
