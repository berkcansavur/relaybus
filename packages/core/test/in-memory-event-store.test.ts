import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConfigurationError, InMemoryEventStore, InMemoryTransaction } from '@relaybus/core';
import { at, createDelivery, createEnvelope, eventStoreContract } from '@relaybus/testing';

eventStoreContract('InMemoryEventStore', {
  createStore: async () => new InMemoryEventStore(),
  // One process holds the only copy of the data, so the "peer" is the same instance.
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
