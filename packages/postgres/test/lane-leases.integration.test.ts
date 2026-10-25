import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { PoolClient } from 'pg';
import { at, createEnvelope } from '@relaybus/testing';
import {
  PostgresEventStore,
  type PostgresExecutor,
  type PostgresParameter,
  type PostgresRow,
} from '../src/index.js';
import { LEASE_MS } from './support/claim.js';
import { createLaneHelpers, error } from './support/lane-helpers.js';
import { startDatabase, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { append, claim, createStore } = createLaneHelpers(() => database);

describe('claiming does not write delivery rows', () => {
  it('leaves the tuple of a claimed delivery untouched and updates only HOT lane rows', async () => {
    const { store, schema } = await createStore();
    await append(
      store,
      Array.from({ length: 20 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const before = await database.pool.query(
      `SELECT id, xmin::text, ctid::text FROM "${schema}".relaybus_deliveries ORDER BY id`,
    );

    const client = await database.pool.connect();
    try {
      await client.query('BEGIN');
      const oneConnection: PostgresExecutor<PoolClient> = {
        transaction: (work) => work(client),
        query: async (sql, parameters: readonly PostgresParameter[]) => {
          const result = await client.query<PostgresRow>(sql, [...parameters]);
          return result.rows;
        },
      };
      const tracked = new PostgresEventStore({ executor: oneConnection, schema });
      const claimed = await claim(tracked, ['h'], 'worker-1', at(0), 20);
      expect(claimed).toHaveLength(20);
      expect(await tracked.extendLease(claimed[0]!, at(LEASE_MS * 2))).toBe(true);
      const midway = await client.query(
        `SELECT id, xmin::text, ctid::text FROM "${schema}".relaybus_deliveries ORDER BY id`,
      );
      expect(midway.rows).toEqual(before.rows);
      expect(await tracked.completeMany(claimed, at(2))).toEqual(claimed.map(() => true));
      const stats = await client.query<{ relname: string; updated: string; hot: string }>(
        `SELECT c.relname, pg_stat_get_xact_tuples_updated(c.oid)::text AS updated,
                pg_stat_get_xact_tuples_hot_updated(c.oid)::text AS hot
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname IN ('relaybus_deliveries', 'relaybus_lanes')`,
        [schema],
      );
      const byTable = Object.fromEntries(stats.rows.map((row) => [row.relname, row]));
      expect(byTable['relaybus_deliveries']!.updated).toBe('0');
      expect(Number(byTable['relaybus_lanes']!.updated)).toBeGreaterThan(0);
      expect(byTable['relaybus_lanes']!.hot).toBe(byTable['relaybus_lanes']!.updated);
    } finally {
      await client.query('ROLLBACK');
      client.release();
    }
    const remaining = await database.pool.query(
      `SELECT count(*)::int AS count FROM "${schema}".relaybus_deliveries`,
    );
    expect(remaining.rows[0]).toEqual({ count: 20 });
  });
});

describe('lane leases', () => {
  async function sameLaneTwoHandlers() {
    const { store, schema } = await createStore();
    await append(store, [{ event: createEnvelope({ key: 'shared' }), handlers: ['h1', 'h2'] }]);
    return { store, schema };
  }

  it('keeps the deliveries of a lane away from other owners while the lease is live', async () => {
    const { store } = await sameLaneTwoHandlers();
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    expect(await claim(store, ['h2'], 'worker-2', at(LEASE_MS - 1))).toEqual([]);
    expect(await claim(store, ['h2'], 'worker-1', at(1))).toHaveLength(1);
  });

  it('gives the lane to another owner once the lease expired and fences the old epoch', async () => {
    const { store, schema } = await sameLaneTwoHandlers();
    const [first] = await claim(store, ['h1'], 'worker-1');
    const [second] = await claim(store, ['h2'], 'worker-2', at(LEASE_MS));
    expect(second).toMatchObject({ handler: 'h2', attempt: 1 });
    const [firstLane, firstEpoch] = first!.leaseToken.split(':');
    const [secondLane, secondEpoch] = second!.leaseToken.split(':');
    expect(secondLane).toBe(firstLane);
    expect(Number(secondEpoch)).toBe(Number(firstEpoch) + 1);
    expect(await store.complete(first!, at(LEASE_MS + 1))).toBe(false);
    const lanes = await database.pool.query(
      `SELECT lease_owner, in_flight FROM "${schema}".relaybus_lanes WHERE lane = $1`,
      [Number(firstLane)],
    );
    expect(lanes.rows[0]).toEqual({ lease_owner: 'worker-2', in_flight: [second!.deliveryId] });
  });

  it('charges a lost attempt only to deliveries that were still running', async () => {
    const { store } = await createStore();
    await append(store, [{ event: createEnvelope({ key: 'k' }), handlers: ['a', 'b', 'c'] }]);
    const [running, retrying, finished] = (await claim(store, ['a', 'b', 'c'], 'worker-1')).sort(
      (left, right) => left.handler.localeCompare(right.handler),
    );
    await store.reschedule(retrying!, at(1_000), error, at(1));
    await store.complete(finished!, at(2));
    const taken = await claim(store, ['a', 'b', 'c'], 'worker-2', at(LEASE_MS + 1_000));
    expect(
      Object.fromEntries(taken.map((delivery) => [delivery.handler, delivery.attempt])),
    ).toEqual({ a: 2, b: 2 });
    expect(running!.handler).toBe('a');
  });

  it('does not let a completion and a takeover both win', async () => {
    const { store, schema } = await sameLaneTwoHandlers();
    const [stale] = await claim(store, ['h1'], 'worker-1');
    const peer = new PostgresEventStore({ pool: database.pool, schema });
    const outcome = await store.transaction(async (transaction) => {
      expect(await store.complete(stale!, at(LEASE_MS + 1), transaction)).toBe(true);
      // The transaction holds the lane row, so the takeover skips the lane instead of waiting.
      const claimedMeanwhile = await claim(peer, ['h2'], 'worker-2', at(LEASE_MS));
      return claimedMeanwhile;
    });
    expect(outcome).toEqual([]);
    // After the commit the delivery is gone and the lane can be taken over.
    const [second] = await claim(peer, ['h2'], 'worker-2', at(LEASE_MS + 2));
    expect(second).toMatchObject({ handler: 'h2' });
    expect(await store.statistics(at(LEASE_MS + 3))).toMatchObject({ processing: 1 });
  });

  it('lets a takeover proceed and redelivers when the completing transaction rolls back', async () => {
    const { store, schema } = await sameLaneTwoHandlers();
    const [stale] = await claim(store, ['h1'], 'worker-1');
    const peer = new PostgresEventStore({ pool: database.pool, schema });
    await expect(
      store.transaction(async (transaction) => {
        await store.complete(stale!, at(LEASE_MS + 1), transaction);
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    const taken = await claim(peer, ['h1', 'h2'], 'worker-2', at(LEASE_MS + 2));
    expect(taken.map((delivery) => [delivery.handler, delivery.attempt]).sort()).toEqual([
      ['h1', 2],
      ['h2', 1],
    ]);
  });

  it('spreads a backlog over few lanes per worker and over all workers', async () => {
    const { store, schema } = await createStore();
    await append(
      store,
      Array.from({ length: 600 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const first = await claim(store, ['h'], 'worker-1', at(0), 10);
    expect(first).toHaveLength(10);
    const held = await database.pool.query(
      `SELECT count(*)::int AS count FROM "${schema}".relaybus_lanes WHERE lease_owner = 'worker-1'`,
    );
    expect(held.rows[0].count).toBeLessThanOrEqual(12);
    const second = await claim(store, ['h'], 'worker-2', at(0), 300);
    const third = await claim(store, ['h'], 'worker-3', at(0), 300);
    const rest = await claim(store, ['h'], 'worker-1', at(0), 300);
    const ids = [...first, ...second, ...third, ...rest].map((delivery) => delivery.deliveryId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(second).toHaveLength(300);
    expect(ids.length).toBeGreaterThan(300 + 10);
  });

  it('hands lanes back when a claim finds nothing to run', async () => {
    const { store, schema } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1');
    await store.complete(delivery!, at(1));
    expect(await claim(store, ['h'], 'worker-1', at(2))).toEqual([]);
    const held = await database.pool.query(
      `SELECT count(*)::int AS count FROM "${schema}".relaybus_lanes WHERE lease_owner IS NOT NULL`,
    );
    expect(held.rows[0]).toEqual({ count: 0 });
  });

  it('frees a lane whose deliveries all went back to pending', async () => {
    const { store, schema } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1');
    await store.release(delivery!, at(1));
    // Not yet due for anyone else's attempt count: the next claim of the owner finds it again.
    expect(await claim(store, ['other'], 'worker-1', at(2))).toEqual([]);
    const held = await database.pool.query(
      `SELECT count(*)::int AS count FROM "${schema}".relaybus_lanes WHERE lease_owner IS NOT NULL`,
    );
    expect(held.rows[0]).toEqual({ count: 0 });
    expect(await claim(store, ['h'], 'worker-2', at(3))).toHaveLength(1);
  });

  it('shows running deliveries as processing with the lease of their lane', async () => {
    const { store } = await createStore();
    await append(store, [
      { event: createEnvelope(), handlers: ['running'] },
      { event: createEnvelope(), handlers: ['waiting'] },
    ]);
    await claim(store, ['running'], 'worker-1', at(5));
    const processing = await store.listDeliveries({ status: 'processing' });
    expect(processing).toHaveLength(1);
    expect(processing[0]).toMatchObject({
      handler: 'running',
      attempts: 1,
      leaseOwner: 'worker-1',
    });
    expect(processing[0]!.leaseExpiresAt).toEqual(at(5 + LEASE_MS));
    const pending = await store.listDeliveries({ status: 'pending' });
    expect(pending.map((record) => [record.handler, record.attempts, record.leaseOwner])).toEqual([
      ['waiting', 0, null],
    ]);
  });
});

describe('unissued lease tokens', () => {
  it.each(['not-a-token', '999:1', '3:x', '3:', '-1:2', '7:123456789012345678901', ''])(
    'are refused by every operation: %j',
    async (leaseToken) => {
      const { store } = await createStore();
      await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
      const [real] = await claim(store, ['h'], 'worker-1');
      const lease = { deliveryId: real!.deliveryId, leaseToken };
      expect(await store.extendLease(lease, at(LEASE_MS * 2))).toBe(false);
      expect(await store.complete(lease, at(1))).toBe(false);
      expect(await store.completeMany([lease, real!], at(1))).toEqual([false, true]);
      expect(await store.reschedule(lease, at(5), error, at(1))).toBe(false);
      expect(await store.bury(lease, error, at(1))).toBe(false);
      expect(await store.release(lease, at(1))).toBe(false);
    },
  );
});

describe('claiming behind a clogged window', () => {
  it('finds unrelated deliveries behind a long backlog of one blocked key', async () => {
    const { store } = await createStore();
    await append(
      store,
      Array.from({ length: 300 }, () => ({
        event: createEnvelope({ key: 'hot' }),
        handlers: ['h'],
      })),
    );
    await append(
      store,
      [
        ...Array.from({ length: 5 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
        ...Array.from({ length: 5 }, (_, index) => ({
          event: createEnvelope({ key: `other-${index}` }),
          handlers: ['h'],
        })),
      ],
      at(10),
    );
    const claimed = await claim(store, ['h'], 'worker-1', at(100), 20);
    expect(claimed).toHaveLength(11);
    expect(claimed.filter((delivery) => delivery.event.key === 'hot')).toHaveLength(1);
    expect(claimed[0]!.event.key).toBe('hot');
  });
});

describe('completion history', () => {
  it('lists, filters, counts and purges completed deliveries', async () => {
    const { store } = await createStore({ completionHistory: true, purgeBatchSize: 2 });
    const kept = createEnvelope({ type: 'kept.v1' });
    await append(store, [
      { event: kept, handlers: ['a', 'b'] },
      ...Array.from({ length: 4 }, () => ({ event: createEnvelope(), handlers: ['a'] })),
    ]);
    const claimed = await claim(store, ['a'], 'worker-1');
    expect(claimed).toHaveLength(5);
    await store.completeMany(claimed, at(7));
    expect(await store.statistics(at(8))).toMatchObject({ completed: 5, pending: 1 });
    const completed = await store.listDeliveries({ status: 'completed', limit: 3 });
    expect(completed).toHaveLength(3);
    expect(completed[0]).toMatchObject({ status: 'completed', attempts: 1, leaseOwner: null });
    expect(completed[0]!.completedAt).toEqual(at(7));
    expect(await store.listDeliveries({ eventType: 'kept.v1' })).toHaveLength(2);
    expect(await store.listDeliveries({ handler: 'a', status: 'pending' })).toEqual([]);
    expect(await store.listDeliveries({ eventId: kept.id, status: 'completed' })).toHaveLength(1);
    expect(await store.purge({ completedBefore: at(7) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 0,
    });
    // Four events lose their only record; the shared one keeps its pending delivery of handler b.
    expect(await store.purge({ completedBefore: at(8) })).toEqual({
      deletedDeliveries: 5,
      deletedEvents: 4,
    });
    expect(await store.listDeliveries({})).toHaveLength(1);
  });

  it('keeps an event while a completion record references it', async () => {
    const { store } = await createStore({ completionHistory: true });
    await append(store, [{ event: createEnvelope(), handlers: ['a'] }]);
    const [delivery] = await claim(store, ['a'], 'worker-1');
    await store.complete(delivery!, at(1));
    // The sweep only looks at events without deliveries, and a record counts as a reference.
    expect(await store.purge({ deadBefore: at(1_000) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 0,
    });
    expect(await store.listDeliveries({ status: 'completed' })).toHaveLength(1);
  });
});
