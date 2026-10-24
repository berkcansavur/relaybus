import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { PoolClient } from 'pg';
import { createUuidV7, type ClaimedDelivery, type EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import {
  PgExecutor,
  PostgresEventStore,
  type PostgresExecutor,
  type PostgresParameter,
  type PostgresRow,
} from '../src/index.js';
import { createVersion010Schema } from './support/version-0.1.0-schema.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

const LEASE_MS = 30_000;

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

async function createStore(
  options: { completionHistory?: boolean; schema?: string; purgeBatchSize?: number } = {},
): Promise<{ store: PostgresEventStore; schema: string }> {
  const schema = options.schema ?? uniqueSchema();
  const store = new PostgresEventStore({ pool: database.pool, ...options, schema });
  await store.setup();
  return { store, schema };
}

async function append(
  store: PostgresEventStore,
  entries: Array<{ event: EventEnvelope; handlers: string[] }>,
  now: Date = at(0),
): Promise<void> {
  await store.transaction((transaction) =>
    store.append(
      transaction,
      entries.map(({ event, handlers }) => ({
        event,
        deliveries: handlers.map((handler) => createDelivery(handler)),
      })),
      now,
    ),
  );
}

function claim(
  store: PostgresEventStore,
  handlers: string[],
  owner: string,
  now: Date = at(0),
  limit = 100,
): Promise<ClaimedDelivery[]> {
  return store.claim({
    handlers,
    limit,
    leaseOwner: owner,
    leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
    now,
  });
}

const error = { name: 'Error', message: 'x' };

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

describe('upgrade from 0.1.0', () => {
  async function seedVersion010(schema: string) {
    await createVersion010Schema(database.pool, schema);
    const events = Array.from({ length: 5 }, (_, index) =>
      createEnvelope({ key: index < 2 ? 'order-1' : null }),
    );
    for (const [index, event] of events.entries()) {
      await database.pool.query(
        `INSERT INTO "${schema}".relaybus_events (id, type, source, key, sequence, payload, headers, occurred_at, created_at)
         VALUES ($1, 'contract.event.v1', 's', $2, $3, '{}', '{}', now(), $4)`,
        [event.id, event.key, event.key === null ? null : index + 1, at(0)],
      );
    }
    const rows = [
      {
        id: createUuidV7(),
        event: events[0]!,
        status: 'processing',
        attempts: 2,
        key: 'order-1',
        sequence: 1,
      },
      {
        id: createUuidV7(),
        event: events[1]!,
        status: 'pending',
        attempts: 0,
        key: 'order-1',
        sequence: 2,
      },
      {
        id: createUuidV7(),
        event: events[2]!,
        status: 'completed',
        attempts: 1,
        key: null,
        sequence: null,
      },
      {
        id: createUuidV7(),
        event: events[3]!,
        status: 'dead',
        attempts: 5,
        key: null,
        sequence: null,
      },
      {
        id: createUuidV7(),
        event: events[4]!,
        status: 'pending',
        attempts: 1,
        key: null,
        sequence: null,
      },
    ];
    for (const row of rows) {
      await database.pool.query(
        `INSERT INTO "${schema}".relaybus_deliveries
           (id, event_id, handler, status, attempts, available_at, lease_owner, lease_token, lease_expires_at, key, sequence, created_at, updated_at, completed_at)
         VALUES ($1, $2, 'h', $3, $4, $5, $6, $7, $8, $9, $10, $5, $5, $11)`,
        [
          row.id,
          row.event.id,
          row.status,
          row.attempts,
          at(0),
          row.status === 'processing' ? 'old-worker' : null,
          row.status === 'processing' ? 'old-token' : null,
          row.status === 'processing' ? at(LEASE_MS) : null,
          row.key,
          row.sequence,
          row.status === 'completed' ? at(1) : null,
        ],
      );
    }
    return rows;
  }

  it('converts the tables in place, keeps every delivery and can run again', async () => {
    const schema = uniqueSchema();
    const rows = await seedVersion010(schema);
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    await store.setup();

    const columns = await database.pool.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = 'relaybus_deliveries'`,
      [schema],
    );
    const names = columns.rows.map((row) => row.column_name);
    expect(names).toContain('lane');
    expect(names).not.toEqual(expect.arrayContaining(['lease_token']));
    expect(names).not.toEqual(expect.arrayContaining(['lease_owner']));
    expect(names).not.toEqual(expect.arrayContaining(['lease_expires_at']));
    const indexes = await database.pool.query<{ indexname: string }>(
      `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = 'relaybus_deliveries'`,
      [schema],
    );
    expect(indexes.rows.map((row) => row.indexname).sort()).toEqual([
      'relaybus_deliveries_completed_idx',
      'relaybus_deliveries_dead_idx',
      'relaybus_deliveries_event_handler_unique',
      'relaybus_deliveries_key_pending_idx',
      'relaybus_deliveries_lane_due_idx',
      'relaybus_deliveries_pkey',
    ]);

    // Every event remembers its handlers, so a handler that completed under 0.1.0 never runs again.
    const known = await database.pool.query<{ handlers: string[] }>(
      `SELECT handlers FROM "${schema}".relaybus_events WHERE id = $1`,
      [rows[2]!.event.id],
    );
    expect(known.rows[0]!.handlers).toEqual(['h']);
    const republished = await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event: rows[2]!.event, deliveries: [createDelivery('h'), createDelivery('other')] }],
        at(LEASE_MS),
      ),
    );
    expect(republished).toMatchObject({
      duplicateEventIds: [rows[2]!.event.id],
      appendedDeliveryCount: 1,
    });

    // The two deliveries of one key share a lane, as new deliveries of that key would.
    const lanes = await database.pool.query<{ id: string; lane: number }>(
      `SELECT id, lane FROM "${schema}".relaybus_deliveries WHERE key = 'order-1' ORDER BY sequence`,
    );
    expect(new Set(lanes.rows.map((row) => row.lane)).size).toBe(1);

    expect(await store.statistics(at(LEASE_MS * 2))).toMatchObject({
      pending: 4,
      processing: 0,
      completed: 1,
      dead: 1,
    });
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS * 2));
    // The delivery that was processing keeps the attempt it was already charged.
    expect(claimed.map((delivery) => [delivery.deliveryId, delivery.attempt])).toEqual(
      expect.arrayContaining([
        [rows[0]!.id, 3],
        [rows[4]!.id, 2],
      ]),
    );
    expect(claimed.map((delivery) => delivery.deliveryId)).not.toContain(rows[1]!.id);
    expect(await store.completeMany(claimed, at(LEASE_MS * 2 + 1))).toEqual(
      claimed.map(() => true),
    );
    // The follower of the key is next, and legacy completed rows are purged as before.
    const [follower] = await claim(store, ['h'], 'worker-1', at(LEASE_MS * 2 + 2));
    expect(follower!.deliveryId).toBe(rows[1]!.id);
    expect(await store.purge({ completedBefore: at(LEASE_MS * 3) })).toMatchObject({
      deletedDeliveries: 1,
    });
  });

  it('upgrades a schema that has a custom prefix and no deliveries', async () => {
    const schema = uniqueSchema();
    await createVersion010Schema(database.pool, schema);
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    expect(await store.statistics(at(0))).toMatchObject({ pending: 0, oldestDueAt: null });
  });
});

describe('a claim that takes free lanes', () => {
  async function laneOfKey(key: string): Promise<number> {
    const result = await database.pool.query<{ lane: number }>(
      `SELECT (hashtextextended($1::text, 0) & 255)::int AS lane`,
      [key],
    );
    return result.rows[0]!.lane;
  }

  it('claims the earliest due deliveries across lanes, also when an early lane holds a late one too', async () => {
    const { store } = await createStore();
    const sharedLane = await laneOfKey('early');
    let late = 'late-0';
    for (let index = 1; (await laneOfKey(late)) !== sharedLane; index += 1) {
      late = `late-${index}`;
    }
    let middle = 'middle-0';
    for (let index = 1; (await laneOfKey(middle)) === sharedLane; index += 1) {
      middle = `middle-${index}`;
    }
    const events = {
      late: createEnvelope({ key: late }),
      early: createEnvelope({ key: 'early' }),
      middle: createEnvelope({ key: middle }),
    };
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [
          { event: events.late, deliveries: [createDelivery('h', at(300))] },
          { event: events.early, deliveries: [createDelivery('h', at(100))] },
          { event: events.middle, deliveries: [createDelivery('h', at(200))] },
        ],
        at(0),
      ),
    );
    const first = await claim(store, ['h'], 'worker-1', at(1_000), 2);
    expect(first.map((delivery) => delivery.event.id)).toEqual([events.early.id, events.middle.id]);
  });
});

describe('free lanes whose due deliveries all wait behind a later head', () => {
  async function laneOfKey(key: string): Promise<number> {
    const result = await database.pool.query<{ lane: number }>(
      `SELECT (hashtextextended($1::text, 0) & 255)::int AS lane`,
      [key],
    );
    return result.rows[0]!.lane;
  }

  /** Keys in distinct lanes whose heads were retried an hour later, so their due followers are blocked. */
  async function blockLanes(store: PostgresEventStore, count: number): Promise<Set<number>> {
    const lanes = new Set<number>();
    const keys: string[] = [];
    for (let index = 0; keys.length < count; index += 1) {
      const lane = await laneOfKey(`blocked-${index}`);
      if (!lanes.has(lane)) {
        lanes.add(lane);
        keys.push(`blocked-${index}`);
      }
    }
    for (const key of keys) {
      await append(store, [
        { event: createEnvelope({ key }), handlers: ['h'] },
        { event: createEnvelope({ key }), handlers: ['h'] },
      ]);
    }
    const heads = await claim(store, ['h'], 'worker-0', at(0), 100);
    expect(heads).toHaveLength(count);
    for (const head of heads) {
      expect(await store.reschedule(head, at(3_600_000), error, at(0))).toBe(true);
    }
    return lanes;
  }

  async function runnableKey(blocked: Set<number>): Promise<string> {
    let key = 'runnable-0';
    for (let index = 1; blocked.has(await laneOfKey(key)); index += 1) {
      key = `runnable-${index}`;
    }
    return key;
  }

  it('passes over a free lane whose due deliveries are all blocked', async () => {
    const { store } = await createStore();
    const blocked = await blockLanes(store, 1);
    const event = createEnvelope({ key: await runnableKey(blocked) });
    await append(store, [{ event, handlers: ['h'] }]);
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS + 1), 1);
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([event.id]);
  });

  it('keeps looking when more blocked lanes come first than one statement examines', async () => {
    const { store } = await createStore();
    const blocked = await blockLanes(store, 17);
    const event = createEnvelope({ key: await runnableKey(blocked) });
    await append(store, [{ event, handlers: ['h'] }]);
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS + 1), 1);
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([event.id]);
  });
});

describe('a claim that needs more than the lanes it holds', () => {
  /** A store whose statements that take free lanes fail while `failing.on` is set. */
  async function createFailingStore(logger: { error: (message: string) => void }) {
    const schema = uniqueSchema();
    const inner = new PgExecutor(database.pool);
    const failing: { error: unknown } = { error: undefined };
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => inner.transaction(work),
      query: async (sql, parameters, transaction) => {
        if (failing.error !== undefined && sql.includes('AS free')) {
          // The second failure of the test is a string on purpose: stores must cope with any thrown value.
          throw failing.error as Error;
        }
        return inner.query(sql, parameters, transaction);
      },
    };
    const store = new PostgresEventStore({ executor, schema, logger: logger as never });
    await store.setup();
    return { store, failing };
  }

  it('returns what the first step recorded when taking more lanes fails, and throws when it recorded nothing', async () => {
    const errors: string[] = [];
    const { store, failing } = await createFailingStore({
      error: (message) => void errors.push(message),
    });
    await append(store, [
      { event: createEnvelope({ key: 'shared' }), handlers: ['h1', 'h2', 'h3'] },
    ]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    // A worker that crashed with deliveries running: taking its lane over is the transaction that fails.
    await append(store, [
      { event: createEnvelope({ key: 'crashed' }), handlers: ['h1', 'h2', 'h3'] },
    ]);
    const crashed = await store.claim({
      handlers: ['h1', 'h2', 'h3'],
      limit: 10,
      leaseOwner: 'crashed-worker',
      leaseExpiresAt: at(1),
      now: at(0),
    });
    expect(crashed).toHaveLength(3);
    failing.error = new Error('take failed');
    const claimed = await claim(store, ['h2', 'h3'], 'worker-1', at(1), 10);
    expect(claimed.map((delivery) => delivery.handler).sort()).toEqual(['h2', 'h3']);
    expect(errors).toEqual([
      expect.stringContaining('[PostgresEventStore](claim) Taking more lanes failed'),
    ]);
    await expect(claim(store, ['h1'], 'worker-2', at(2), 10)).rejects.toThrow('take failed');
    // A failure that is not an Error is reported by its text.
    failing.error = undefined;
    const rescued = await claim(store, ['h1', 'h2', 'h3'], 'rescuer', at(3), 10);
    expect(rescued.map((delivery) => delivery.attempt)).toEqual([2, 2, 2]);
    expect(await store.completeMany(rescued, at(3))).toEqual([true, true, true]);
    await append(store, [{ event: createEnvelope({ key: 'crashed-again' }), handlers: ['h2'] }]);
    const crashedAgain = await store.claim({
      handlers: ['h2'],
      limit: 1,
      leaseOwner: 'crashed-worker-2',
      leaseExpiresAt: at(4),
      now: at(0),
    });
    expect(crashedAgain.map((delivery) => delivery.event.key)).toEqual(['crashed-again']);
    await append(store, [{ event: createEnvelope({ key: 'again' }), handlers: ['h1', 'h2'] }]);
    expect(await claim(store, ['h1'], 'worker-3', at(5))).toHaveLength(1);
    failing.error = 'plain text failure';
    expect(await claim(store, ['h2'], 'worker-3', at(6), 10)).toHaveLength(1);
    expect(errors[1]).toContain('error=plain text failure');
  });

  it('looks behind the blocked followers of a lane it holds', async () => {
    const { store } = await createStore();
    await append(
      store,
      Array.from({ length: 100 }, () => ({
        event: createEnvelope({ key: 'hot' }),
        handlers: ['h'],
      })),
    );
    const [head] = await claim(store, ['h'], 'worker-1', at(1), 5);
    expect(head!.event.key).toBe('hot');
    // The lane is held, its first 64 due deliveries are all followers of the running head.
    expect(await claim(store, ['h'], 'worker-1', at(2), 5)).toEqual([]);
    expect(await store.complete(head!, at(3))).toBe(true);
    const [next] = await claim(store, ['h'], 'worker-1', at(4), 5);
    expect(next!.event.key).toBe('hot');
  });
});

describe('interleavings that used to break the fence', () => {
  /** Resolves once `count` backend processes wait for a lock and run a statement containing `marker`. */
  async function waitForBlocked(marker: string, count: number): Promise<void> {
    for (let attempt = 0; attempt < 400; attempt += 1) {
      const result = await database.pool.query<{ waiting: number }>(
        `SELECT count(*)::int AS waiting FROM pg_stat_activity
         WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE $1`,
        [`%${marker}%`],
      );
      if (result.rows[0]!.waiting >= count) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error(`fewer than ${count} statements containing ${marker} are waiting for a lock`);
  }

  async function laneOf(key: string): Promise<number> {
    const result = await database.pool.query<{ lane: number }>(
      `SELECT (hashtextextended($1::text, 0) & 255)::int AS lane`,
      [key],
    );
    return result.rows[0]!.lane;
  }

  it('does not let a completion delete a delivery that was rescheduled while the completion waited', async () => {
    const { store, schema } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [stale] = await claim(store, ['h'], 'worker-1');
    const rescheduling = await database.pool.connect();
    try {
      await rescheduling.query('BEGIN');
      // What reschedule does to the delivery row: a new due time and a new claim generation.
      await rescheduling.query(
        `UPDATE "${schema}".relaybus_deliveries SET available_at = $1, attempts = attempts + 1, claims = claims + 1
         WHERE id = $2`,
        [at(10), stale!.deliveryId],
      );
      const completing = store.completeMany([stale!], at(1));
      await waitForBlocked('FOR KEY SHARE', 1);
      await rescheduling.query('COMMIT');
      expect(await completing).toEqual([false]);
    } finally {
      rescheduling.release();
    }
    const remaining = await database.pool.query(
      `SELECT count(*)::int AS count FROM "${schema}".relaybus_deliveries WHERE id = $1`,
      [stale!.deliveryId],
    );
    expect(remaining.rows[0]).toEqual({ count: 1 });
  });

  it('does not claim a delivery from a stale snapshot after waiting behind its reschedule', async () => {
    const { store, schema } = await createStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    const [running] = await claim(store, ['h'], 'worker-1');
    const rescheduling = await database.pool.connect();
    try {
      await rescheduling.query('BEGIN');
      await rescheduling.query(
        `UPDATE "${schema}".relaybus_lanes SET in_flight = array_remove(in_flight, $1) WHERE lease_owner = 'worker-1'`,
        [running!.deliveryId],
      );
      await rescheduling.query(
        `UPDATE "${schema}".relaybus_deliveries SET available_at = $1, attempts = attempts + 1, claims = claims + 1 WHERE id = $2`,
        [at(10 * LEASE_MS), running!.deliveryId],
      );
      // This claim reads the delivery with the snapshot it took before the reschedule committed.
      const waiting = claim(store, ['h'], 'worker-1', at(1));
      await waitForBlocked('held AS MATERIALIZED', 1);
      await rescheduling.query('COMMIT');
      expect(await waiting).toEqual([]);
    } finally {
      rescheduling.release();
    }
    const [due] = await claim(store, ['h'], 'worker-1', at(10 * LEASE_MS));
    expect(due).toMatchObject({ attempt: 2 });
    expect(await store.complete(due!, at(10 * LEASE_MS + 1))).toBe(true);
  });

  it('keeps the deliveries two overlapping claims of one owner recorded', async () => {
    const { store, schema } = await createStore();
    await append(store, [
      { event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2', 'h3'] },
    ]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    const locker = await database.pool.connect();
    try {
      // Hold the lane row so both claims queue behind it and run one after the other afterwards.
      await locker.query('BEGIN');
      await locker.query(
        `SELECT 1 FROM "${schema}".relaybus_lanes WHERE lease_owner = 'worker-1' FOR UPDATE`,
      );
      const second = claim(store, ['h2'], 'worker-1', at(1), 1);
      const third = claim(store, ['h3'], 'worker-1', at(1), 1);
      // One of them waits for the lane row; the other queues behind it in the store (one claim at a time per owner).
      await waitForBlocked('held AS MATERIALIZED', 1);
      await locker.query('COMMIT');
      const batches = [...(await second), ...(await third)];
      expect(batches.map((delivery) => delivery.handler).sort()).toEqual(['h2', 'h3']);
    } finally {
      locker.release();
    }
    // Both are still in flight: nothing may be handed out again.
    expect(await claim(store, ['h1', 'h2', 'h3'], 'worker-1', at(2))).toEqual([]);
  });

  it('finds claimable work behind a window that blocked followers of many lanes fill together', async () => {
    const { store } = await createStore();
    const keys: string[] = [];
    const lanes = new Set<number>();
    for (let index = 0; keys.length < 5; index += 1) {
      const key = `window-${index}`;
      const lane = await laneOf(key);
      if (!lanes.has(lane)) {
        lanes.add(lane);
        keys.push(key);
      }
    }
    for (const key of keys) {
      await append(
        store,
        Array.from({ length: 60 }, () => ({
          event: createEnvelope({ key }),
          handlers: ['blocked'],
        })),
        at(0),
      );
    }
    expect(await claim(store, ['blocked'], 'worker-1', at(5), 5)).toHaveLength(5);
    for (const key of keys) {
      await append(store, [{ event: createEnvelope({ key }), handlers: ['free'] }], at(10));
    }
    // Every lane has fewer than 64 due deliveries, but together the blocked followers fill the 256 window.
    const claimed = await claim(store, ['blocked', 'free'], 'worker-1', at(20), 10);
    expect(claimed.map((delivery) => delivery.handler)).toEqual(Array(5).fill('free'));
  });

  it('locks an existing event before it adds a handler to it', async () => {
    const schema = uniqueSchema();
    const statements: string[] = [];
    const inner = new PgExecutor(database.pool);
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => inner.transaction(work),
      query: (sql, parameters, transaction) => {
        statements.push(sql);
        return inner.query(sql, parameters, transaction);
      },
    };
    const store = new PostgresEventStore({ executor, schema });
    await store.setup();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h1'] }]);
    statements.length = 0;
    await append(store, [{ event, handlers: ['h1', 'h2'] }]);
    const lock = statements.findIndex(
      (sql) => sql.includes('FOR NO KEY UPDATE') && sql.includes('ORDER BY id'),
    );
    const insert = statements.findIndex((sql) => sql.includes('fresh AS'));
    expect(lock).toBeGreaterThanOrEqual(0);
    expect(lock).toBeLessThan(insert);
  });
});

describe('retention after an upgrade from 0.1.0', () => {
  it('keeps the event of a recently completed delivery when an old completed row of 0.1.0 is purged', async () => {
    const { store, schema } = await createStore();
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['legacy', 'current'] }]);
    // The 0.1.0 delivery: kept as a completed row.
    await database.pool.query(
      `UPDATE "${schema}".relaybus_deliveries SET status = 'completed', completed_at = $1 WHERE handler = 'legacy'`,
      [at(10)],
    );
    const [current] = await claim(store, ['current'], 'worker-1', at(100));
    expect(await store.complete(current!, at(100))).toBe(true);
    expect(await store.purge({ completedBefore: at(50) })).toEqual({
      deletedDeliveries: 1,
      deletedEvents: 0,
    });
    const again = await store.transaction((transaction) =>
      store.append(transaction, [{ event, deliveries: [createDelivery('current')] }], at(101)),
    );
    expect(again.appendedDeliveryCount).toBe(0);
    expect(await store.purge({ completedBefore: at(200) })).toEqual({
      deletedDeliveries: 0,
      deletedEvents: 1,
    });
  });
});

describe('a claim whose response was lost', () => {
  /** A store whose statements can fail after the database executed them, like a connection reset before the answer. */
  async function createLossyStore() {
    const schema = uniqueSchema();
    const inner = new PgExecutor(database.pool);
    const errors: string[] = [];
    const lossy = {
      loseNext: undefined as string | undefined,
      staleForever: false,
      failReconcile: undefined as unknown,
      failReconcileOnce: false,
      running: 0,
      maximumRunning: 0,
      late: undefined as (() => Promise<unknown>) | undefined,
      delayNext: false,
      delayReconcile: false,
      delayTransaction: false,
      reconciled: [] as unknown[],
    };
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => {
        if (lossy.delayTransaction) {
          lossy.delayTransaction = false;
          lossy.late = () => inner.transaction(work);
          return Promise.reject(new Error('query timeout'));
        }
        return inner.transaction(work);
      },
      query: async (sql, parameters, transaction) => {
        if (lossy.failReconcile !== undefined && sql.includes('SET in_flight = ARRAY(')) {
          const failure = lossy.failReconcile as Error;
          if (lossy.failReconcileOnce) {
            lossy.failReconcile = undefined;
          }
          throw failure;
        }
        if (sql.includes('SET in_flight = ARRAY(')) {
          lossy.reconciled.push(JSON.parse(parameters[1] as string));
        }
        if (lossy.delayReconcile && sql.includes('SET in_flight = ARRAY(')) {
          lossy.delayReconcile = false;
          lossy.late = () => inner.query(sql, parameters, transaction);
          throw new Error('query timeout');
        }
        if (lossy.staleForever && sql.includes('stale AS')) {
          return [
            {
              stale: true,
              delivery_id: null,
              held_lanes: '[]',
              examined_lanes: '[]',
              running_ids: '[]',
              window_saturated: false,
            },
          ];
        }
        if (lossy.delayNext && sql.includes('held AS MATERIALIZED')) {
          // The client gives up (a timeout); the statement is still on its way to the server.
          lossy.delayNext = false;
          lossy.late = () => inner.query(sql, parameters, transaction);
          throw new Error('query timeout');
        }
        const claimingHeld = sql.includes('held AS MATERIALIZED');
        if (claimingHeld) {
          lossy.running += 1;
          lossy.maximumRunning = Math.max(lossy.maximumRunning, lossy.running);
        }
        try {
          const rows = await inner.query(sql, parameters, transaction);
          if (lossy.loseNext !== undefined && sql.includes(lossy.loseNext)) {
            lossy.loseNext = undefined;
            throw new Error('connection reset before the answer');
          }
          return rows;
        } finally {
          if (claimingHeld) {
            lossy.running -= 1;
          }
        }
      },
    };
    const store = new PostgresEventStore({
      executor,
      schema,
      logger: {
        log: () => undefined,
        warn: () => undefined,
        error: (message) => void errors.push(message),
      },
    });
    await store.setup();
    return { store, lossy, errors };
  }

  it('does not keep the deliveries it recorded but never handed out', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    lossy.loseNext = 'held AS MATERIALIZED';
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // The statement recorded h2 in the lane; nobody received it, so it must run again from the next claim.
    const [again] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(again).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('keeps what a claim handed out when it reconciles', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [running] = await claim(store, ['h1'], 'worker-1');
    lossy.loseNext = 'held AS MATERIALIZED';
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // h1 was handed out and is still running: the reconciliation must not release it.
    expect(await claim(store, ['h1'], 'worker-1', at(2))).toEqual([]);
    expect(await store.complete(running!, at(3))).toBe(true);
  });

  it('reports a reconciliation that fails too and still raises the original error', async () => {
    const { store, lossy, errors } = await createLossyStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.failReconcile = new Error('still down');
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('connection reset');
    lossy.failReconcile = 'plain text failure';
    // The reconciliation owed from the first failure is retried first and fails too: no claim is made.
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('no claim is made');
    expect(errors).toEqual(
      expect.arrayContaining([
        expect.stringContaining('error=still down'),
        expect.stringContaining('error=plain text failure'),
      ]),
    );
  });

  it('does not forget the claim of a newer attempt when a stale lease of the delivery is settled', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [stale] = await claim(store, ['h1'], 'worker-1');
    expect(await store.release(stale!, at(1))).toBe(true);
    const [current] = await claim(store, ['h1'], 'worker-1', at(2));
    expect(await store.complete(stale!, at(3))).toBe(false);
    lossy.loseNext = 'held AS MATERIALIZED';
    await expect(claim(store, ['h2'], 'worker-1', at(4))).rejects.toThrow('connection reset');
    // The newer attempt is still handed out: the reconciliation must keep it listed and its lease valid.
    expect(await claim(store, ['h1'], 'worker-1', at(5))).toEqual([]);
    expect(await store.complete(current!, at(6))).toBe(true);
  });

  it('keeps the claim of a transactional completion that may still roll back', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [running] = await claim(store, ['h1'], 'worker-1');
    await expect(
      store.transaction(async (transaction) => {
        expect(await store.complete(running!, at(1), transaction)).toBe(true);
        lossy.loseNext = 'held AS MATERIALIZED';
        await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toThrow('connection reset');
        throw new Error('handler transaction rolled back');
      }),
    ).rejects.toThrow('rolled back');
    // The delivery is still running under its lease: the reconciliation must not have unlisted it.
    expect(await store.complete(running!, at(3))).toBe(true);
  });

  it('retries a failed reconciliation before the next claim of the owner', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.failReconcile = new Error('still down');
    lossy.failReconcileOnce = true;
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // h2 is recorded in the lane but nobody has it; the next claim reconciles first and then takes it.
    const [again] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(again).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('runs the claims of one owner one at a time', async () => {
    const { store, lossy } = await createLossyStore();
    await append(
      store,
      Array.from({ length: 6 }, () => ({ event: createEnvelope(), handlers: ['h'] })),
    );
    const batches = await Promise.all([
      claim(store, ['h'], 'worker-1', at(0), 2),
      claim(store, ['h'], 'worker-1', at(0), 2),
      claim(store, ['h'], 'worker-1', at(0), 2),
    ]);
    expect(lossy.maximumRunning).toBe(1);
    const ids = batches.flat().map((delivery) => delivery.deliveryId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toHaveLength(6);
  });

  it('ignores a claim statement that reaches the server after its caller gave up and the owner reconciled', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    expect(await claim(store, ['h1'], 'worker-1')).toHaveLength(1);
    lossy.delayNext = true;
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('query timeout');
    // The abandoned statement arrives only now, after the reconciliation of the owner.
    await lossy.late!();
    const [again] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(again).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('ignores the lane a free-lane claim takes when it reaches the server after its caller gave up', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    // The claim statement takes the free lane; it is what reaches the server late.
    lossy.delayNext = true;
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('query timeout');
    // The abandoned transaction takes the lane and records the delivery after the owner reconciled.
    await lossy.late!();
    // Nobody was given that delivery: the owner's next claim takes the lane over, like one of a crashed worker.
    const [again] = await claim(store, ['h'], 'worker-1', at(1));
    expect(again).toMatchObject({ handler: 'h', attempt: 2 });
    expect(await claim(store, ['h'], 'worker-1', at(2))).toEqual([]);
    expect(await store.complete(again!, at(3))).toBe(true);
  });

  it('charges the attempt of a delivery its previous incarnation held when an owner restarts under the same name', async () => {
    const schema = uniqueSchema();
    const first = new PostgresEventStore({ executor: new PgExecutor(database.pool), schema });
    await first.setup();
    await append(first, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [poison] = await claim(first, ['h'], 'worker-1');
    expect(poison).toMatchObject({ attempt: 1 });
    // The process crashed with the delivery running and came back before its lease expired.
    const second = new PostgresEventStore({ executor: new PgExecutor(database.pool), schema });
    const [again] = await claim(second, ['h'], 'worker-1', at(1));
    expect(again).toMatchObject({ deliveryId: poison!.deliveryId, attempt: 2 });
    expect(await first.complete(poison!, at(2))).toBe(false);
  });

  it('holds a taken-over lane for its new owner whatever claim floor the previous owner left', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ executor: new PgExecutor(database.pool), schema });
    await store.setup();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [first] = await claim(store, ['h'], 'worker-1');
    await database.pool.query(`UPDATE "${schema}".relaybus_lanes SET claim_floor = $1`, [
      String(Date.now() * 2),
    ]);
    // worker-1 dies; worker-2 takes the lane after its lease expired.
    const [taken] = await claim(store, ['h'], 'worker-2', at(LEASE_MS + 1));
    expect(taken).toMatchObject({ deliveryId: first!.deliveryId, attempt: 2 });
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['g'] }]);
    const [other] = await claim(store, ['g'], 'worker-2', at(LEASE_MS + 2));
    expect(other).toMatchObject({ handler: 'g', attempt: 1 });
  });

  it('does not retake a live lane of a newer generation when a free-lane claim arrives late', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    // The claim statement takes the free lane; it is what reaches the server late.
    lossy.delayNext = true;
    await expect(claim(store, ['h'], 'worker-1')).rejects.toThrow('query timeout');
    // The owner reconciled and claims again; the abandoned transaction arrives while that delivery runs.
    const [running] = await claim(store, ['h'], 'worker-1', at(1));
    expect(running).toBeDefined();
    await lossy.late!();
    expect(await store.extendLease(running!, at(LEASE_MS * 2))).toBe(true);
    expect(await claim(store, ['h'], 'worker-2', at(2))).toEqual([]);
    expect(await store.complete(running!, at(3))).toBe(true);
  });

  it('does not claim again while the reconciliation of a failed claim still fails', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [running] = await claim(store, ['h1'], 'worker-1');
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.failReconcile = new Error('still down');
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // The generation moved on but the lanes were not reconciled: claiming now would treat the live lane as foreign.
    await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toThrow('no claim is made');
    await expect(claim(store, ['h2'], 'worker-1', at(2))).rejects.toMatchObject({
      name: 'ReconcileFailedError',
      code: 'RELAYBUS_POSTGRES_RECONCILE_FAILED',
    });
    expect(await store.complete(running!, at(3))).toBe(true);
    lossy.failReconcile = undefined;
    const [next] = await claim(store, ['h2'], 'worker-1', at(4));
    expect(next).toMatchObject({ handler: 'h2', attempt: 1 });
  });

  it('forgets a claim completed in the caller transaction once the next claim freed its lane', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [running] = await claim(store, ['h'], 'worker-1');
    await store.transaction((transaction) => store.complete(running!, at(1), transaction));
    // Nothing runs in the lane any more: this claim frees it.
    expect(await claim(store, ['h'], 'worker-1', at(2))).toEqual([]);
    lossy.loseNext = 'held AS MATERIALIZED';
    await append(store, [{ event: createEnvelope({ key: 'two' }), handlers: ['h'] }]);
    await expect(claim(store, ['h'], 'worker-1', at(3))).rejects.toThrow('connection reset');
    expect(lossy.reconciled).toEqual([[]]);
  });

  it('ignores a reconciliation that reaches the server after a newer reconciliation and a new claim', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h1', 'h2'] }]);
    const [first] = await claim(store, ['h1'], 'worker-1');
    expect(first).toBeDefined();
    lossy.loseNext = 'held AS MATERIALIZED';
    lossy.delayReconcile = true;
    await expect(claim(store, ['h2'], 'worker-1', at(1))).rejects.toThrow('connection reset');
    // The next claim reconciles again, then hands out the delivery the lost claim had recorded.
    const [second] = await claim(store, ['h2'], 'worker-1', at(2));
    expect(second).toMatchObject({ handler: 'h2', attempt: 1 });
    // The first reconciliation arrives only now with its old picture of what was handed out.
    await lossy.late!();
    expect(await claim(store, ['h2'], 'worker-1', at(3))).toEqual([]);
    expect(await store.complete(second!, at(4))).toBe(true);
  });

  it('gives up a held-lane claim that stays stale and still takes free lanes', async () => {
    const { store, lossy } = await createLossyStore();
    await append(store, [{ event: createEnvelope(), handlers: ['h'] }]);
    lossy.staleForever = true;
    expect(await claim(store, ['h'], 'worker-1')).toHaveLength(1);
  });
});

describe('takeover of a lane that changed while the statement waited', () => {
  it('charges only the deliveries the locked version of the lane still lists', async () => {
    const schema = uniqueSchema();
    const inner = new PgExecutor(database.pool);
    let gated = false;
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => inner.transaction(work),
      query: (sql, parameters, transaction) =>
        inner.query(
          gated && sql.includes('FOR UPDATE OF free SKIP LOCKED')
            ? // Waits, after the statement took its snapshot and before it locks the lane, for the test.
              sql
                .replace(
                  'WHERE free.lease_expires_at IS NULL',
                  'WHERE (SELECT pg_advisory_xact_lock_shared(7777) IS NOT NULL) AND (free.lease_expires_at IS NULL',
                )
                .replace(
                  'ORDER BY oldest.available_at, oldest.id',
                  ') ORDER BY oldest.available_at, oldest.id',
                )
            : sql,
          parameters,
          transaction,
        ),
    };
    const store = new PostgresEventStore({ executor, schema });
    await store.setup();
    await append(store, [{ event: createEnvelope({ key: 'one-lane' }), handlers: ['h'] }]);
    const [crashed] = await claim(store, ['h'], 'worker-1');
    const blocker = await database.pool.connect();
    try {
      await blocker.query('SELECT pg_advisory_lock(7777)');
      gated = true;
      const takeover = claim(store, ['h'], 'worker-2', at(LEASE_MS + 1));
      for (;;) {
        const waiting = await database.pool.query(
          "SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        );
        if (waiting.rowCount !== 0) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      // The previous owner records its failed attempt after the takeover statement took its snapshot.
      expect(await store.reschedule(crashed!, at(0), error, at(LEASE_MS + 1))).toBe(true);
      await blocker.query('SELECT pg_advisory_unlock(7777)');
      const [taken] = await takeover;
      // One attempt failed (charged by the reschedule) and this is the second: it was not charged twice.
      expect(taken).toMatchObject({ deliveryId: crashed!.deliveryId, attempt: 2 });
    } finally {
      blocker.release();
    }
  });
});

describe('claim behind a saturated fast window', () => {
  it('claims the head of every key of a lane whose front is full of followers, one per key, in commit order', async () => {
    const { store, schema } = await createStore();
    const head = createEnvelope({ key: 'blocked' });
    await append(store, [{ event: head, handlers: ['h'] }], at(0));
    const [running] = await claim(store, ['h'], 'worker-0', at(0), 1);
    expect(running!.event.id).toBe(head.id);
    // More due followers of the key than one lane window reads, so they fill the front of the lane.
    for (let index = 0; index < 80; index += 1) {
      await append(store, [{ event: createEnvelope({ key: 'blocked' }), handlers: ['h'] }], at(2));
    }
    // Probe keys until two other keys hash into the lane of 'blocked'.
    const probes = new Map<string, string>();
    for (let index = 0; index < 600; index += 1) {
      const event = createEnvelope({ key: `probe-${index}` });
      probes.set(event.id, `probe-${index}`);
      await append(store, [{ event, handlers: ['h'] }], at(100));
    }
    const lane = await database.pool.query(
      `SELECT lane FROM "${schema}".relaybus_deliveries WHERE id = $1`,
      [running!.deliveryId],
    );
    const sameLane = await database.pool.query<{ event_id: string }>(
      `SELECT event_id FROM "${schema}".relaybus_deliveries WHERE lane = $1 AND key LIKE 'probe-%' ORDER BY sequence`,
      [lane.rows[0]!.lane],
    );
    const [first, second] = sameLane.rows.map((row) => row.event_id);
    expect(second).toBeDefined();
    // And unkeyed deliveries, which have no ordering to keep.
    const unkeyed = Array.from({ length: 1_000 }, () => createEnvelope());
    await append(
      store,
      unkeyed.map((event) => ({ event, handlers: ['h'] })),
      at(100),
    );
    const unkeyedInLane = await database.pool.query<{ event_id: string }>(
      `SELECT event_id FROM "${schema}".relaybus_deliveries WHERE lane = $1 AND key IS NULL`,
      [lane.rows[0]!.lane],
    );
    const laneUnkeyed = new Set(unkeyedInLane.rows.map((row) => row.event_id));
    expect(laneUnkeyed.size).toBeGreaterThan(0);
    const firstFollower = createEnvelope({ key: probes.get(first!)! });
    await append(store, [{ event: firstFollower, handlers: ['h'] }], at(101));
    // The first worker lost its lease: the lane has to be taken over, in the second step of the claim.
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS + 1), 5_000);
    const inLane = claimed
      .map((delivery) => delivery.event.id)
      .filter((id) => id === head.id || id === first || id === second || id === firstFollower.id);
    expect(inLane).toEqual([head.id, first, second]);
    expect(claimed.filter((delivery) => delivery.event.key === 'blocked')).toHaveLength(1);
    expect(claimed.filter((delivery) => laneUnkeyed.has(delivery.event.id))).toHaveLength(
      laneUnkeyed.size,
    );
  });
});

describe('interleavings of two statements of one call', () => {
  async function hookedStore(
    hook: (sql: string, schema: string) => Promise<void>,
  ): Promise<{ store: PostgresEventStore; schema: string }> {
    const schema = uniqueSchema();
    const inner = new PgExecutor(database.pool);
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => inner.transaction(work),
      query: async (sql, parameters, transaction) => {
        const rows = await inner.query(sql, parameters, transaction);
        await hook(sql, schema);
        return rows;
      },
    };
    const store = new PostgresEventStore({ executor, schema });
    await store.setup();
    return { store, schema };
  }

  it('hands out only one delivery of a key when a dead letter is requeued while the claim runs', async () => {
    let requeue: (() => Promise<void>) | undefined;
    const { store, schema } = await hookedStore(async (sql) => {
      if (requeue !== undefined && sql.includes('held AS MATERIALIZED')) {
        const run = requeue;
        requeue = undefined;
        await run();
      }
    });
    const events = Array.from({ length: 72 }, () => createEnvelope({ key: 'one-key' }));
    for (const event of events) {
      await append(store, [{ event, handlers: ['h'] }]);
    }
    const [head] = await claim(store, ['h'], 'worker-0', at(0), 1);
    expect(head!.event.id).toBe(events[0]!.id);
    expect(await store.bury(head!, error, at(1))).toBe(true);
    requeue = async () => {
      await database.pool.query(
        `UPDATE "${schema}".relaybus_deliveries SET status = 'pending', attempts = 0 WHERE id = $1`,
        [head!.deliveryId],
      );
    };
    const claimed = await claim(store, ['h'], 'worker-1', at(LEASE_MS + 2), 200);
    // The claim is one statement with one snapshot: the dead letter requeued after it took the snapshot
    // is not seen, so exactly one delivery of the key runs, and the requeued one waits behind it.
    expect(claimed.map((delivery) => delivery.event.id)).toEqual([events[1]!.id]);
    expect(await claim(store, ['h'], 'worker-1', at(LEASE_MS + 3), 200)).toEqual([]);
  });

  it('keeps an event whose completion time moved after the purge selected it', async () => {
    let touch: (() => Promise<void>) | undefined;
    const { store, schema } = await hookedStore(async (sql) => {
      if (touch !== undefined && sql.includes('ORDER BY completion.completed_at')) {
        const run = touch;
        touch = undefined;
        await run();
      }
    });
    const event = createEnvelope();
    await append(store, [{ event, handlers: ['h'] }]);
    const [delivery] = await claim(store, ['h'], 'worker-1', at(0));
    expect(await store.complete(delivery!, at(10))).toBe(true);
    // A newer completion of the same event is recorded after the sweep selected the event.
    touch = async () => {
      await database.pool.query(
        `UPDATE "${schema}".relaybus_event_completions SET completed_at = $1 WHERE event_id = $2`,
        [at(1_000), event.id],
      );
    };
    expect(await store.purge({ completedBefore: at(100) })).toMatchObject({ deletedEvents: 0 });
    expect(await store.purge({ completedBefore: at(2_000) })).toMatchObject({ deletedEvents: 1 });
  });

  it('appends deliveries of an existing event next to an existing event without deliveries', async () => {
    const { store } = await createStore();
    const first = createEnvelope();
    const second = createEnvelope();
    await append(store, [
      { event: first, handlers: [] },
      { event: second, handlers: [] },
    ]);
    await append(
      store,
      [
        { event: first, handlers: ['h'] },
        { event: second, handlers: [] },
      ],
      at(1),
    );
    expect(await store.listDeliveries({ eventId: first.id })).toHaveLength(1);
    expect(await store.listDeliveries({ eventId: second.id })).toHaveLength(0);
  });

  it('keeps an event that a purge removed between the insert and the lock of an append', async () => {
    let purgeEvent: (() => Promise<void>) | undefined;
    const { store, schema } = await hookedStore(async (sql) => {
      if (purgeEvent !== undefined && sql.includes('ON CONFLICT (id) DO NOTHING')) {
        const run = purgeEvent;
        purgeEvent = undefined;
        await run();
      }
    });
    const event = createEnvelope();
    await append(store, [{ event, handlers: [] }]);
    purgeEvent = async () => {
      await database.pool.query(`DELETE FROM "${schema}".relaybus_events WHERE id = $1`, [
        event.id,
      ]);
    };
    const result = await store.transaction((transaction) =>
      store.append(transaction, [{ event, deliveries: [createDelivery('h')] }], at(1)),
    );
    expect(result.appendedDeliveryCount).toBe(1);
    expect(await store.listDeliveries({ eventId: event.id })).toHaveLength(1);
  });
});
