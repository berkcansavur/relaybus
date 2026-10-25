import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { PoolClient } from 'pg';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { PgExecutor, PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import { LEASE_MS } from './support/claim.js';
import { createLaneHelpers, error } from './support/lane-helpers.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

const { append, claim, createStore } = createLaneHelpers(() => database);

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
      expect.stringContaining('[PostgresClaims](claimMoreLanes) Taking more lanes failed'),
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
