import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createUuidV7, type EventEnvelope } from '@relaybus/core';
import { createPostgresSchemaSql, PgExecutor, PostgresEventStore } from '../src/index.js';
import {
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

const NOW = new Date('2026-01-01T00:00:00.000Z');

function envelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: createUuidV7(),
    type: 'test.event.v1',
    source: 'test',
    key: null,
    payload: { value: 1 },
    headers: {},
    correlationId: null,
    causationId: null,
    occurredAt: NOW,
    ...overrides,
  };
}

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

async function tableNames(schema: string): Promise<string[]> {
  const result = await database.pool.query<{ table_name: string }>(
    'SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name',
    [schema],
  );
  return result.rows.map((row) => row.table_name);
}

describe('setup', () => {
  it('can run concurrently from many pools without colliding', async () => {
    const schema = uniqueSchema();
    for (let round = 0; round < 3; round += 1) {
      const stores = Array.from(
        { length: 12 },
        () =>
          new PostgresEventStore({
            connectionString: database.connectionString,
            schema,
            tablePrefix: 'bus_',
          }),
      );
      await Promise.all(stores.map((store) => store.setup()));
      await Promise.all(stores.map((store) => store.close()));
    }
    expect(await tableNames(schema)).toEqual([
      'bus_deliveries',
      'bus_event_completions',
      'bus_events',
      'bus_key_sequences',
      'bus_lanes',
    ]);
  });

  it('creates the default tables in the public schema', async () => {
    const store = new PostgresEventStore({ pool: database.pool });
    await Promise.all([store.setup(), store.setup(), store.setup()]);
    expect(await tableNames('public')).toEqual(
      expect.arrayContaining(['relaybus_deliveries', 'relaybus_events', 'relaybus_key_sequences']),
    );
  });

  it('is replaced by exported DDL applied through a migration', async () => {
    const schema = uniqueSchema();
    const options = { schema, tablePrefix: 'migrated_' };
    const sql = createPostgresSchemaSql(options);
    await database.pool.query(sql);
    await database.pool.query(sql);
    const store = new PostgresEventStore({ pool: database.pool, ...options });
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [
          {
            event: envelope(),
            deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }],
          },
        ],
        NOW,
      ),
    );
    expect(await store.statistics(NOW)).toMatchObject({ pending: 1 });
  });
});

describe('pool ownership', () => {
  it('does not close a pool it was given', async () => {
    const pool = createTestPool(database.connectionString, 10);
    const store = new PostgresEventStore({ pool, schema: uniqueSchema() });
    await store.setup();
    await store.close();
    await store.close();
    await expect(pool.query('SELECT 1')).resolves.toMatchObject({ rowCount: 1 });
    await pool.end();
  });

  it('closes the pool it created, once', async () => {
    const store = new PostgresEventStore({
      connectionString: database.connectionString,
      poolConfig: { max: 2 },
      schema: uniqueSchema(),
    });
    await store.setup();
    await store.close();
    await store.close();
    await expect(store.statistics(NOW)).rejects.toThrow(/end/);
  });
});

describe('purge batching', () => {
  it('deletes in bounded batches until nothing is left and keeps referenced events', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({
      pool: database.pool,
      schema,
      purgeBatchSize: 2,
      completionHistory: true,
    });
    await store.setup();
    const shared = envelope();
    const entries = [
      ...Array.from({ length: 6 }, () => ({
        event: envelope(),
        deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }],
      })),
      {
        event: shared,
        deliveries: [
          { id: createUuidV7(), handler: 'h', availableAt: NOW },
          { id: createUuidV7(), handler: 'other', availableAt: NOW },
        ],
      },
    ];
    await store.transaction((transaction) => store.append(transaction, entries, NOW));
    const claimed = await store.claim({
      handlers: ['h'],
      limit: 100,
      leaseOwner: 'w',
      leaseExpiresAt: new Date(NOW.getTime() + 60_000),
      now: NOW,
    });
    expect(claimed).toHaveLength(7);
    for (const delivery of claimed) {
      await store.complete(delivery, NOW);
    }
    const later = new Date(NOW.getTime() + 1_000);
    expect(await store.purge({ completedBefore: later })).toEqual({
      deletedDeliveries: 7,
      deletedEvents: 6,
    });
    expect(await store.listDeliveries({})).toHaveLength(1);
  });

  it('purges completed and dead deliveries in one call', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema, completionHistory: true });
    await store.setup();
    await store.transaction((transaction) =>
      store.append(
        transaction,
        Array.from({ length: 2 }, () => ({
          event: envelope(),
          deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }],
        })),
        NOW,
      ),
    );
    const [first, second] = await store.claim({
      handlers: ['h'],
      limit: 2,
      leaseOwner: 'w',
      leaseExpiresAt: new Date(NOW.getTime() + 60_000),
      now: NOW,
    });
    await store.complete(first!, NOW);
    await store.bury(second!, { name: 'Error', message: 'x' }, NOW);
    const later = new Date(NOW.getTime() + 1_000);
    expect(await store.purge({ completedBefore: later, deadBefore: later })).toEqual({
      deletedDeliveries: 2,
      deletedEvents: 2,
    });
  });
});

describe('append edge cases', () => {
  it('stores payloads that serialize to nothing as JSON null', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    const event = envelope({ payload: undefined });
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event, deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }] }],
        NOW,
      ),
    );
    const [record] = await store.listDeliveries({});
    expect(record!.event.payload).toBeNull();
  });

  it('assigns consecutive sequences to several events of one key in a single call', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    const events = Array.from({ length: 3 }, () => envelope({ key: 'k' }));
    await store.transaction((transaction) =>
      store.append(
        transaction,
        events.map((event) => ({
          event,
          deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }],
        })),
        NOW,
      ),
    );
    const result = await database.pool.query<{ id: string; sequence: string }>(
      `SELECT id, sequence::text FROM "${schema}".relaybus_events ORDER BY sequence`,
    );
    expect(result.rows.map((row) => row.id)).toEqual(events.map((event) => event.id));
    expect(result.rows.map((row) => row.sequence)).toEqual(['1', '2', '3']);
  });
});

describe('reports of new deliveries', () => {
  async function publish(store: PostgresEventStore, availableAt: Date): Promise<void> {
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [{ event: envelope(), deliveries: [{ id: createUuidV7(), handler: 'h', availableAt }] }],
        NOW,
      ),
    );
  }

  async function settle(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 150));
  }

  it('reports a committed publish of due deliveries once, and neither delayed nor rolled back ones', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    const peer = new PostgresEventStore({ pool: database.pool, schema });
    let reports = 0;
    const unwatch = await peer.watchNewDeliveries(() => void (reports += 1));
    let afterBoth = 0;
    try {
      await publish(store, NOW);
      await settle();
      expect(reports).toBe(1);
      // Two publish calls of one transaction notify at its commit; the second, sent within the
      // notification interval, may add one wake-up of its own when the interval ends.
      await store.transaction(async (transaction) => {
        for (let index = 0; index < 2; index += 1) {
          await store.append(
            transaction,
            [
              {
                event: envelope(),
                deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }],
              },
            ],
            NOW,
          );
        }
      });
      await settle();
      expect(reports).toBeGreaterThanOrEqual(2);
      expect(reports).toBeLessThanOrEqual(3);
      afterBoth = reports;
      await publish(store, new Date(NOW.getTime() + 60_000));
      await expect(
        store.transaction(async (transaction) => {
          await store.append(
            transaction,
            [
              {
                event: envelope(),
                deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }],
              },
            ],
            NOW,
          );
          throw new Error('rolled back');
        }),
      ).rejects.toThrow('rolled back');
      await settle();
      expect(reports).toBe(afterBoth);
      // A handler added to an existing event is reported too.
      const existing = envelope();
      await store.transaction((transaction) =>
        store.append(transaction, [{ event: existing, deliveries: [] }], NOW),
      );
      await store.transaction((transaction) =>
        store.append(
          transaction,
          [
            {
              event: existing,
              deliveries: [{ id: createUuidV7(), handler: 'late', availableAt: NOW }],
            },
          ],
          NOW,
        ),
      );
      await settle();
      expect(reports).toBe(afterBoth + 1);
    } finally {
      await unwatch!();
    }
    await publish(store, NOW);
    await settle();
    expect(reports).toBe(afterBoth + 1);
  });

  it('sends about one wake-up per interval for a burst of publishing transactions', async () => {
    const schema = uniqueSchema();
    const store = new PostgresEventStore({ pool: database.pool, schema });
    await store.setup();
    const peer = new PostgresEventStore({ pool: database.pool, schema });
    let reports = 0;
    const unwatch = await peer.watchNewDeliveries(() => void (reports += 1));
    // Every publish is a transaction of its own that would notify; the store sends at most one per
    // 10 ms interval of the burst, and one trailing wake-up per interval.
    const started = performance.now();
    for (let index = 0; index < 40; index += 1) {
      await publish(store, NOW);
    }
    const intervals = Math.ceil((performance.now() - started) / 10);
    await settle();
    await unwatch!();
    expect(reports).toBeGreaterThanOrEqual(1);
    expect(reports).toBeLessThanOrEqual(Math.min(40, 2 * intervals + 1));
    await store.close();
  });

  it('cannot listen through an executor, which has no connection of its own', async () => {
    const store = new PostgresEventStore({
      executor: { transaction: (work) => work(null as never), query: async () => [] },
      schema: uniqueSchema(),
    });
    expect(await store.watchNewDeliveries(() => undefined)).toBeUndefined();
  });

  it('uses a hashed channel when the readable one would be too long', async () => {
    const schema = `s${'x'.repeat(60)}`;
    const pool = createTestPool(database.connectionString, 2);
    const store = new PostgresEventStore({ pool, schema });
    await store.setup();
    let reports = 0;
    const unwatch = await store.watchNewDeliveries(() => void (reports += 1));
    await publish(store, NOW);
    await settle();
    await unwatch!();
    await pool.end();
    expect(reports).toBe(1);
  });
});

describe('claims of an owner draining its held lanes', () => {
  /** A store whose full claim statement (the one that also looks at free lanes) fails while `failing` is set. */
  async function storeWithFailingFullClaim() {
    const pool = createTestPool(database.connectionString, 2);
    const inner = new PgExecutor(pool);
    const state = {
      failing: false,
      failure: new Error('free lanes unreachable') as unknown,
      fullClaims: 0,
      heldClaims: 0,
    };
    const errors: string[] = [];
    const store = new PostgresEventStore({
      schema: uniqueSchema(),
      logger: {
        error: (message: string) => errors.push(message),
        warn() {},
        info() {},
        debug() {},
      } as never,
      executor: {
        transaction: (work) => inner.transaction(work),
        async query(sql, parameters, transaction) {
          if (sql.startsWith('WITH held AS MATERIALIZED')) {
            const full = sql.includes('free_lanes AS MATERIALIZED');
            state[full ? 'fullClaims' : 'heldClaims'] += 1;
            if (full && state.failing) {
              throw state.failure;
            }
          }
          return inner.query(sql, parameters, transaction);
        },
      },
    });
    await store.setup();
    const claim = (limit: number) =>
      store.claim({
        handlers: ['h'],
        limit,
        leaseOwner: 'w',
        leaseExpiresAt: new Date(NOW.getTime() + 60_000),
        now: NOW,
      });
    return { store, pool, state, errors, claim };
  }

  async function publishKeyed(store: PostgresEventStore, count: number): Promise<void> {
    await store.transaction((transaction) =>
      store.append(
        transaction,
        Array.from({ length: count }, () => ({
          event: envelope({ key: 'k' }),
          deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: NOW }],
        })),
        NOW,
      ),
    );
  }

  it('asks the held lanes first and the free lanes only when they came up short', async () => {
    const { store, pool, state, errors, claim } = await storeWithFailingFullClaim();
    await publishKeyed(store, 5);
    // Cold: the lane of `k` is taken from the free lanes, so the owner is not draining yet.
    const [first] = await claim(1);
    expect(await store.complete(first!, NOW)).toBe(true);
    // Filled from the held lane: the owner drains it now.
    const [second] = await claim(1);
    expect(await store.complete(second!, NOW)).toBe(true);
    expect(state).toMatchObject({ fullClaims: 2, heldClaims: 0 });
    // The held lane gives the last head; looking at the free lanes for the second one fails, and the
    // claim returns what it has.
    state.failing = true;
    const third = await claim(2);
    expect(third).toHaveLength(1);
    expect(state).toMatchObject({ fullClaims: 3, heldClaims: 1 });
    expect(errors).toEqual([
      expect.stringContaining('[PostgresEventStore](claim) Claiming from free lanes failed'),
    ]);
    expect(await store.complete(third[0]!, NOW)).toBe(true);
    // Short of the limit, so not draining; the next claim fills from the held lane and is again.
    state.failing = false;
    const [fourth] = await claim(1);
    expect(await store.complete(fourth!, NOW)).toBe(true);
    state.failing = true;
    state.failure = 'connection reset';
    expect(await claim(2)).toHaveLength(1);
    expect(errors.at(-1)).toContain('error=connection reset');
    await pool.end();
  });

  it('fails a claim whose held lanes gave nothing when the free lanes cannot be read', async () => {
    const { store, pool, state, claim } = await storeWithFailingFullClaim();
    await publishKeyed(store, 2);
    const [first] = await claim(1);
    expect(await store.complete(first!, NOW)).toBe(true);
    const [second] = await claim(1);
    expect(await store.complete(second!, NOW)).toBe(true);
    state.failing = true;
    await expect(claim(1)).rejects.toThrow('free lanes unreachable');
    state.failing = false;
    // No longer draining: the next claim goes to the full statement directly.
    const before = state.heldClaims;
    expect(await claim(1)).toEqual([]);
    expect(state.heldClaims).toBe(before);
    await pool.end();
  });
});
