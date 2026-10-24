import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createUuidV7 } from '@relaybus/core';
import type { PoolClient } from 'pg';
import {
  PostgresEventStore,
  createPostgresSchemaStatements,
  resolvePostgresNames,
} from '../src/index.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

function createStore(schema: string): PostgresEventStore {
  return new PostgresEventStore({
    connectionString: database.connectionString,
    schema,
    poolConfig: { max: 4 },
  });
}

async function publish(store: PostgresEventStore, count: number): Promise<void> {
  const now = new Date();
  await store.transaction((transaction) =>
    store.append(
      transaction,
      Array.from({ length: count }, (_, index) => ({
        event: {
          id: createUuidV7(),
          type: 'setup.v1',
          source: 'setup',
          key: index % 3 === 0 ? `key-${index % 7}` : null,
          payload: { index },
          headers: {},
          correlationId: null,
          causationId: null,
          occurredAt: now,
        },
        deliveries: [{ id: createUuidV7(), handler: 'h', availableAt: now }],
      })),
      now,
    ),
  );
}

/** Waits until `count` backends of the schema's database wait for a heavyweight lock. */
async function waitForLockWaiters(count: number): Promise<void> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    const { rows } = await database.pool.query<{ waiting: number }>(
      `SELECT count(*)::int AS waiting FROM pg_stat_activity
       WHERE datname = current_database() AND wait_event_type = 'Lock'`,
    );
    if (rows[0]!.waiting >= count) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`only ${rows[0]!.waiting} of ${count} backends wait for a lock`);
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

async function holdInTransaction(
  statement: string,
  parameters: unknown[] = [],
): Promise<PoolClient> {
  const client = await database.pool.connect();
  await client.query('BEGIN');
  await client.query(statement, parameters);
  return client;
}

async function finish(client: PoolClient): Promise<void> {
  await client.query('COMMIT');
  client.release();
}

describe('setup next to running dispatchers', () => {
  it('does not lock the tables when the schema is current, so a claim cannot deadlock with it', async () => {
    const schema = uniqueSchema();
    const names = resolvePostgresNames({ schema });
    const dispatcher = createStore(schema);
    const starting = createStore(schema);
    await dispatcher.setup();
    await publish(dispatcher, 20);

    // A writer on deliveries (any running publish or completion) makes a table lock of setup wait.
    const writer = await holdInTransaction(`LOCK TABLE ${names.deliveries} IN ROW EXCLUSIVE MODE`);
    const setup = starting.setup();
    const setupWaited = await Promise.race([
      setup.then(() => false),
      waitForLockWaiters(1).then(() => true),
    ]);
    // A claim locks its lanes first, then needs deliveries.
    const claim = dispatcher.claim({
      handlers: ['h'],
      limit: 5,
      leaseOwner: 'dispatcher',
      leaseExpiresAt: new Date(Date.now() + 60_000),
      now: new Date(),
    });
    if (setupWaited) {
      await waitForLockWaiters(2);
    }
    await finish(writer);

    const [setupResult, claimResult] = await Promise.allSettled([setup, claim]);
    expect(setupResult).toEqual({ status: 'fulfilled', value: undefined });
    expect(claimResult.status).toBe('fulfilled');
    expect(setupWaited).toBe(false);
    await Promise.all([dispatcher.close(), starting.close()]);
  });

  it('runs no DDL when the schema became current while it waited for the setup lock', async () => {
    const schema = uniqueSchema();
    const holder = await database.pool.connect();
    const store = createStore(schema);
    try {
      await holder.query('BEGIN');
      // The same advisory lock setup takes, held while another process creates the schema.
      await holder.query('SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))', [
        `relaybus:${schema}`,
      ]);
      const settingUp = store.setup();
      // setup found no schema and now waits for the lock held above.
      await expect
        .poll(async () => {
          const { rows } = await database.pool.query<{ waiting: number }>(
            "SELECT count(*)::int AS waiting FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
          );
          return rows[0]!.waiting;
        })
        .toBe(1);
      for (const statement of createPostgresSchemaStatements({ schema })) {
        await holder.query(statement);
      }
      await holder.query('COMMIT');
      // It takes the lock, finds the marker of the current statements and returns without running them.
      await settingUp;
      await publish(store, 3);
      expect(await store.listDeliveries({})).toHaveLength(3);
    } finally {
      holder.release();
      await store.close();
    }
  });

  it('runs the schema statements once when setups race on a new schema', async () => {
    const schema = uniqueSchema();
    const stores = [createStore(schema), createStore(schema), createStore(schema)];
    // Every setup finds the schema missing and queues on the setup lock; one creates it, the others find it current.
    const lock = await holdInTransaction(
      'SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))',
      [`relaybus:${schema}`],
    );
    const setups = stores.map((store) => store.setup());
    await waitForLockWaiters(stores.length);
    await finish(lock);
    await Promise.all(setups);
    await publish(stores[0]!, 3);
    expect(await stores[1]!.statistics(new Date())).toMatchObject({ pending: 3 });
    await Promise.all(stores.map((store) => store.close()));
  });

  it('keeps claiming, completing, reconciling and relinquishing while other instances start', async () => {
    const schema = uniqueSchema();
    const dispatchers = Array.from({ length: 4 }, () => createStore(schema));
    await dispatchers[0]!.setup();
    const total = 400;
    await publish(dispatchers[0]!, total);
    let completed = 0;
    const errors: unknown[] = [];

    async function dispatch(index: number): Promise<void> {
      const store = dispatchers[index]!;
      const owner = `dispatcher-${index}`;
      const deadline = Date.now() + 60_000;
      while (completed < total && Date.now() < deadline) {
        const now = new Date();
        const batch = await store.claim({
          handlers: ['h'],
          limit: 10,
          leaseOwner: owner,
          leaseExpiresAt: new Date(now.getTime() + 60_000),
          now,
        });
        if (batch.length === 0) {
          await store.relinquish(owner);
          await new Promise((resolve) => setTimeout(resolve, 2));
          continue;
        }
        const results = await store.completeMany(batch, new Date());
        completed += results.filter(Boolean).length;
        if (index % 2 === 0) {
          await store.relinquish(owner);
        }
      }
    }

    let dispatching = true;
    async function start(): Promise<void> {
      while (dispatching) {
        const store = createStore(schema);
        await store.setup();
        await store.close();
      }
    }

    const dispatched = Promise.all(
      dispatchers.map((_, index) => dispatch(index).catch((error: unknown) => errors.push(error))),
    ).finally(() => {
      dispatching = false;
    });
    await Promise.all([
      dispatched,
      ...Array.from({ length: 3 }, () => start().catch((error: unknown) => errors.push(error))),
    ]);
    expect(errors).toEqual([]);
    expect(completed).toBe(total);
    await Promise.all(dispatchers.map((store) => store.close()));
  });
});
