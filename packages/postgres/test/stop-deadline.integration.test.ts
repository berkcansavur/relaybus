import type { PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, defineEvent, type StopResult } from '@relaybus/core';
import { PostgresEventStore } from '../src/index.js';
import {
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const TIMEOUT_MS = 600;
/** What `stop` may take beyond its timeout: the force-close itself and the scheduling of the test. */
const EPSILON_MS = 400;
const LEASE_MS = 1_500;
const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let database: TestDatabase;
let lockKey = 7_000;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

/** A test connection holding an advisory lock, so a handler that asks for it waits until it is released. */
async function holdLock(): Promise<{ key: number; release: () => Promise<void> }> {
  const key = (lockKey += 1);
  const holder = await database.pool.connect();
  await holder.query('SELECT pg_advisory_lock($1)', [key]);
  return {
    key,
    release: async () => {
      await holder.query('SELECT pg_advisory_unlock($1)', [key]);
      holder.release();
    },
  };
}

/** A running bus whose transactional handler is blocked in PostgreSQL on the advisory lock `key`. */
async function startBlockedBus(
  store: PostgresEventStore,
  key: number,
): Promise<{ bus: RelayBus<PoolClient>; failures: unknown[] }> {
  const bus = new RelayBus<PoolClient>({
    store,
    logger: silent,
    transactionMode: 'optional',
    dispatcher: { pollInterval: 20, leaseDuration: LEASE_MS, shutdownTimeout: TIMEOUT_MS },
  });
  const failures: unknown[] = [];
  let started = false;
  bus.subscribe(
    OrderPlaced,
    async (_event, context) => {
      started = true;
      try {
        await context.transaction!.query('SELECT pg_advisory_xact_lock($1)', [key]);
      } catch (error) {
        failures.push(error);
        throw error;
      }
    },
    { id: 'blocked', transactional: true },
  );
  await bus.start();
  await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
  await waitFor(() => started);
  // Give the lock request time to reach the server.
  await new Promise((resolve) => setTimeout(resolve, 50));
  return { bus, failures };
}

async function timedStop(bus: RelayBus<PoolClient>): Promise<StopResult & { elapsed: number }> {
  const started = Date.now();
  const result = await bus.stop();
  return { ...result, elapsed: Date.now() - started };
}

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
): Promise<void> {
  const started = Date.now();
  while (!(await condition())) {
    if (Date.now() - started > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

describe('stop deadline on PostgreSQL', () => {
  it('force-closes its own pool when a query hangs, and the delivery runs again after the lease', async () => {
    const schema = uniqueSchema();
    const lock = await holdLock();
    const store = new PostgresEventStore({
      connectionString: database.connectionString,
      poolConfig: { application_name: schema },
      schema,
    });
    const { bus, failures } = await startBlockedBus(store, lock.key);

    const result = await timedStop(bus);

    expect(result).toMatchObject({ graceful: false, abandonedDeliveries: 1 });
    expect(result.elapsed).toBeLessThan(TIMEOUT_MS + EPSILON_MS);
    // The blocked query failed on the client at once: its connection was destroyed.
    await waitFor(() => failures.length === 1);
    expect(String(failures[0])).toMatch(/Connection terminated/);
    await lock.release();
    await waitFor(async () => {
      const { rows } = await database.pool.query<{ count: string }>(
        'SELECT count(*) FROM pg_stat_activity WHERE application_name = $1',
        [schema],
      );
      return rows[0]!.count === '0';
    });

    const next = new RelayBus<PoolClient>({
      store: new PostgresEventStore({ pool: database.pool, schema }),
      logger: silent,
      dispatcher: { enabled: false, leaseDuration: LEASE_MS },
    });
    const attempts: number[] = [];
    next.subscribe(OrderPlaced, (_event, context) => void attempts.push(context.attempt), {
      id: 'blocked',
      transactional: true,
    });
    await next.start();
    await waitFor(async () => (await next.runOnce()) === 1);
    expect(attempts).toEqual([2]);
    await next.stop();
  }, 30_000);

  it('never ends or cuts a pool the application passed in', async () => {
    const schema = uniqueSchema();
    const lock = await holdLock();
    const pool = createTestPool(database.connectionString, 5);
    const { bus, failures } = await startBlockedBus(
      new PostgresEventStore({ pool, schema }),
      lock.key,
    );

    const result = await timedStop(bus);

    expect(result).toMatchObject({ graceful: false, abandonedDeliveries: 1 });
    expect(result.elapsed).toBeLessThan(TIMEOUT_MS + EPSILON_MS);
    expect(pool.ended).toBe(false);
    expect(failures).toEqual([]);
    await lock.release();
    await expect(pool.query('SELECT 1 AS one')).resolves.toMatchObject({ rows: [{ one: 1 }] });
    await waitFor(() => pool.idleCount === pool.totalCount);
    await pool.end();
  }, 30_000);
});
