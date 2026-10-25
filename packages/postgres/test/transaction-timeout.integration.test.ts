import { setTimeout as sleep } from 'node:timers/promises';
import type { PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, TransactionTimeoutError, defineEvent } from '@relaybus/core';
import { PgExecutor, PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import {
  POSTGRES_IMAGES,
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const quiet = { log: () => undefined, warn: () => undefined, error: () => undefined };
const BUDGET = '400ms';

describe.each(POSTGRES_IMAGES)('transaction time budget on %s', (image) => {
  const supportsTransactionTimeout = /postgres:(1[7-9]|[2-9]\d)/.test(image);
  let database: TestDatabase;

  beforeAll(async () => {
    database = await startDatabase(image);
  });

  afterAll(async () => {
    await database.stop();
  });

  async function createBus(
    executor?: PostgresExecutor<PoolClient>,
  ): Promise<{ bus: RelayBus<PoolClient>; store: PostgresEventStore<PoolClient> }> {
    const store = new PostgresEventStore<PoolClient>({
      schema: uniqueSchema(),
      logger: quiet,
      ...(executor === undefined ? { pool: database.pool } : { executor }),
    });
    await store.setup();
    const bus = new RelayBus<PoolClient>({
      store,
      transactionTimeout: BUDGET,
      dispatcher: { enabled: false },
      logger: quiet,
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    return { bus, store };
  }

  async function settingsInside(bus: RelayBus<PoolClient>): Promise<Record<string, unknown>> {
    return bus.transaction(async (client) => {
      const { rows } = await client.query<Record<string, unknown>>(
        "SELECT current_setting('statement_timeout') AS statement, " +
          "current_setting('idle_in_transaction_session_timeout') AS idle, " +
          "current_setting('transaction_timeout', true) AS total",
      );
      return rows[0]!;
    });
  }

  it('sets the budget for the transaction only', async () => {
    const pool = createTestPool(database.connectionString, 1);
    try {
      const store = new PostgresEventStore<PoolClient>({
        pool,
        schema: uniqueSchema(),
        logger: quiet,
      });
      await store.setup();
      const bus = new RelayBus<PoolClient>({
        store,
        transactionTimeout: BUDGET,
        dispatcher: { enabled: false },
        logger: quiet,
      });

      expect(await settingsInside(bus)).toEqual({
        statement: '400ms',
        idle: '400ms',
        total: supportsTransactionTimeout ? '400ms' : null,
      });
      // The single pooled connection is back to the server defaults.
      const { rows } = await pool.query<{ statement: string; idle: string }>(
        "SELECT current_setting('statement_timeout') AS statement, " +
          "current_setting('idle_in_transaction_session_timeout') AS idle",
      );
      expect(rows[0]).toEqual({ statement: '0', idle: '0' });
    } finally {
      await pool.end();
    }
  });

  it('cancels a statement that runs past the budget and rolls back', async () => {
    const { bus, store } = await createBus();

    const error = await bus
      .transaction(async (client) => {
        await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
        await client.query('SELECT pg_sleep(5)');
      })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    // statement_timeout (57014), or on PostgreSQL 17+ transaction_timeout (25P04) at the same instant.
    expect(['57014', '25P04']).toContain(((error as Error).cause as { code?: string }).code);
    expect(await store.listDeliveries({})).toEqual([]);
  });

  it('ends a transaction left idle by a slow external call, without crashing the process', async () => {
    const { bus, store } = await createBus();

    const error = await bus
      .transaction(async (client) => {
        await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
        await sleep(1_500); // a slow HTTP call inside the transaction
        await client.query('SELECT 1');
      })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    expect((error as Error).message).toMatch(/slow call to another service/);
    expect((error as Error).cause).toBeInstanceOf(Error);
    expect(await store.listDeliveries({})).toEqual([]);
    // The pool replaced the ended connection and keeps working.
    await bus.transaction(async () => {
      await bus.publish(new OrderPlaced({ orderId: 'o-2' }));
    });
    expect(await store.listDeliveries({})).toHaveLength(1);
  });

  it('bounds the whole transaction even when every step is short', async () => {
    const { bus, store } = await createBus();

    const error = await bus
      .transaction(async (client) => {
        await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
        for (let step = 0; step < 8; step += 1) {
          await client.query('SELECT pg_sleep(0.05)');
          await sleep(50);
        }
      })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TransactionTimeoutError);
    // PostgreSQL 17+ ends the session itself (transaction_timeout); older servers leave the
    // overrun to the bus, which rolls back before the commit.
    expect((error as Error).cause !== undefined).toBe(supportsTransactionTimeout);
    expect(await store.listDeliveries({})).toEqual([]);
  });

  describe('on a pool the store created', () => {
    async function createOwnedBus(
      options: { readonly preparedStatements?: boolean } = {},
    ): Promise<{ bus: RelayBus<PoolClient>; store: PostgresEventStore<PoolClient> }> {
      // One connection, so every transaction below runs on the same session.
      const store = new PostgresEventStore<PoolClient>({
        connectionString: database.connectionString,
        poolConfig: { max: 1 },
        schema: uniqueSchema(),
        logger: quiet,
        ...options,
      });
      await store.setup();
      const bus = new RelayBus<PoolClient>({
        store,
        transactionTimeout: BUDGET,
        dispatcher: { enabled: false },
        logger: quiet,
      });
      bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
      return { bus, store };
    }

    const sessionSettings = (
      store: PostgresEventStore<PoolClient>,
    ): Promise<Record<string, unknown>> =>
      store.transaction(async (client) => {
        const { rows } = await client.query<Record<string, unknown>>(
          "SELECT current_setting('statement_timeout') AS statement, " +
            "current_setting('idle_in_transaction_session_timeout') AS idle",
        );
        return rows[0]!;
      });

    it('keeps the budget on the connection and still cancels a statement past it', async () => {
      const { bus, store } = await createOwnedBus();
      try {
        expect(await settingsInside(bus)).toEqual({
          statement: '400ms',
          idle: '400ms',
          total: supportsTransactionTimeout ? '400ms' : null,
        });
        // The connection keeps the budget: this transaction only sends BEGIN, and is still held to it.
        const error = await bus
          .transaction(async (client) => {
            await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
            await client.query('SELECT pg_sleep(5)');
          })
          .catch((caught: unknown) => caught);

        expect(error).toBeInstanceOf(TransactionTimeoutError);
        expect(['57014', '25P04']).toContain(((error as Error).cause as { code?: string }).code);
        expect(await store.listDeliveries({})).toEqual([]);
        expect(await settingsInside(bus)).toMatchObject({ statement: '400ms', idle: '400ms' });
      } finally {
        await store.close();
      }
    });

    it('gives a transaction without a budget the session defaults, and the next budget back', async () => {
      const { bus, store } = await createOwnedBus();
      try {
        await settingsInside(bus);
        // A transactional handler or setup() runs without a budget, as it did before.
        expect(await sessionSettings(store)).toEqual({ statement: '0', idle: '0' });
        expect(await sessionSettings(store)).toEqual({ statement: '0', idle: '0' });
        expect(await settingsInside(bus)).toMatchObject({ statement: '400ms', idle: '400ms' });
      } finally {
        await store.close();
      }
    });

    it('sets the budget again after a transaction that set it rolled back', async () => {
      const { bus, store } = await createOwnedBus();
      try {
        await expect(
          bus.transaction(() => Promise.reject(new Error('rolled back'))),
        ).rejects.toThrow('rolled back');
        expect(await settingsInside(bus)).toMatchObject({ statement: '400ms', idle: '400ms' });
      } finally {
        await store.close();
      }
    });

    it('sets the budget again after a transaction whose COMMIT answered ROLLBACK', async () => {
      const { bus, store } = await createOwnedBus();
      try {
        // The work swallows a failed statement: the transaction is aborted and COMMIT rolls it back.
        await bus.transaction(async (client) => {
          await client.query('SELECT 1 / 0').catch(() => undefined);
        });
        expect(await settingsInside(bus)).toMatchObject({ statement: '400ms', idle: '400ms' });
      } finally {
        await store.close();
      }
    });

    it('resets every transaction without a budget once a pooler was noticed', async () => {
      const pool = createTestPool(database.connectionString, 1);
      try {
        const executor = new PgExecutor(pool, { logger: quiet, sessionBudget: true });
        await executor.transactionWithBudget(() => Promise.resolve(), 400);
        // A server session that lost the prepared statement looks like a pooler in transaction mode.
        await executor.query('SELECT 1', [], undefined, { prepare: true });
        await pool.query('DEALLOCATE ALL');
        await executor.query('SELECT 1', [], undefined, { prepare: true });
        const settings = (): Promise<unknown> =>
          executor.transaction(async (client) => {
            const { rows } = await client.query<{ s: string }>(
              "SELECT current_setting('statement_timeout') AS s",
            );
            return rows[0];
          });
        expect(await settings()).toEqual({ s: '0' });
        // The budget is transaction-scoped from now on, and the next transaction resets again.
        await pool.query("SET statement_timeout = '7s'");
        expect(await settings()).toEqual({ s: '0' });
      } finally {
        await pool.end();
      }
    });

    it('scopes the budget to the transaction with preparedStatements: false (a pooler)', async () => {
      const { bus, store } = await createOwnedBus({ preparedStatements: false });
      try {
        expect(await settingsInside(bus)).toMatchObject({ statement: '400ms', idle: '400ms' });
        expect(await sessionSettings(store)).toEqual({ statement: '0', idle: '0' });
      } finally {
        await store.close();
      }
    });
  });

  it('sets only statement_timeout for an executor that does not survive a server-ended session', async () => {
    const pg = new PgExecutor(database.pool, { logger: quiet });
    const executor: PostgresExecutor<PoolClient> = {
      transaction: (work) => pg.transaction(work),
      query: (sql, parameters, transaction, options) =>
        pg.query(sql, parameters, transaction, options),
    };
    const { bus } = await createBus(executor);

    expect(await settingsInside(bus)).toEqual({
      statement: '400ms',
      idle: '0',
      total: supportsTransactionTimeout ? '0' : null,
    });
  });
});
