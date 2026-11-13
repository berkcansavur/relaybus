import { once } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import { Client, Pool, type PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, TransactionTimeoutError, defineEvent } from '@relaybus/core';
import { PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import { createDrizzleExecutor, type DrizzleQueryable } from '../src/drizzle/index.js';
import { startDatabase, uniqueSchema, type TestDatabase } from './support/postgres-container.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const quiet = { log: () => undefined, warn: () => undefined, error: () => undefined };
const BUDGET = '400ms';

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

function singleConnectionPool(): Pool {
  const pool = new Pool({
    connectionString: database.connectionString,
    max: 1,
    connectionTimeoutMillis: 1_000,
  });
  pool.on('error', () => undefined);
  return pool;
}

function failStatement(pool: Pool, text: string): void {
  pool.on('acquire', (client: PoolClient) => {
    const query = client.query.bind(client) as (...args: unknown[]) => Promise<unknown>;
    (client as unknown as { query: unknown }).query = (...args: unknown[]) => {
      const first = args[0] as string | { text: string };
      const statement = typeof first === 'string' ? first : first.text;
      return statement.toLowerCase() === text
        ? Promise.reject(new Error(`${text} failed`))
        : query(...args);
    };
  });
}

async function backendPid(transaction: DrizzleQueryable): Promise<number> {
  const { rows } = await transaction.execute(sql`SELECT pg_backend_pid() AS pid`);
  return rows[0]!['pid'] as number;
}

async function terminate(pid: number): Promise<void> {
  await database.pool.query('SELECT pg_terminate_backend($1)', [pid]);
}

describe('createDrizzleExecutor on a pg pool', () => {
  it('releases the connection when BEGIN fails', async () => {
    const pool = singleConnectionPool();
    try {
      failStatement(pool, 'begin');
      const executor = createDrizzleExecutor(drizzle(pool));

      await expect(executor.transaction(async () => 'never')).rejects.toThrow(/begin/);
      const client = await pool.connect();
      client.release();
    } finally {
      await pool.end();
    }
  });

  it('destroys the connection when ROLLBACK fails', async () => {
    const pool = singleConnectionPool();
    try {
      failStatement(pool, 'rollback');
      const executor = createDrizzleExecutor(drizzle(pool));

      await expect(
        executor.transaction(async () => {
          throw new Error('work failed');
        }),
      ).rejects.toThrow(/rollback/);
      expect(pool.totalCount).toBe(0);
    } finally {
      await pool.end();
    }
  });

  it('destroys the connection once when it is lost during the ROLLBACK after a failed one', async () => {
    const pool = singleConnectionPool();
    try {
      let rollbacks = 0;
      pool.on('acquire', (client: PoolClient) => {
        const query = client.query.bind(client) as (...args: unknown[]) => Promise<unknown>;
        (client as unknown as { query: unknown }).query = async (...args: unknown[]) => {
          const first = args[0] as string | { text: string };
          if ((typeof first === 'string' ? first : first.text).toLowerCase() !== 'rollback') {
            return query(...args);
          }
          rollbacks += 1;
          if (rollbacks === 1) {
            throw new Error('rollback failed');
          }
          const { rows } = (await query('SELECT pg_backend_pid() AS pid')) as {
            rows: { pid: number }[];
          };
          const lost = once(client, 'error');
          await terminate(rows[0]!.pid);
          await lost;
          throw new Error('connection lost');
        };
      });
      const executor = createDrizzleExecutor(drizzle(pool));

      await expect(
        executor.transaction(async () => {
          throw new Error('work failed');
        }),
      ).rejects.toThrow(/rollback/);
      expect(rollbacks).toBe(2);
      expect(pool.totalCount).toBe(0);
    } finally {
      await pool.end();
    }
  });

  it('keeps the connection when COMMIT fails and the rollback after it works', async () => {
    const pool = singleConnectionPool();
    try {
      const db = drizzle(pool);
      const executor = createDrizzleExecutor(db);
      const schema = uniqueSchema();
      await db.execute(sql.raw(`CREATE SCHEMA "${schema}"`));
      await db.execute(
        sql.raw(`CREATE TABLE "${schema}".orders (id text UNIQUE DEFERRABLE INITIALLY DEFERRED)`),
      );

      let pid = 0;
      const error = await executor
        .transaction(async (transaction) => {
          pid = await backendPid(transaction);
          await transaction.execute(sql.raw(`INSERT INTO "${schema}".orders VALUES ('o'), ('o')`));
        })
        .catch((caught: unknown) => caught);

      expect((error as { cause?: { code?: string } }).cause?.code).toBe('23505');
      const client = await pool.connect();
      try {
        expect((await client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid).toBe(pid);
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  });

  it('keeps the connection after the work fails and Drizzle rolled back', async () => {
    const pool = singleConnectionPool();
    try {
      const executor = createDrizzleExecutor(drizzle(pool));
      let pid = 0;

      await expect(
        executor.transaction(async (transaction) => {
          pid = await backendPid(transaction);
          throw new Error('work failed');
        }),
      ).rejects.toThrow('work failed');
      expect(await executor.transaction(backendPid)).toBe(pid);
    } finally {
      await pool.end();
    }
  });

  it('fails the transaction without crashing when the connection is lost while the work awaits something else', async () => {
    const pool = singleConnectionPool();
    try {
      const executor = createDrizzleExecutor(drizzle(pool));

      const error = await executor
        .transaction(async (transaction) => {
          await terminate(await backendPid(transaction));
          await sleep(300);
          return 'done';
        })
        .catch((caught: unknown) => caught);

      expect(error).toBeInstanceOf(Error);
      expect(await executor.transaction(async () => 'next')).toBe('next');
    } finally {
      await pool.end();
    }
  });

  it('fails the transaction without crashing when the connection is lost under a statement', async () => {
    const pool = singleConnectionPool();
    try {
      const executor = createDrizzleExecutor(drizzle(pool));

      const error = await executor
        .transaction(async (transaction) => {
          const pid = await backendPid(transaction);
          await Promise.all([transaction.execute(sql`SELECT pg_sleep(5)`), terminate(pid)]);
        })
        .catch((caught: unknown) => caught);

      expect(error).toBeInstanceOf(Error);
      expect(await executor.transaction(async () => 'next')).toBe('next');
    } finally {
      await pool.end();
    }
  });

  describe('under a transaction budget', () => {
    async function createBus(pool: Pool): Promise<{
      bus: RelayBus<DrizzleQueryable>;
      store: PostgresEventStore<DrizzleQueryable>;
    }> {
      const store = new PostgresEventStore<DrizzleQueryable>({
        executor: createDrizzleExecutor(drizzle(pool)),
        schema: uniqueSchema(),
        logger: quiet,
      });
      await store.setup();
      const bus = new RelayBus<DrizzleQueryable>({
        store,
        transactionTimeout: BUDGET,
        dispatcher: { enabled: false },
        logger: quiet,
      });
      bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
      return { bus, store };
    }

    it('sets the session timeouts as well as statement_timeout', async () => {
      const pool = singleConnectionPool();
      try {
        const { bus } = await createBus(pool);

        const settings = await bus.transaction(async (transaction) => {
          const { rows } = await transaction.execute(
            sql`SELECT current_setting('statement_timeout') AS statement, current_setting('idle_in_transaction_session_timeout') AS idle`,
          );
          return rows[0];
        });

        expect(settings).toEqual({ statement: '400ms', idle: '400ms' });
      } finally {
        await pool.end();
      }
    });

    it('ends a transaction left idle by a slow external call, without crashing the process', async () => {
      const pool = singleConnectionPool();
      try {
        const { bus, store } = await createBus(pool);
        let pid = 0;

        const error = await bus
          .transaction(async (transaction) => {
            pid = await backendPid(transaction);
            await bus.publish(new OrderPlaced({ orderId: 'o-1' }));
            await sleep(1_500);
            await transaction.execute(sql`SELECT 1`);
          })
          .catch((caught: unknown) => caught);

        expect(error).toBeInstanceOf(TransactionTimeoutError);
        expect(await store.listDeliveries({})).toEqual([]);
        const next = await bus.transaction(async (transaction) => {
          await bus.publish(new OrderPlaced({ orderId: 'o-2' }));
          return backendPid(transaction);
        });
        expect(next).not.toBe(pid);
        expect(await store.listDeliveries({})).toHaveLength(1);
      } finally {
        await pool.end();
      }
    });
  });
});

describe('createDrizzleExecutor on a pool it recognizes', () => {
  async function checkoutAfterFailedBegin(pool: Pool, drizzlePool: Pool): Promise<void> {
    failStatement(pool, 'begin');
    const executor = createDrizzleExecutor(drizzle(drizzlePool));
    expect(executor.sessionTimeouts).toBe(true);
    await expect(executor.transaction(async () => 'never')).rejects.toThrow(/begin/);
    const client = await pool.connect();
    client.release();
  }

  it('takes a subclass of the pg pool', async () => {
    class InstrumentedPool extends Pool {}
    const pool = new InstrumentedPool({
      connectionString: database.connectionString,
      max: 1,
      connectionTimeoutMillis: 1_000,
    });
    try {
      await checkoutAfterFailedBegin(pool, pool);
    } finally {
      await pool.end();
    }
  });

  it('takes a pool of another copy of pg by its class name', async () => {
    class ForeignPool {
      constructor(private readonly pool: Pool) {}
      connect(): Promise<PoolClient> {
        return this.pool.connect();
      }
    }
    const pool = singleConnectionPool();
    try {
      await checkoutAfterFailedBegin(pool, new ForeignPool(pool) as unknown as Pool);
    } finally {
      await pool.end();
    }
  });
});

describe('createDrizzleExecutor on anything but a pg pool', () => {
  it('leaves a transaction the application wraps to the wrapper', async () => {
    const pool = singleConnectionPool();
    try {
      const db = drizzle(pool);
      const transaction = db.transaction.bind(db);
      let wrapped = 0;
      db.transaction = ((...args: Parameters<typeof transaction>) => {
        wrapped += 1;
        return transaction(...args);
      }) as typeof db.transaction;
      const executor = createDrizzleExecutor(db);

      expect(executor.sessionTimeouts).toBeUndefined();
      expect(await executor.transaction(async () => 'done')).toBe('done');
      expect(wrapped).toBe(1);
    } finally {
      await pool.end();
    }
  });

  it('leaves a transaction to a Drizzle session that runs its own', async () => {
    const pool = singleConnectionPool();
    try {
      const db = drizzle(pool);
      const session = (
        db as unknown as { session: { transaction: (...args: unknown[]) => unknown } }
      ).session;
      const transaction = session.transaction.bind(session);
      let wrapped = 0;
      session.transaction = (...args: unknown[]) => {
        wrapped += 1;
        return transaction(...args);
      };
      const executor = createDrizzleExecutor(db);

      expect(executor.sessionTimeouts).toBeUndefined();
      expect(await executor.transaction(async () => 'done')).toBe('done');
      expect(wrapped).toBe(1);
    } finally {
      await pool.end();
    }
  });

  it('leaves the transaction to Drizzle when its connections look like pools to Drizzle', async () => {
    class PoolClient extends Client {}
    const pool = new Pool({
      connectionString: database.connectionString,
      max: 1,
      Client: PoolClient,
    });
    try {
      const executor = createDrizzleExecutor(drizzle(pool));

      expect(executor.sessionTimeouts).toBeUndefined();
      expect(await executor.transaction(async () => 'done')).toBe('done');
    } finally {
      await pool.end();
    }
  });

  it('takes a database or a client without a prototype', () => {
    const queryable: DrizzleQueryable = { execute: async () => ({ rows: [] }) };
    const bare = Object.assign(Object.create(null) as object, {
      ...queryable,
      transaction: <T>(work: (transaction: DrizzleQueryable) => Promise<T>) => work(queryable),
    });

    expect(createDrizzleExecutor(bare).sessionTimeouts).toBeUndefined();
    expect(
      createDrizzleExecutor(drizzle({ client: Object.create(null) as Pool })).sessionTimeouts,
    ).toBeUndefined();
  });

  it('leaves the transaction to anything that only looks like Drizzle', async () => {
    const rows = [{ one: 1 }];
    const queryable: DrizzleQueryable = { execute: async () => ({ rows }) };
    const executor = createDrizzleExecutor({
      ...queryable,
      transaction: (work) => work(queryable),
    });

    expect(executor.sessionTimeouts).toBeUndefined();
    expect(
      await executor.transaction((transaction) => executor.query('SELECT 1', [], transaction)),
    ).toEqual(rows);
  });

  it('leaves the transaction to Drizzle and does not ask for the session timeouts', async () => {
    const client = new Client({ connectionString: database.connectionString });
    await client.connect();
    try {
      const executor: PostgresExecutor<DrizzleQueryable> = createDrizzleExecutor(drizzle(client));

      expect(executor.sessionTimeouts).toBeUndefined();
      expect(await executor.transaction(async (transaction) => backendPid(transaction))).toBe(
        (await client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid,
      );
    } finally {
      await client.end();
    }
  });
});
