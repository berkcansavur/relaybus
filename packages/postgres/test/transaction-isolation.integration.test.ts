import type { Pool, PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, defineEvent, type TransactionIsolationLevel } from '@relaybus/core';
import { PgExecutor, PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import {
  POSTGRES_IMAGES,
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

class LimitReachedError extends Error {}

const quiet = { log: () => undefined, warn: () => undefined, error: () => undefined };

/**
 * The scenario isolation levels exist for: each of two concurrent transactions counts the reservations
 * of a user, checks the limit (one) and inserts. Under READ COMMITTED both pass the check (write skew).
 */
describe.each(POSTGRES_IMAGES)('transactions at an isolation level on %s', (image) => {
  let database: TestDatabase;

  beforeAll(async () => {
    database = await startDatabase(image);
  });

  afterAll(async () => {
    await database.stop();
  });

  interface Fixture {
    readonly bus: RelayBus<PoolClient>;
    readonly store: PostgresEventStore<PoolClient>;
    readonly schema: string;
    readonly pool: Pool;
    readonly statements: string[];
  }

  /** A store on a pool whose connections record the text of every statement they send. */
  async function createFixture(
    options: {
      readonly executor?: (pool: Pool) => PostgresExecutor<PoolClient>;
      readonly poolSize?: number;
    } = {},
  ): Promise<Fixture> {
    const schema = uniqueSchema();
    const statements: string[] = [];
    const pool = createTestPool(database.connectionString, options.poolSize ?? 10);
    pool.on('connect', (client) => {
      const query = client.query.bind(client) as (...arguments_: unknown[]) => unknown;
      (client as { query: unknown }).query = (...arguments_: unknown[]) => {
        const [first] = arguments_;
        statements.push(typeof first === 'string' ? first : (first as { text: string }).text);
        return query(...arguments_);
      };
    });
    const store = new PostgresEventStore<PoolClient>({
      schema,
      logger: quiet,
      ...(options.executor === undefined ? { pool } : { executor: options.executor(pool) }),
    });
    await store.setup();
    await pool.query(
      `CREATE TABLE "${schema}".reservations (id text PRIMARY KEY, user_id text NOT NULL)`,
    );
    const bus = new RelayBus<PoolClient>({
      store,
      transactionTimeout: '5s',
      dispatcher: { enabled: false },
      logger: quiet,
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    return { bus, store, schema, pool, statements };
  }

  /** Lets `parties` callers through together once all of them arrived; later callers pass at once. */
  function barrier(parties: number): () => Promise<void> {
    let arrived = 0;
    let release!: () => void;
    const open = new Promise<void>((resolve) => (release = resolve));
    return async () => {
      arrived += 1;
      if (arrived >= parties) {
        release();
      }
      await open;
    };
  }

  async function reservations(fixture: Fixture): Promise<string[]> {
    const { rows } = await fixture.pool.query<{ id: string }>(
      `SELECT id FROM "${fixture.schema}".reservations ORDER BY id`,
    );
    return rows.map((row) => row.id);
  }

  /** Count the user's reservations, check the limit, insert and publish: the application's code. */
  function reserve(fixture: Fixture, { id, level, arrive, attempts }: ReserveInput): Promise<void> {
    return fixture.bus.transaction(
      async (client) => {
        attempts.push(id);
        const { rows } = await client.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM "${fixture.schema}".reservations WHERE user_id = 'u1'`,
        );
        await arrive();
        if (rows[0]!.n >= 1) {
          throw new LimitReachedError(`limit reached for ${id}`);
        }
        await client.query(
          `INSERT INTO "${fixture.schema}".reservations (id, user_id) VALUES ($1, 'u1')`,
          [id],
        );
        await fixture.bus.publish(new OrderPlaced({ orderId: id }));
      },
      level === undefined ? undefined : { isolationLevel: level },
    );
  }

  interface ReserveInput {
    readonly id: string;
    readonly level: TransactionIsolationLevel | undefined;
    readonly arrive: () => Promise<void>;
    readonly attempts: string[];
  }

  it.each([undefined, 'read committed'] as const)(
    'lets both concurrent reservations pass the limit check at the database default (%s)',
    async (level) => {
      const fixture = await createFixture();
      const arrive = barrier(2);
      const attempts: string[] = [];

      await Promise.all([
        reserve(fixture, { id: 'a', level, arrive, attempts }),
        reserve(fixture, { id: 'b', level, arrive, attempts }),
      ]);

      expect(await reservations(fixture)).toEqual(['a', 'b']);
      await fixture.pool.end();
    },
  );

  it.each(['repeatable read', 'serializable'] as const)(
    'rejects the second reservation with the application error after a rerun at %s',
    async (level) => {
      const fixture = await createFixture();
      const arrive = barrier(2);
      const attempts: string[] = [];

      const outcomes = await Promise.allSettled([
        reserve(fixture, { id: 'a', level, arrive, attempts }),
        reserve(fixture, { id: 'b', level, arrive, attempts }),
      ]);

      if (level === 'serializable') {
        const rejected = outcomes.filter((outcome) => outcome.status === 'rejected');
        expect(rejected).toHaveLength(1);
        expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(LimitReachedError);
        // The loser ran twice: the serialization failure, then the rerun that saw the other's row.
        expect(attempts).toHaveLength(3);
        expect(await reservations(fixture)).toHaveLength(1);
        // Only the winner's event: the loser's first attempt was rolled back with its transaction.
        expect(await fixture.store.listDeliveries({})).toHaveLength(1);
      } else {
        // Repeatable read does not stop write skew (disjoint writes): it is the same as read committed here.
        expect(outcomes.map((outcome) => outcome.status)).toEqual(['fulfilled', 'fulfilled']);
      }
      await fixture.pool.end();
    },
  );

  /** A transaction that always loses against a concurrent update of the same row. */
  async function conflictingTransaction(
    fixture: Fixture,
    {
      level,
      maxAttempts,
      runs,
    }: {
      readonly level: TransactionIsolationLevel;
      readonly maxAttempts?: number | undefined;
      readonly runs: number[];
    },
  ): Promise<void> {
    await fixture.pool.query(
      `CREATE TABLE "${fixture.schema}".counters (id int PRIMARY KEY, n int)`,
    );
    await fixture.pool.query(`INSERT INTO "${fixture.schema}".counters VALUES (1, 0)`);
    await fixture.bus.transaction(
      async (client) => {
        runs.push(runs.length + 1);
        await client.query(`SELECT n FROM "${fixture.schema}".counters`); // takes the snapshot
        await fixture.bus.publish(new OrderPlaced({ orderId: `attempt-${runs.length}` }));
        await fixture.pool.query(`UPDATE "${fixture.schema}".counters SET n = n + 1`);
        await client.query(`UPDATE "${fixture.schema}".counters SET n = n + 1`);
      },
      { isolationLevel: level, ...(maxAttempts === undefined ? {} : { maxAttempts }) },
    );
  }

  it.each([
    ['repeatable read', undefined, 3],
    ['serializable', 2, 2],
    ['serializable', 1, 1],
  ] as const)(
    'rethrows the serialization failure unchanged once the attempts are used up (%s, maxAttempts %s)',
    async (level, maxAttempts, expectedRuns) => {
      const fixture = await createFixture();
      const runs: number[] = [];

      const error = await conflictingTransaction(fixture, { level, maxAttempts, runs }).catch(
        (caught: unknown) => caught,
      );

      expect(error).toMatchObject({ code: '40001', severity: 'ERROR' });
      expect((error as Error).message).toMatch(/could not serialize access/);
      expect(runs).toHaveLength(expectedRuns);
      // No event of any attempt survived, and the store's wake-up never fired for them.
      expect(await fixture.store.listDeliveries({})).toEqual([]);
      const { rows } = await fixture.pool.query<{ n: number }>(
        `SELECT n FROM "${fixture.schema}".counters`,
      );
      expect(rows[0]!.n).toBe(expectedRuns);
      await fixture.pool.end();
    },
  );

  it('does not wake the dispatcher for the events of a failed attempt', async () => {
    const fixture = await createFixture();
    let notifications = 0;
    const stop = await fixture.store.watchNewDeliveries(() => {
      notifications += 1;
    });
    expect(stop).toBeDefined();
    try {
      await fixture.pool.query(
        `CREATE TABLE "${fixture.schema}".counters (id int PRIMARY KEY, n int)`,
      );
      await fixture.pool.query(`INSERT INTO "${fixture.schema}".counters VALUES (1, 0)`);
      let run = 0;

      await fixture.bus.transaction(
        async (client) => {
          run += 1;
          await client.query(`SELECT n FROM "${fixture.schema}".counters`);
          await fixture.bus.publish(new OrderPlaced({ orderId: `attempt-${run}` }));
          if (run === 1) {
            await fixture.pool.query(`UPDATE "${fixture.schema}".counters SET n = n + 1`);
          }
          await client.query(`UPDATE "${fixture.schema}".counters SET n = n + 1`);
        },
        { isolationLevel: 'serializable' },
      );

      expect(run).toBe(2);
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(notifications).toBe(1);
      expect((await fixture.store.listDeliveries({})).map((d) => d.event.payload)).toEqual([
        { orderId: 'attempt-2' },
      ]);
    } finally {
      await stop!();
      await fixture.pool.end();
    }
  });

  describe('statements', () => {
    const settings = (value: string): string =>
      `SELECT set_config('statement_timeout', '${value}', true), ` +
      `set_config('idle_in_transaction_session_timeout', '${value}', true), ` +
      "CASE WHEN current_setting('server_version_num')::int >= 170000 " +
      `THEN set_config('transaction_timeout', '${value}', true) END`;

    it('sends exactly the statements of before without an isolation level', async () => {
      const fixture = await createFixture();
      await fixture.pool.query('SELECT 1'); // a connection that already ran the recorder's setup
      fixture.statements.length = 0;

      await fixture.bus.transaction(async (client) => {
        await client.query('SELECT 1');
      });
      await fixture.bus.transaction(async (client) => {
        await client.query('SELECT 1');
      }, {});

      expect(fixture.statements).toEqual([
        `BEGIN; ${settings('5000ms')}`,
        'SELECT 1',
        'COMMIT',
        `BEGIN; ${settings('5000ms')}`,
        'SELECT 1',
        'COMMIT',
      ]);
      await fixture.pool.end();
    });

    it('sets the level in the BEGIN round trip of PgExecutor, without an extra statement', async () => {
      const fixture = await createFixture();
      fixture.statements.length = 0;
      const inside: string[] = [];

      await fixture.bus.transaction(
        async (client) => {
          const { rows } = await client.query<{ level: string; timeout: string }>(
            "SELECT current_setting('transaction_isolation') AS level, current_setting('statement_timeout') AS timeout",
          );
          inside.push(rows[0]!.level, rows[0]!.timeout);
        },
        { isolationLevel: 'serializable' },
      );

      expect(fixture.statements).toHaveLength(3);
      expect(fixture.statements[0]).toBe(
        `BEGIN ISOLATION LEVEL SERIALIZABLE; ${settings('5000ms')}`,
      );
      expect(fixture.statements[2]).toBe('COMMIT');
      expect(inside).toEqual(['serializable', '5s']);
      await fixture.pool.end();
    });

    it.each(['read committed', 'repeatable read', 'serializable'] as const)(
      'runs a transaction at %s and returns the connection to the default level',
      async (level) => {
        const fixture = await createFixture({ poolSize: 1 });
        const levelOf = (client: PoolClient): Promise<string> =>
          client
            .query<{ level: string }>("SELECT current_setting('transaction_isolation') AS level")
            .then(({ rows }) => rows[0]!.level);

        const during = await fixture.bus.transaction(levelOf, { isolationLevel: level });
        const after = await fixture.bus.transaction(levelOf);

        expect(during).toBe(level);
        expect(after).toBe('read committed');
        await fixture.pool.end();
      },
    );

    it('holds the transaction to its budget as well', async () => {
      const fixture = await createFixture();

      const error = await fixture.bus
        .transaction(
          async (client) => {
            await client.query('SET LOCAL statement_timeout = 300');
            await client.query('SELECT pg_sleep(5)');
          },
          { isolationLevel: 'serializable' },
        )
        .catch((caught: unknown) => caught);

      expect((error as { code?: string }).code).toBe('57014');
      await fixture.pool.end();
    });

    it('keeps the budget of a pool of its own and the level per transaction', async () => {
      const schema = uniqueSchema();
      const store = new PostgresEventStore<PoolClient>({
        connectionString: database.connectionString,
        poolConfig: { max: 1 },
        schema,
        logger: quiet,
      });
      await store.setup();
      const bus = new RelayBus<PoolClient>({
        store,
        transactionTimeout: '5s',
        dispatcher: { enabled: false },
        logger: quiet,
      });
      const settingsOf = (client: PoolClient): Promise<unknown> =>
        client
          .query<{ level: string; timeout: string }>(
            "SELECT current_setting('transaction_isolation') AS level, current_setting('statement_timeout') AS timeout",
          )
          .then(({ rows }) => rows[0]);
      try {
        expect(await bus.transaction(settingsOf, { isolationLevel: 'serializable' })).toEqual({
          level: 'serializable',
          timeout: '5s',
        });
        expect(await bus.transaction(settingsOf, { isolationLevel: 'repeatable read' })).toEqual({
          level: 'repeatable read',
          timeout: '5s',
        });
        expect(await bus.transaction(settingsOf)).toEqual({
          level: 'read committed',
          timeout: '5s',
        });
        expect(await store.transaction(settingsOf)).toEqual({
          level: 'read committed',
          timeout: '0',
        });
      } finally {
        await store.close();
      }
    });

    it('works on an executor without transactionWithIsolation: SET TRANSACTION first, then the budget', async () => {
      const queries: string[] = [];
      const fixture = await createFixture({
        executor: (pool) => {
          const pg = new PgExecutor(pool, { logger: quiet });
          return {
            sessionTimeouts: true,
            transaction: (work) => pg.transaction(work),
            query: (sql, parameters, transaction, options) => {
              queries.push(sql);
              return pg.query(sql, parameters, transaction, options);
            },
          };
        },
      });
      queries.length = 0;

      const level = await fixture.bus.transaction(
        async (client) => {
          const { rows } = await client.query<{ level: string; timeout: string }>(
            "SELECT current_setting('transaction_isolation') AS level, current_setting('statement_timeout') AS timeout",
          );
          return rows[0];
        },
        { isolationLevel: 'serializable' },
      );

      expect(level).toEqual({ level: 'serializable', timeout: '5s' });
      expect(queries[0]).toBe('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE');
      expect(queries[1]).toMatch(/^SELECT set_config\('statement_timeout', \$1, true\)/);
      await fixture.pool.end();
    });

    it('is accepted after SET LOCAL statements but refused once a query ran, which set_config is', async () => {
      const fixture = await createFixture();
      // Documents why the store orders the statements as it does.
      const attempt = async (...before: string[]): Promise<unknown> => {
        const client = await fixture.pool.connect();
        try {
          await client.query('BEGIN');
          for (const sql of before) {
            await client.query(sql);
          }
          await client.query('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE');
          return undefined;
        } catch (error) {
          return error;
        } finally {
          await client.query('ROLLBACK');
          client.release();
        }
      };

      expect(await attempt("SET LOCAL statement_timeout = '4s'")).toBeUndefined();
      expect(await attempt("SELECT set_config('statement_timeout', '4s', true)")).toMatchObject({
        code: '25001',
      });
      expect(await attempt('SELECT 1')).toMatchObject({ code: '25001' });
      await fixture.pool.end();
    });
  });

  it('runs a transactional handler and the dispatcher unchanged next to isolated transactions', async () => {
    const fixture = await createFixture();
    const handled: string[] = [];
    fixture.bus.subscribe(
      OrderPlaced,
      (event) => {
        handled.push(event.payload.orderId);
      },
      { id: 'recorder' },
    );

    await fixture.bus.transaction(
      async () => {
        await fixture.bus.publish(new OrderPlaced({ orderId: 'o-1' }));
      },
      { isolationLevel: 'serializable' },
    );
    await fixture.bus.drain();

    expect(handled).toEqual(['o-1']);
    await fixture.pool.end();
  });
});
