import { EventEmitter } from 'node:events';
import { ConfigurationError, TRANSACTION_ISOLATION_LEVELS } from '@relaybus/core';
import type { Pool } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import {
  PgExecutor,
  PostgresEventStore,
  type PostgresExecutor,
  type PostgresParameter,
} from '../src/index.js';
import { withRootPool } from '../src/root-pool-executor.js';
import { isPostgresSerializationFailure } from '../src/serialization-failure.js';
import { isolationKeywords } from '../src/transaction-isolation.js';

const BUDGET_SETTINGS =
  "SELECT set_config('statement_timeout', '400ms', true), " +
  "set_config('idle_in_transaction_session_timeout', '400ms', true), " +
  "CASE WHEN current_setting('server_version_num')::int >= 170000 " +
  "THEN set_config('transaction_timeout', '400ms', true) END";

describe('isolationKeywords', () => {
  it.each([
    ['read committed', 'READ COMMITTED'],
    ['repeatable read', 'REPEATABLE READ'],
    ['serializable', 'SERIALIZABLE'],
  ] as const)('writes %s as SQL', (level, keywords) => {
    expect(isolationKeywords(level)).toBe(keywords);
  });

  it.each(['snapshot', 'serializable; DROP TABLE x', 'toString', '__proto__', ''])(
    'refuses %j before it reaches SQL text',
    (level) => {
      expect(() => isolationKeywords(level as never)).toThrow(ConfigurationError);
    },
  );
});

describe('isPostgresSerializationFailure', () => {
  const pgError = Object.assign(new Error('could not serialize access'), { code: '40001' });

  it('finds the SQLSTATE of a pg error, which Kysely and Knex pass through', () => {
    expect(isPostgresSerializationFailure(pgError)).toBe(true);
  });

  it('finds it in the driverError of a TypeORM QueryFailedError and in a Drizzle cause', () => {
    expect(isPostgresSerializationFailure({ message: 'query failed', driverError: pgError })).toBe(
      true,
    );
    expect(isPostgresSerializationFailure(new Error('Failed query', { cause: pgError }))).toBe(
      true,
    );
    expect(isPostgresSerializationFailure({ originalError: pgError })).toBe(true);
  });

  it('finds the write conflict code of Prisma, and the SQLSTATE in the meta of a raw query error', () => {
    expect(isPostgresSerializationFailure({ code: 'P2034' })).toBe(true);
    expect(isPostgresSerializationFailure({ code: 'P2010', meta: { code: '40001' } })).toBe(true);
  });

  it('finds the SQLSTATE a driver adapter nests', () => {
    expect(
      isPostgresSerializationFailure({
        code: 'P2010',
        meta: { driverAdapterError: { cause: { originalCode: '40001' } } },
      }),
    ).toBe(true);
    expect(isPostgresSerializationFailure({ originalCode: '40001' })).toBe(true);
  });

  it('finds it in the shapes the shipped adapters raise, as the integration tests saw them', () => {
    // Prisma with the pg driver adapter: DriverAdapterError { name, cause: { originalCode, kind, ... } }.
    expect(
      isPostgresSerializationFailure({
        name: 'DriverAdapterError',
        cause: {
          originalCode: '40001',
          originalMessage: 'could not serialize access',
          kind: 'postgres',
        },
      }),
    ).toBe(true);
    // Drizzle: DrizzleQueryError { query, params, cause: DatabaseError }.
    expect(isPostgresSerializationFailure({ query: 'insert', params: [], cause: pgError })).toBe(
      true,
    );
    // TypeORM: QueryFailedError { query, parameters, driverError, code, ... }.
    expect(
      isPostgresSerializationFailure({
        query: 'insert',
        parameters: [],
        driverError: pgError,
        code: '40001',
      }),
    ).toBe(true);
  });

  it.each([
    undefined,
    null,
    '40001',
    40001,
    new Error('plain'),
    { code: '23505' },
    { code: '40P01' },
    { code: 'P2002', meta: { target: ['id'] } },
    { cause: { cause: { code: '23505' } } },
    { meta: { driverAdapterError: { cause: { originalCode: '23505' } } } },
  ])('is false for %j', (error) => {
    expect(isPostgresSerializationFailure(error)).toBe(false);
  });

  it('stops looking in wrappers nested deeper than real ones, and in cycles', () => {
    let deep: unknown = pgError;
    for (let level = 0; level < 10; level += 1) {
      deep = { cause: deep };
    }
    expect(isPostgresSerializationFailure(deep)).toBe(false);
    const cyclic: { cause?: unknown } = {};
    cyclic.cause = cyclic;
    expect(isPostgresSerializationFailure(cyclic)).toBe(false);
  });
});

describe('PostgresEventStore.isSerializationFailure', () => {
  const quiet = { log: () => undefined, warn: () => undefined, error: () => undefined };

  function storeOn(executor: PostgresExecutor<string>): PostgresEventStore<string> {
    return new PostgresEventStore<string>({ executor, logger: quiet });
  }

  const executor = (extra: Partial<PostgresExecutor<string>> = {}): PostgresExecutor<string> => ({
    transaction: (work) => work('tx'),
    query: async () => [],
    ...extra,
  });

  it('lists every isolation level', () => {
    expect(storeOn(executor()).isolationLevels).toEqual(TRANSACTION_ISOLATION_LEVELS);
  });

  it('knows the SQLSTATE without help from the executor', () => {
    const store = storeOn(executor());
    expect(store.isSerializationFailure({ code: '40001' })).toBe(true);
    expect(store.isSerializationFailure({ code: '23505' })).toBe(false);
  });

  it('asks the executor about errors of a library it wraps in another shape', () => {
    const isSerializationFailure = vi.fn((error: unknown) => error === 'wrapped conflict');
    const store = storeOn(executor({ isSerializationFailure }));
    expect(store.isSerializationFailure('wrapped conflict')).toBe(true);
    expect(store.isSerializationFailure('something else')).toBe(false);
    expect(store.isSerializationFailure({ code: '40001' })).toBe(true);
  });
});

describe('a transaction at an isolation level on an executor without transactionWithIsolation', () => {
  const quiet = { log: () => undefined, warn: () => undefined, error: () => undefined };

  function recorder(extra: Partial<PostgresExecutor<string>> = {}) {
    const statements: Array<{ sql: string; parameters: readonly PostgresParameter[] }> = [];
    const executor: PostgresExecutor<string> = {
      transaction: (work) => work('tx'),
      query: async (sql, parameters) => {
        statements.push({ sql, parameters });
        return [];
      },
      ...extra,
    };
    return { statements, store: new PostgresEventStore<string>({ executor, logger: quiet }) };
  }

  it('sets the level first, then the budget, and runs the work', async () => {
    const { statements, store } = recorder();

    const result = await store.transaction(async () => 'done', {
      timeoutMs: 400,
      isolationLevel: 'serializable',
    });

    expect(result).toBe('done');
    expect(statements.map(({ sql }) => sql.split(',')[0])).toEqual([
      'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
      "SELECT set_config('statement_timeout'",
    ]);
    expect(statements[0]!.parameters).toEqual([]);
    expect(statements[1]!.parameters).toEqual(['400ms']);
  });

  it('adds the session timeouts for an executor that survives a server-ended session', async () => {
    const { statements, store } = recorder({ sessionTimeouts: true });

    await store.transaction(async () => undefined, {
      timeoutMs: 400,
      isolationLevel: 'repeatable read',
    });

    expect(statements[0]!.sql).toBe('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
    expect(statements[1]!.sql).toContain('idle_in_transaction_session_timeout');
  });

  it('refuses an unknown level before it opens a transaction', async () => {
    const transaction = vi.fn();
    const { store } = recorder({ transaction });

    await expect(
      store.transaction(async () => 'done', {
        timeoutMs: 400,
        isolationLevel: 'snapshot' as never,
      }),
    ).rejects.toThrow(ConfigurationError);
    expect(transaction).not.toHaveBeenCalled();
  });

  it('leaves a transaction without a level to transactionWithBudget as before', async () => {
    const transactionWithBudget = vi.fn((work: (transaction: string) => Promise<unknown>) =>
      work('budgeted'),
    ) as NonNullable<PostgresExecutor<string>['transactionWithBudget']>;
    const { statements, store } = recorder({ transactionWithBudget });

    await store.transaction(async () => 'done', { timeoutMs: 400 });

    expect(transactionWithBudget).toHaveBeenCalledWith(expect.any(Function), 400);
    expect(statements).toEqual([]);
  });

  it('hands the level and the budget to transactionWithIsolation when the executor has it', async () => {
    const transactionWithIsolation = vi.fn((work: (transaction: string) => Promise<unknown>) =>
      work('isolated'),
    ) as NonNullable<PostgresExecutor<string>['transactionWithIsolation']>;
    const transaction = vi.fn();
    const { statements, store } = recorder({ transactionWithIsolation, transaction });

    await store.transaction(async (handle) => handle, {
      timeoutMs: 400,
      isolationLevel: 'serializable',
    });

    expect(transactionWithIsolation).toHaveBeenCalledWith(expect.any(Function), {
      isolationLevel: 'serializable',
      timeoutMs: 400,
    });
    expect(transaction).not.toHaveBeenCalled();
    expect(statements).toEqual([]);
  });
});

describe('withRootPool', () => {
  it('keeps transactionWithIsolation and isSerializationFailure of the executor', async () => {
    const orm: PostgresExecutor<string> = {
      transaction: (work) => work('plain'),
      transactionWithIsolation: vi.fn((work: (transaction: string) => Promise<unknown>) =>
        work('isolated'),
      ) as NonNullable<PostgresExecutor<string>['transactionWithIsolation']>,
      isSerializationFailure: vi.fn(() => true),
      query: async () => [],
    };
    const executor = withRootPool(orm, { transaction: vi.fn(), query: async () => [] });

    await expect(
      executor.transactionWithIsolation!(async (transaction) => transaction, {
        isolationLevel: 'serializable',
        timeoutMs: 50,
      }),
    ).resolves.toBe('isolated');
    expect(orm.transactionWithIsolation).toHaveBeenCalledWith(expect.any(Function), {
      isolationLevel: 'serializable',
      timeoutMs: 50,
    });
    expect(executor.isSerializationFailure!('anything')).toBe(true);
    expect(orm.isSerializationFailure).toHaveBeenCalledWith('anything');
  });

  it('adds neither when the executor has none', () => {
    const executor = withRootPool(
      { transaction: vi.fn(), query: async () => [] },
      { transaction: vi.fn(), query: async () => [] },
    );
    expect(executor.transactionWithIsolation).toBeUndefined();
    expect(executor.isSerializationFailure).toBeUndefined();
  });
});

describe('PgExecutor.transactionWithIsolation', () => {
  function createPool() {
    const client = Object.assign(new EventEmitter(), {
      query: vi.fn(async (sql: string) => ({ rows: [], command: sql })),
      release: vi.fn(),
    });
    const pool = { connect: vi.fn(async () => client), query: vi.fn() };
    return { client, executor: new PgExecutor(pool as unknown as Pool) };
  }

  it('sets the level with the BEGIN of the budget statement, in the same round trip', async () => {
    const { client, executor } = createPool();

    await expect(
      executor.transactionWithIsolation(async () => 'done', {
        isolationLevel: 'serializable',
        timeoutMs: 400,
      }),
    ).resolves.toBe('done');

    expect(client.query.mock.calls.map(([sql]) => sql)).toEqual([
      `BEGIN ISOLATION LEVEL SERIALIZABLE; ${BUDGET_SETTINGS}`,
      'COMMIT',
    ]);
    expect(client.release).toHaveBeenCalledWith();
  });

  it('writes each level and rolls back when the work fails', async () => {
    const { client, executor } = createPool();

    for (const level of TRANSACTION_ISOLATION_LEVELS) {
      await executor.transactionWithIsolation(async () => undefined, {
        isolationLevel: level,
        timeoutMs: 400,
      });
    }
    await expect(
      executor.transactionWithIsolation(() => Promise.reject(new Error('conflict')), {
        isolationLevel: 'repeatable read',
        timeoutMs: 400,
      }),
    ).rejects.toThrow('conflict');

    const begins = client.query.mock.calls
      .map(([sql]) => sql)
      .filter((sql) => sql.startsWith('BEGIN'))
      .map((sql) => sql.split(';')[0]);
    expect(begins).toEqual([
      'BEGIN ISOLATION LEVEL READ COMMITTED',
      'BEGIN ISOLATION LEVEL REPEATABLE READ',
      'BEGIN ISOLATION LEVEL SERIALIZABLE',
      'BEGIN ISOLATION LEVEL REPEATABLE READ',
    ]);
    expect(client.query).toHaveBeenLastCalledWith('ROLLBACK');
  });

  it('refuses an unknown level before it takes a connection', async () => {
    const { client, executor } = createPool();

    expect(() =>
      executor.transactionWithIsolation(async () => undefined, {
        isolationLevel: 'snapshot' as never,
        timeoutMs: 400,
      }),
    ).toThrow(ConfigurationError);
    expect(client.query).not.toHaveBeenCalled();
  });

  it('keeps the budget of a connection of its own as a session setting, level per transaction', async () => {
    const client = Object.assign(new EventEmitter(), {
      query: vi.fn(async (sql: string) => ({
        rows: [],
        command: sql === 'COMMIT' ? 'COMMIT' : '',
      })),
      release: vi.fn(),
    });
    const pool = { connect: vi.fn(async () => client), query: vi.fn() };
    const executor = new PgExecutor(pool as unknown as Pool, { sessionBudget: true });

    for (const level of ['serializable', 'repeatable read'] as const) {
      await executor.transactionWithIsolation(async () => undefined, {
        isolationLevel: level,
        timeoutMs: 400,
      });
    }

    const [first, , second] = client.query.mock.calls.map(([sql]) => sql);
    expect(first).toMatch(
      /^BEGIN ISOLATION LEVEL SERIALIZABLE; SELECT set_config\('statement_timeout', '400ms', false\)/,
    );
    // The connection keeps the budget now: only the BEGIN of this transaction's level is sent.
    expect(second).toBe('BEGIN ISOLATION LEVEL REPEATABLE READ');
  });
});
