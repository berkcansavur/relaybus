import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import type { Pool, PoolClient } from 'pg';
import {
  PgExecutor,
  type PostgresExecutor,
  PostgresEventStore,
  createPostgresSchemaSql,
  createPostgresSchemaStatements,
  resolvePostgresNames,
} from '../src/index.js';
import { splitPositionalSql } from '../src/positional-sql.js';

describe('identifier validation', () => {
  it('uses the defaults', () => {
    const names = resolvePostgresNames();
    expect(names.events).toBe('"public"."relaybus_events"');
    expect(names.deliveries).toBe('"public"."relaybus_deliveries"');
    expect(names.keySequences).toBe('"public"."relaybus_key_sequences"');
  });

  it('applies a custom schema and prefix', () => {
    const names = resolvePostgresNames({ schema: 'messaging', tablePrefix: 'bus_' });
    expect(names.events).toBe('"messaging"."bus_events"');
    expect(names.indexes.deliveryLaneDue).toBe('"bus_deliveries_lane_due_idx"');
  });

  it('accepts an empty prefix', () => {
    expect(resolvePostgresNames({ tablePrefix: '' }).events).toBe('"public"."events"');
  });

  it.each([
    ['schema', { schema: 'public"; DROP TABLE x; --' }],
    ['schema', { schema: 'Public' }],
    ['schema', { schema: '' }],
    ['schema', { schema: 'a'.repeat(64) }],
    ['schema', { schema: '1abc' }],
    ['tablePrefix', { tablePrefix: 'x"; DROP TABLE y; --' }],
    ['tablePrefix', { tablePrefix: 'UPPER_' }],
    ['tablePrefix', { tablePrefix: 'with space' }],
    ['tablePrefix', { tablePrefix: '1digit_' }],
    ['tablePrefix', { tablePrefix: 'p'.repeat(40) }],
  ])('rejects an unsafe %s', (_label, options) => {
    expect(() => resolvePostgresNames(options)).toThrow(ConfigurationError);
    expect(() => createPostgresSchemaStatements(options)).toThrow(ConfigurationError);
    expect(() => new PostgresEventStore({ executor: fakeExecutor(), ...options })).toThrow(
      ConfigurationError,
    );
  });
});

describe('schema SQL', () => {
  it('creates everything idempotently', () => {
    const statements = createPostgresSchemaStatements();
    expect(statements.length).toBeGreaterThan(8);
    for (const statement of statements) {
      // Guarded creation, a conditional upgrade block, a drop of an obsolete index, a storage
      // parameter, an insert that skips existing lanes or the schema marker: running any of them again changes nothing.
      expect(statement).toMatch(
        /IF NOT EXISTS|IF EXISTS|ON CONFLICT \(lane\) DO NOTHING|ALTER TABLE .* SET \(autovacuum|COMMENT ON TABLE/,
      );
    }
    expect(statements[0]).not.toMatch(/CREATE SCHEMA/);
  });

  it('ends with a marker that changes with the statements before it', () => {
    const plain = createPostgresSchemaStatements();
    const withHistory = createPostgresSchemaStatements({ completionHistory: true });
    expect(plain.at(-1)).toMatch(
      /^COMMENT ON TABLE "public"\."relaybus_lanes" IS 'relaybus schema [0-9a-f]{32}'$/,
    );
    expect(withHistory.at(-1)).not.toBe(plain.at(-1));
  });

  it('creates a custom schema first', () => {
    expect(createPostgresSchemaStatements({ schema: 'messaging' })[0]).toBe(
      'CREATE SCHEMA IF NOT EXISTS "messaging"',
    );
  });

  it('renders one script for migration tools', () => {
    const sql = createPostgresSchemaSql({ tablePrefix: 'bus_' });
    expect(sql).toContain('"public"."bus_deliveries"');
    expect(sql.trimEnd().endsWith(';')).toBe(true);
  });
});

describe('positional SQL', () => {
  it('splits text and parameter references in order, keeping repeats', () => {
    expect(splitPositionalSql('a = $1 AND b = $2 OR c = $1')).toEqual([
      'a = ',
      { parameterIndex: 0 },
      ' AND b = ',
      { parameterIndex: 1 },
      ' OR c = ',
      { parameterIndex: 0 },
      '',
    ]);
    expect(splitPositionalSql('SELECT 1')).toEqual(['SELECT 1']);
  });
});

function fakeExecutor(): PostgresExecutor<unknown> {
  return {
    transaction: (work) => work(undefined),
    query: () => Promise.resolve([]),
  };
}

describe('PostgresEventStore options', () => {
  it('requires exactly one way to connect', () => {
    expect(() => new PostgresEventStore({} as never)).toThrow(ConfigurationError);
    expect(
      () => new PostgresEventStore({ pool: {} as Pool, executor: fakeExecutor() } as never),
    ).toThrow(ConfigurationError);
  });

  it.each([0, -1, 1.5, Number.NaN])('rejects purgeBatchSize %s', (purgeBatchSize) => {
    expect(() => new PostgresEventStore({ executor: fakeExecutor(), purgeBatchSize })).toThrow(
      ConfigurationError,
    );
  });

  it('rejects a preparedStatements that is not a boolean', () => {
    expect(
      () => new PostgresEventStore({ executor: fakeExecutor(), preparedStatements: 1 as never }),
    ).toThrow('preparedStatements must be a boolean, received 1');
  });

  it('asks the executor to prepare its statements unless preparedStatements is false', async () => {
    const prepared = vi.fn(async () => [{ count: 0 }]);
    await new PostgresEventStore<undefined>({
      executor: { transaction: (work) => work(undefined), query: prepared },
    }).relinquish('w');
    expect(prepared).toHaveBeenCalledWith(expect.any(String), ['w'], undefined, {
      prepare: true,
    });
    const unprepared = vi.fn(async () => [{ count: 0 }]);
    await new PostgresEventStore<undefined>({
      executor: { transaction: (work) => work(undefined), query: unprepared },
      preparedStatements: false,
    }).relinquish('w');
    expect(unprepared).toHaveBeenCalledWith(expect.any(String), ['w'], undefined, undefined);
  });

  it('claims nothing for an empty handler list without touching the database', async () => {
    const query = vi.fn();
    const store = new PostgresEventStore({
      executor: { transaction: vi.fn(), query } as never,
    });
    await expect(
      store.claim({
        handlers: [],
        limit: 5,
        leaseOwner: 'w',
        leaseExpiresAt: new Date(),
        now: new Date(),
      }),
    ).resolves.toEqual([]);
    for (const limit of [0, -3, Number.NaN]) {
      await expect(
        store.claim({
          handlers: ['h'],
          limit,
          leaseOwner: 'w',
          leaseExpiresAt: new Date(),
          now: new Date(),
        }),
      ).resolves.toEqual([]);
    }
    await expect(store.append({} as never, [], new Date())).resolves.toEqual({
      appendedEventIds: [],
      duplicateEventIds: [],
      appendedDeliveryCount: 0,
    });
    expect(query).not.toHaveBeenCalled();
  });
});

describe('PgExecutor', () => {
  function createPool(clientQuery: (sql: string) => Promise<unknown>) {
    const client = { query: vi.fn(clientQuery), release: vi.fn() };
    const pool = { connect: vi.fn(async () => client), query: vi.fn() };
    return { client, pool, executor: new PgExecutor(pool as unknown as Pool) };
  }

  it('commits and releases the client', async () => {
    const { client, executor } = createPool(async () => ({ rows: [] }));
    await expect(executor.transaction(async () => 'done')).resolves.toBe('done');
    expect(client.query.mock.calls.map(([sql]) => sql)).toEqual(['BEGIN', 'COMMIT']);
    expect(client.release).toHaveBeenCalledWith();
  });

  it('rolls back and reuses the client when the work fails', async () => {
    const { client, executor } = createPool(async () => ({ rows: [] }));
    await expect(
      executor.transaction(async (transaction: PoolClient) => {
        expect(transaction).toBe(client);
        throw new Error('work failed');
      }),
    ).rejects.toThrow('work failed');
    expect(client.query.mock.calls.map(([sql]) => sql)).toEqual(['BEGIN', 'ROLLBACK']);
    expect(client.release).toHaveBeenCalledWith();
  });

  it('destroys a client whose rollback fails and reports the original error', async () => {
    const { client, executor } = createPool(async (sql) => {
      if (sql === 'ROLLBACK') {
        throw new Error('connection lost');
      }
      return { rows: [] };
    });
    await expect(
      executor.transaction(async () => {
        throw new Error('work failed');
      }),
    ).rejects.toThrow('work failed');
    expect(client.release).toHaveBeenCalledWith(true);
  });

  it('sends a statement to prepare as a named statement whose name follows from its text', async () => {
    const { pool, executor } = createPool(async () => ({ rows: [] }));
    pool.query.mockResolvedValue({ rows: [] });
    await executor.query('SELECT $1', ['x'], undefined, { prepare: true });
    await executor.query('SELECT $1', ['y'], undefined, { prepare: true });
    await executor.query('SELECT $1::int', [1], undefined, { prepare: true });
    await executor.query('SELECT $1', ['z'], undefined, { prepare: false });
    const [first, second, third, fourth] = pool.query.mock.calls as unknown[][];
    expect(first![0]).toEqual({
      name: expect.stringMatching(/^relaybus_[0-9a-f]{24}$/),
      text: 'SELECT $1',
      values: ['x'],
    });
    expect((second![0] as { name: string }).name).toBe((first![0] as { name: string }).name);
    expect((third![0] as { name: string }).name).not.toBe((first![0] as { name: string }).name);
    expect(fourth).toEqual(['SELECT $1', ['z']]);
  });

  it.each(['26000', '42P05'])(
    'stops preparing for good when the server answers %s, running the statement again unprepared',
    async (code) => {
      const { pool, executor } = createPool(async () => ({ rows: [] }));
      const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
      const logged = new PgExecutor(pool as unknown as Pool, { logger });
      pool.query.mockImplementation(async (config: unknown) => {
        if (typeof config === 'object') {
          throw Object.assign(new Error('prepared statement does not exist'), { code });
        }
        return { rows: [{ ok: true }] };
      });
      await expect(logged.query('SELECT 1', [], undefined, { prepare: true })).resolves.toEqual([
        { ok: true },
      ]);
      await expect(logged.query('SELECT 2', [], undefined, { prepare: true })).resolves.toEqual([
        { ok: true },
      ]);
      expect(pool.query.mock.calls.map(([config]) => typeof config)).toEqual([
        'object',
        'string',
        'string',
      ]);
      expect(logger.warn).toHaveBeenCalledTimes(1);
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining(
          `[PgExecutor](query) The server does not keep prepared statements per connection`,
        ),
      );
      expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining(`code=${code}`));
      void executor;
    },
  );

  it('fails a statement of a transaction whose prepared statement the server lost, preparing nothing after', async () => {
    const lost = Object.assign(new Error('prepared statement does not exist'), { code: '26000' });
    const { client, executor } = createPool(async () => {
      throw lost;
    });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    try {
      await expect(
        executor.query('SELECT 1', [], client as unknown as PoolClient, { prepare: true }),
      ).rejects.toBe(lost);
      expect(warn).toHaveBeenCalledTimes(1);
      client.query.mockResolvedValue({ rows: [] });
      await executor.query('SELECT 1', [], client as unknown as PoolClient, { prepare: true });
      expect(client.query).toHaveBeenLastCalledWith('SELECT 1', []);
    } finally {
      warn.mockRestore();
    }
  });

  it('warns once when statements sent before the switch fail the same way', async () => {
    const { pool } = createPool(async () => ({ rows: [] }));
    const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const executor = new PgExecutor(pool as unknown as Pool, { logger });
    pool.query.mockImplementation(async (config: unknown) => {
      if (typeof config === 'object') {
        throw Object.assign(new Error('prepared statement does not exist'), { code: '26000' });
      }
      return { rows: [] };
    });
    await Promise.all([
      executor.query('SELECT 1', [], undefined, { prepare: true }),
      executor.query('SELECT 2', [], undefined, { prepare: true }),
    ]);
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  it('rethrows any other error of a prepared statement and keeps preparing', async () => {
    const { pool, executor } = createPool(async () => ({ rows: [] }));
    const broken = Object.assign(new Error('syntax error'), { code: '42601' });
    pool.query.mockRejectedValueOnce(broken).mockResolvedValue({ rows: [] });
    await expect(executor.query('SELEC 1', [], undefined, { prepare: true })).rejects.toBe(broken);
    await executor.query('SELECT 1', [], undefined, { prepare: true });
    expect(typeof (pool.query.mock.calls[1] as unknown[])[0]).toBe('object');
  });

  it('runs queries on the pool, or on the transaction when given one', async () => {
    const { pool, client, executor } = createPool(async () => ({ rows: [{ a: 1 }] }));
    pool.query.mockResolvedValue({ rows: [{ b: 2 }] });
    await expect(executor.query('SELECT $1', ['x'])).resolves.toEqual([{ b: 2 }]);
    await expect(
      executor.query('SELECT $1', ['x'], client as unknown as PoolClient),
    ).resolves.toEqual([{ a: 1 }]);
  });
});

describe('wake-up notifications', () => {
  function appendingStore(notify: () => Promise<unknown>) {
    const sent: unknown[][] = [];
    const logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() };
    const store = new PostgresEventStore<undefined>({
      executor: {
        transaction: (work) => work(undefined),
        query: async (sql, parameters) => {
          if (sql.startsWith('SELECT pg_notify')) {
            sent.push(['notify']);
            await notify();
            return [];
          }
          sent.push([...parameters]);
          return [{ id: 'e', inserted: true, sequence: null, deliveries: 1 }];
        },
      },
      logger: logger as never,
    });
    const entries = (availableAt: Date) => [
      {
        event: {
          id: 'e',
          type: 't',
          source: 's',
          key: null,
          payload: {},
          headers: {},
          correlationId: null,
          causationId: null,
          occurredAt: availableAt,
        },
        deliveries: [{ id: 'd', handler: 'h', availableAt }],
      },
    ];
    // Publishes in a transaction of the store, which knows when it committed.
    const append = (availableAt: Date) =>
      store.transaction((transaction) =>
        store.append(transaction, entries(availableAt), new Date(0)),
      );
    // Publishes in a transaction of the caller, whose commit the store does not see.
    const appendInCallerTransaction = (availableAt: Date) =>
      store.append(undefined, entries(availableAt), new Date(0));
    return { store, sent, logger, append, appendInCallerTransaction };
  }

  it('notifies the first due publish at once and the rest of the interval once, after it', async () => {
    vi.useFakeTimers();
    try {
      const { sent, append, appendInCallerTransaction } = appendingStore(async () => undefined);
      await append(new Date(0));
      await append(new Date(0));
      await append(new Date(0));
      expect(sent.map((parameters) => parameters[4])).toEqual([
        '1970-01-01T00:00:00.000Z',
        null,
        null,
      ]);
      await vi.advanceTimersByTimeAsync(10);
      expect(sent.at(-1)).toEqual(['notify']);
      // A publish of deliveries due later wakes nobody.
      await vi.advanceTimersByTimeAsync(100);
      await append(new Date(60_000));
      expect(sent.at(-1)![4]).toBeNull();
      // A caller's own transaction always carries its notification, whatever was sent just before.
      await append(new Date(0));
      await appendInCallerTransaction(new Date(0));
      expect(sent.slice(-2).map((parameters) => parameters[4])).toEqual([
        '1970-01-01T00:00:00.000Z',
        '1970-01-01T00:00:00.000Z',
      ]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('logs a failed trailing notification and sends none after close', async () => {
    vi.useFakeTimers();
    try {
      const failing = appendingStore(async () => {
        throw new Error('gone');
      });
      await failing.append(new Date(0));
      await failing.append(new Date(0));
      await vi.advanceTimersByTimeAsync(10);
      expect(failing.logger.error).toHaveBeenCalledWith(
        expect.stringContaining(
          '[PostgresEventStore](wakeAfterCommit) Waking the dispatchers failed',
        ),
      );
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
      const failingText = appendingStore(() => Promise.reject('text'));
      await failingText.append(new Date(0));
      await failingText.append(new Date(0));
      await vi.advanceTimersByTimeAsync(10);
      expect(failingText.logger.error).toHaveBeenCalledWith(expect.stringContaining('error=text'));

      const closing = appendingStore(async () => undefined);
      await closing.append(new Date(0));
      await closing.append(new Date(0));
      await closing.store.close();
      await vi.advanceTimersByTimeAsync(50);
      expect(closing.sent).toHaveLength(2);
    } finally {
      vi.useRealTimers();
    }
  });
});
