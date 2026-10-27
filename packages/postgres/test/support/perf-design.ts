import type { Pool, PoolClient } from 'pg';
import { expect } from 'vitest';
import type { ClaimedDelivery, EventEnvelope, EventStoreTransactionOptions } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { expectWithinBudget } from '../../../../test-support/perf-budget.js';
import {
  PgExecutor,
  PostgresEventStore,
  type PostgresExecutor,
  type PostgresIsolatedTransactionOptions,
  type PostgresParameter,
  type PostgresQueryOptions,
  type PostgresRow,
} from '../../src/index.js';
import {
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './postgres-container.js';
import { claimWithLease } from './claim.js';

/**
 * Deterministic performance design tests: they count statements, tuples, WAL bytes and buffers
 * instead of timing anything, so a budget in perf-budgets.json fails the same way on a loaded
 * laptop and in CI. The server runs without autovacuum and full-page writes, so WAL and tuple
 * counters only reflect what the store itself does.
 */

/** Statements without parameters that the server parses in no time; they never count as parsed. */
const TRANSACTION_CONTROL = new Set(['BEGIN', 'COMMIT', 'ROLLBACK']);

export function startPerfDatabase(): Promise<TestDatabase> {
  return startDatabase('postgres:17-alpine', [
    'autovacuum=off',
    'full_page_writes=off',
    'fsync=off',
    'synchronous_commit=off',
  ]);
}

/**
 * Counts round trips (a transaction is BEGIN and COMMIT). While `explaining` is set, every statement is
 * first run under `EXPLAIN (ANALYZE, BUFFERS)` and rolled back (in a savepoint when it belongs to a
 * transaction), so the plan measured is the plan of the very state the real statement then runs on.
 */
export class CountingExecutor implements PostgresExecutor<PoolClient> {
  readonly #inner: PgExecutor;
  statements = 0;
  /** Messages the client sent and waited on: every `client.query` of a transaction or a statement. */
  roundTrips = 0;
  /** SQL statements in those messages (`BEGIN; SELECT ...` is two). The server also counts trigger queries such as a foreign key check. */
  sqlStatements = 0;
  /** Statements the server had to parse and plan from their text: sent unnamed, or named for the first time on the connection. */
  parsed = 0;
  explaining: PlanNode[] | undefined;
  /** Explain the generic plan a prepared statement switches to after five runs, instead of the plan for the parameters. */
  explainingGeneric = false;

  readonly #getDatabase: () => TestDatabase;

  constructor(
    pool: Pool,
    { getDatabase, sessionBudget }: { getDatabase: () => TestDatabase; sessionBudget: boolean },
  ) {
    this.#getDatabase = getDatabase;
    this.#inner = new PgExecutor(pool, { sessionBudget });
    pool.on('connect', (client) => {
      const prepared = new Set<string>();
      const query = client.query.bind(client) as (...arguments_: unknown[]) => unknown;
      (client as { query: unknown }).query = (...arguments_: unknown[]) => {
        const [first] = arguments_;
        const name = typeof first === 'object' ? (first as { name?: string }).name : undefined;
        const text = typeof first === 'string' ? first : (first as { text: string }).text;
        this.roundTrips += 1;
        this.sqlStatements += text.split(';').filter((part) => part.trim().length > 0).length;
        if (name === undefined ? !TRANSACTION_CONTROL.has(text) : !prepared.has(name)) {
          this.parsed += 1;
        }
        if (name !== undefined) {
          prepared.add(name);
        }
        return query(...arguments_);
      };
    });
  }

  transaction<T>(work: (transaction: PoolClient) => Promise<T>): Promise<T> {
    this.statements += 2;
    return this.#inner.transaction(work);
  }

  transactionWithBudget<T>(
    work: (transaction: PoolClient) => Promise<T>,
    timeoutMs: number,
  ): Promise<T> {
    this.statements += 2;
    return this.#inner.transactionWithBudget(work, timeoutMs);
  }

  transactionWithIsolation<T>(
    work: (transaction: PoolClient) => Promise<T>,
    options: PostgresIsolatedTransactionOptions,
  ): Promise<T> {
    this.statements += 2;
    return this.#inner.transactionWithIsolation(work, options);
  }

  async query(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: PoolClient,
    options?: PostgresQueryOptions,
  ): Promise<readonly PostgresRow[]> {
    this.statements += 1;
    if (this.explaining !== undefined) {
      this.explaining.push(
        await explainStatement(
          this.#getDatabase(),
          sql,
          parameters,
          transaction,
          this.explainingGeneric,
        ),
      );
    }
    return this.#inner.query(sql, parameters, transaction, options);
  }

  /** Statements the server parsed and planned from their text while `operation` ran. */
  async countParsed(operation: () => Promise<unknown>): Promise<number> {
    const before = this.parsed;
    await operation();
    return this.parsed - before;
  }

  /** Statements sent by `operation`. */
  async count(operation: () => Promise<unknown>): Promise<number> {
    const before = this.statements;
    await operation();
    return this.statements - before;
  }

  /** Executor statements, round trips on the wire and SQL statements in them of `operation`. */
  async countBoth(operation: () => Promise<unknown>): Promise<{
    readonly statements: number;
    readonly roundTrips: number;
    readonly sqlStatements: number;
  }> {
    const { statements, roundTrips, sqlStatements } = this;
    await operation();
    return {
      statements: this.statements - statements,
      roundTrips: this.roundTrips - roundTrips,
      sqlStatements: this.sqlStatements - sqlStatements,
    };
  }
}

async function explainStatement(
  database: TestDatabase,
  sql: string,
  parameters: readonly PostgresParameter[],
  transaction: PoolClient | undefined,
  generic = false,
): Promise<PlanNode> {
  const client = transaction ?? (await database.pool.connect());
  const [begin, end] =
    transaction === undefined
      ? ['BEGIN', 'ROLLBACK']
      : ['SAVEPOINT explained', 'ROLLBACK TO SAVEPOINT explained'];
  // EXECUTE takes its arguments as SQL expressions, not as parameters of the EXPLAIN around it.
  const literals = parameters
    .map((value) =>
      value === null
        ? 'NULL'
        : typeof value === 'number'
          ? String(value)
          : `'${value.replaceAll("'", "''")}'`,
    )
    .join(', ');
  try {
    await client.query(begin);
    try {
      if (generic) {
        await client.query('SET LOCAL plan_cache_mode = force_generic_plan');
        await client.query(`PREPARE relaybus_explained AS ${sql}`);
      }
      const { rows } = await client.query<{ 'QUERY PLAN': Array<{ Plan: PlanNode }> }>(
        `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${generic ? `EXECUTE relaybus_explained(${literals})` : sql}`,
        generic ? [] : [...parameters],
      );
      return rows[0]!['QUERY PLAN'][0]!.Plan;
    } finally {
      await client.query(end);
      if (generic) {
        // A prepared statement outlives the rollback.
        await client.query('DEALLOCATE relaybus_explained');
      }
    }
  } finally {
    if (transaction === undefined) {
      client.release();
    }
  }
}

export interface Fixture {
  readonly schema: string;
  readonly store: PostgresEventStore;
  readonly executor: CountingExecutor;
  readonly pool: Pool;
}

export async function publish(
  store: PostgresEventStore,
  events: readonly EventEnvelope[],
  handlers: readonly string[] = ['h'],
  now: Date = at(0),
): Promise<void> {
  await publishWith(store, { events, handlers, now, options: undefined });
}

/** {@link publish} with the transaction options of `bus.transaction` (its time budget). */
export async function publishWith(
  store: PostgresEventStore,
  {
    events,
    handlers = ['h'],
    now = at(0),
    options,
  }: {
    readonly events: readonly EventEnvelope[];
    readonly handlers?: readonly string[];
    readonly now?: Date;
    readonly options: EventStoreTransactionOptions | undefined;
  },
): Promise<void> {
  await store.transaction(
    (transaction) =>
      store.append(
        transaction,
        events.map((event) => ({
          event,
          deliveries: handlers.map((handler) => ({
            ...createDelivery(handler, now),
            id: nextId('delivery'),
          })),
        })),
        now,
      ),
    options,
  );
}

export function claim(
  store: PostgresEventStore,
  limit: number,
  handlers: readonly string[] = ['h'],
  owner = 'worker-1',
  now: Date = at(0),
): Promise<ClaimedDelivery[]> {
  return claimWithLease(store, { handlers, limit, owner, now });
}

let identifiers = 0;

/** Ids to skip: ids decide lanes and byte-tight WAL budgets, so a file that used to run after others starts where they ended. */
export function skipIdentifiers(count: number): void {
  identifiers = count;
}

/**
 * UUID-shaped ids in creation order: as long as the ids relaybus generates, but the same in every run
 * (random ids would change lanes and scan orders, and with them the counts).
 */
export function nextId(kind: 'event' | 'delivery'): string {
  identifiers += 1;
  return `00000000-0000-7000-${kind === 'event' ? '8000' : '9000'}-${String(identifiers).padStart(12, '0')}`;
}

export function events(count: number, key: (index: number) => string | null = () => null) {
  return Array.from({ length: count }, (_, index) =>
    createEnvelope({ id: nextId('event'), key: key(index) }),
  );
}

/** Publishes in batches of 500 events: one huge statement is not what the tests measure. */
export async function publishMany(
  store: PostgresEventStore,
  all: readonly EventEnvelope[],
  handlers: readonly string[] = ['h'],
): Promise<void> {
  for (let offset = 0; offset < all.length; offset += 500) {
    await publish(store, all.slice(offset, offset + 500), handlers);
  }
}
export interface TableCounters {
  readonly inserted: number;
  readonly updated: number;
  readonly hotUpdated: number;
  readonly deleted: number;
}

export const TABLES = [
  'deliveries',
  'lanes',
  'events',
  'event_completions',
  'key_sequences',
] as const;
export type Table = (typeof TABLES)[number];

export function difference(
  after: Record<Table, TableCounters>,
  before: Record<Table, TableCounters>,
): Record<Table, TableCounters> {
  return Object.fromEntries(
    TABLES.map((table) => [
      table,
      {
        inserted: after[table].inserted - before[table].inserted,
        updated: after[table].updated - before[table].updated,
        hotUpdated: after[table].hotUpdated - before[table].hotUpdated,
        deleted: after[table].deleted - before[table].deleted,
      },
    ]),
  ) as Record<Table, TableCounters>;
}
/** Claims `batch` at a time and completes each batch with one call, until nothing is due. */
export async function drain(
  store: PostgresEventStore,
  batch: number,
  handlers: readonly string[] = ['h'],
): Promise<number> {
  let total = 0;
  for (;;) {
    const claimed = await claim(store, batch, handlers);
    if (claimed.length === 0) {
      return total;
    }
    expect(await store.completeMany(claimed, at(1))).toEqual(claimed.map(() => true));
    total += claimed.length;
  }
}
export interface PlanNode {
  readonly 'Node Type': string;
  readonly 'Relation Name'?: string;
  readonly 'Shared Hit Blocks': number;
  readonly 'Shared Read Blocks': number;
  readonly 'Total Cost': number;
  readonly Plans?: readonly PlanNode[];
}

export function planNodes(node: PlanNode): PlanNode[] {
  return [node, ...(node.Plans ?? []).flatMap(planNodes)];
}

/**
 * Runs `operation` while every statement it sends is explained first (see {@link CountingExecutor}), and
 * returns the buffers they touched together and their plan nodes.
 */
export async function explain(
  fixture: Fixture,
  operation: () => Promise<unknown>,
  generic = false,
): Promise<{ readonly buffers: number; readonly nodes: PlanNode[]; readonly maximumCost: number }> {
  const plans: PlanNode[] = [];
  fixture.executor.explaining = plans;
  fixture.executor.explainingGeneric = generic;
  try {
    await operation();
  } finally {
    fixture.executor.explaining = undefined;
    fixture.executor.explainingGeneric = false;
  }
  return {
    buffers: plans.reduce(
      (sum, plan) => sum + plan['Shared Hit Blocks'] + plan['Shared Read Blocks'],
      0,
    ),
    nodes: plans.flatMap(planNodes),
    maximumCost: Math.max(0, ...plans.map((plan) => plan['Total Cost'])),
  };
}

/**
 * Checks the plans of an operation against its budgets: buffers touched, and sequential scans of
 * the tables that grow with the backlog (deliveries, events), whose budget is 0.
 */
export function expectPlanWithinBudget(
  name: string,
  plan: { readonly buffers: number; readonly nodes: readonly PlanNode[] },
): void {
  expectWithinBudget(`postgres.explain.${name}.buffers`, plan.buffers);
  expectWithinBudget(
    `postgres.explain.${name}.largeTableSequentialScans`,
    plan.nodes.filter(
      (node) =>
        node['Node Type'] === 'Seq Scan' && /deliveries|events/.test(node['Relation Name'] ?? ''),
    ).length,
  );
}
/** Helpers that need the database of the calling test file; `getDatabase` is read when a helper runs. */
export interface PerfHelpers {
  readonly pools: Pool[];
  closePools(): Promise<void>;
  /** `sessionBudget`: the executor of a pool the store created itself (`connectionString`). */
  createFixture(options?: { readonly sessionBudget?: boolean }): Promise<Fixture>;
  tupleCounters(fixture: Fixture): Promise<Record<Table, TableCounters>>;
  walPosition(): Promise<string>;
  walBytesSince(start: string): Promise<number>;
  analyze(fixture: Fixture): Promise<void>;
}

export function createPerfHelpers(getDatabase: () => TestDatabase): PerfHelpers {
  const pools: Pool[] = [];

  async function closePools(): Promise<void> {
    await Promise.all(pools.map((pool) => pool.end()));
  }

  /** A store on a fresh schema whose statements all run on one connection, so its statistics flush together. */
  async function createFixture({ sessionBudget = false } = {}): Promise<Fixture> {
    const schema = uniqueSchema();
    const pool = createTestPool(getDatabase().connectionString, 1);
    pools.push(pool);
    const executor = new CountingExecutor(pool, { getDatabase, sessionBudget });
    const store = new PostgresEventStore({ executor, schema });
    await store.setup();
    return { schema, store, executor, pool };
  }
  async function tupleCounters(fixture: Fixture): Promise<Record<Table, TableCounters>> {
    await fixture.pool.query('SELECT pg_stat_force_next_flush()');
    // The flush happens when that backend goes idle; a second statement on it is certainly after it.
    await fixture.pool.query('SELECT 1');
    const client = await getDatabase().pool.connect();
    try {
      await client.query('SELECT pg_stat_clear_snapshot()');
      const { rows } = await client.query<{
        relname: string;
        inserted: string;
        updated: string;
        hot: string;
        deleted: string;
      }>(
        `SELECT relname, n_tup_ins::text AS inserted, n_tup_upd::text AS updated,
                n_tup_hot_upd::text AS hot, n_tup_del::text AS deleted
         FROM pg_stat_user_tables WHERE schemaname = $1`,
        [fixture.schema],
      );
      const byTable = Object.fromEntries(rows.map((row) => [row.relname, row]));
      return Object.fromEntries(
        TABLES.map((table) => {
          const row = byTable[`relaybus_${table}`]!;
          return [
            table,
            {
              inserted: Number(row.inserted),
              updated: Number(row.updated),
              hotUpdated: Number(row.hot),
              deleted: Number(row.deleted),
            },
          ];
        }),
      ) as Record<Table, TableCounters>;
    } finally {
      client.release();
    }
  }
  async function walPosition(): Promise<string> {
    const { rows } = await getDatabase().pool.query<{ lsn: string }>(
      'SELECT pg_current_wal_insert_lsn()::text AS lsn',
    );
    return rows[0]!.lsn;
  }

  async function walBytesSince(start: string): Promise<number> {
    const { rows } = await getDatabase().pool.query<{ bytes: string }>(
      'SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::text AS bytes',
      [start],
    );
    return Number(rows[0]!.bytes);
  }
  async function analyze(fixture: Fixture): Promise<void> {
    for (const table of TABLES) {
      await getDatabase().pool.query(`ANALYZE "${fixture.schema}".relaybus_${table}`);
    }
  }

  return { pools, closePools, createFixture, tupleCounters, walPosition, walBytesSince, analyze };
}
