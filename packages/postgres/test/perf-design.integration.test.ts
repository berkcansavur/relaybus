import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Pool, PoolClient } from 'pg';
import { RelayBus, defineEvent, type ClaimedDelivery, type EventEnvelope } from '@relaybus/core';
import { at, createDelivery, createEnvelope } from '@relaybus/testing';
import { expectWithinBudget } from '../../../test-support/perf-budget.js';
import {
  PgExecutor,
  PostgresEventStore,
  type PostgresExecutor,
  type PostgresParameter,
  type PostgresRow,
} from '../src/index.js';
import {
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

/**
 * Deterministic performance design tests: they count statements, tuples, WAL bytes and buffers
 * instead of timing anything, so a budget in perf-budgets.json fails the same way on a loaded
 * laptop and in CI. The server runs without autovacuum and full-page writes, so WAL and tuple
 * counters only reflect what the store itself does.
 */

const LEASE_MS = 30_000;

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase('postgres:17-alpine', [
    'autovacuum=off',
    'full_page_writes=off',
    'fsync=off',
    'synchronous_commit=off',
  ]);
});

afterAll(async () => {
  await database.stop();
});

/**
 * Counts round trips (a transaction is BEGIN and COMMIT). While `explaining` is set, every statement is
 * first run under `EXPLAIN (ANALYZE, BUFFERS)` and rolled back (in a savepoint when it belongs to a
 * transaction), so the plan measured is the plan of the very state the real statement then runs on.
 */
class CountingExecutor implements PostgresExecutor<PoolClient> {
  readonly #inner: PgExecutor;
  statements = 0;
  explaining: PlanNode[] | undefined;

  constructor(pool: Pool) {
    this.#inner = new PgExecutor(pool);
  }

  transaction<T>(work: (transaction: PoolClient) => Promise<T>): Promise<T> {
    this.statements += 2;
    return this.#inner.transaction(work);
  }

  async query(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: PoolClient,
  ): Promise<readonly PostgresRow[]> {
    this.statements += 1;
    if (this.explaining !== undefined) {
      this.explaining.push(await explainStatement(sql, parameters, transaction));
    }
    return this.#inner.query(sql, parameters, transaction);
  }

  /** Statements sent by `operation`. */
  async count(operation: () => Promise<unknown>): Promise<number> {
    const before = this.statements;
    await operation();
    return this.statements - before;
  }
}

async function explainStatement(
  sql: string,
  parameters: readonly PostgresParameter[],
  transaction: PoolClient | undefined,
): Promise<PlanNode> {
  const client = transaction ?? (await database.pool.connect());
  const [begin, end] =
    transaction === undefined
      ? ['BEGIN', 'ROLLBACK']
      : ['SAVEPOINT explained', 'ROLLBACK TO SAVEPOINT explained'];
  try {
    await client.query(begin);
    try {
      const { rows } = await client.query<{ 'QUERY PLAN': Array<{ Plan: PlanNode }> }>(
        `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`,
        [...parameters],
      );
      return rows[0]!['QUERY PLAN'][0]!.Plan;
    } finally {
      await client.query(end);
    }
  } finally {
    if (transaction === undefined) {
      client.release();
    }
  }
}

interface Fixture {
  readonly schema: string;
  readonly store: PostgresEventStore;
  readonly executor: CountingExecutor;
  readonly pool: Pool;
}

const pools: Pool[] = [];

afterAll(async () => {
  await Promise.all(pools.map((pool) => pool.end()));
});

/** A store on a fresh schema whose statements all run on one connection, so its statistics flush together. */
async function createFixture(): Promise<Fixture> {
  const schema = uniqueSchema();
  const pool = createTestPool(database.connectionString, 1);
  pools.push(pool);
  const executor = new CountingExecutor(pool);
  const store = new PostgresEventStore({ executor, schema });
  await store.setup();
  return { schema, store, executor, pool };
}

async function publish(
  store: PostgresEventStore,
  events: readonly EventEnvelope[],
  handlers: readonly string[] = ['h'],
  now: Date = at(0),
): Promise<void> {
  await store.transaction((transaction) =>
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
  );
}

function claim(
  store: PostgresEventStore,
  limit: number,
  handlers: readonly string[] = ['h'],
  owner = 'worker-1',
  now: Date = at(0),
): Promise<ClaimedDelivery[]> {
  return store.claim({
    handlers,
    limit,
    leaseOwner: owner,
    leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
    now,
  });
}

let identifiers = 0;

/**
 * UUID-shaped ids in creation order: as long as the ids relaybus generates, but the same in every run
 * (random ids would change lanes and scan orders, and with them the counts).
 */
function nextId(kind: 'event' | 'delivery'): string {
  identifiers += 1;
  return `00000000-0000-7000-${kind === 'event' ? '8000' : '9000'}-${String(identifiers).padStart(12, '0')}`;
}

function events(count: number, key: (index: number) => string | null = () => null) {
  return Array.from({ length: count }, (_, index) =>
    createEnvelope({ id: nextId('event'), key: key(index) }),
  );
}

/** Publishes in batches of 500 events: one huge statement is not what the tests measure. */
async function publishMany(
  store: PostgresEventStore,
  all: readonly EventEnvelope[],
  handlers: readonly string[] = ['h'],
): Promise<void> {
  for (let offset = 0; offset < all.length; offset += 500) {
    await publish(store, all.slice(offset, offset + 500), handlers);
  }
}

interface TableCounters {
  readonly inserted: number;
  readonly updated: number;
  readonly hotUpdated: number;
  readonly deleted: number;
}

const TABLES = ['deliveries', 'lanes', 'events', 'event_completions', 'key_sequences'] as const;
type Table = (typeof TABLES)[number];

/** Flushes the statistics of the fixture's connection and reads the tuple counters of every relaybus table. */
async function tupleCounters(fixture: Fixture): Promise<Record<Table, TableCounters>> {
  await fixture.pool.query('SELECT pg_stat_force_next_flush()');
  // The flush happens when that backend goes idle; a second statement on it is certainly after it.
  await fixture.pool.query('SELECT 1');
  const client = await database.pool.connect();
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

function difference(
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

async function walPosition(): Promise<string> {
  const { rows } = await database.pool.query<{ lsn: string }>(
    'SELECT pg_current_wal_insert_lsn()::text AS lsn',
  );
  return rows[0]!.lsn;
}

async function walBytesSince(start: string): Promise<number> {
  const { rows } = await database.pool.query<{ bytes: string }>(
    'SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::text AS bytes',
    [start],
  );
  return Number(rows[0]!.bytes);
}

/** Claims `batch` at a time and completes each batch with one call, until nothing is due. */
async function drain(
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

describe('round trips per claim', () => {
  it('claims nothing in one statement on an idle store, without writing a lane', async () => {
    const fixture = await createFixture();
    const before = await tupleCounters(fixture);
    expectWithinBudget(
      'postgres.claim.idleCold.statements',
      await fixture.executor.count(() => claim(fixture.store, 10)),
    );
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget('postgres.claim.idle.laneWrites', written.lanes.updated);
  });

  it('claims nothing in one statement while the worker still holds an idle lane', async () => {
    const fixture = await createFixture();
    await publish(fixture.store, events(1));
    const claimed = await claim(fixture.store, 10);
    await fixture.store.completeMany(claimed, at(1));
    expectWithinBudget(
      'postgres.claim.idleWarm.statements',
      await fixture.executor.count(() => claim(fixture.store, 10)),
    );
  });

  it('claims a backlog in one statement, cold and warm', async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(1_000));
    let first: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.backlogCold.statements',
      await fixture.executor.count(async () => {
        first = await claim(fixture.store, 50);
      }),
    );
    expect(first).toHaveLength(50);
    let second: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.backlogWarm.statements',
      await fixture.executor.count(async () => {
        second = await claim(fixture.store, 50);
      }),
    );
    expect(second).toHaveLength(50);
  });

  it('claims the next deliveries of few hot keys in one statement', async () => {
    const fixture = await createFixture();
    const keys = 10;
    await publishMany(
      fixture.store,
      events(keys * 100, (index) => `hot-${index % keys}`),
    );
    const heads = await claim(fixture.store, keys);
    expect(heads).toHaveLength(keys);
    await fixture.store.completeMany(heads, at(1));
    let successors: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.hotKeysWarm.statements',
      await fixture.executor.count(async () => {
        successors = await claim(fixture.store, keys);
      }),
    );
    expect(successors).toHaveLength(keys);
    // Every key is in flight now: nothing is claimable, which must stay cheap too.
    expectWithinBudget(
      'postgres.claim.hotKeysAllInFlight.statements',
      await fixture.executor.count(async () => {
        expect(await claim(fixture.store, keys)).toHaveLength(0);
      }),
    );
  });

  it('claims many distinct keys in one statement', async () => {
    const fixture = await createFixture();
    await publishMany(
      fixture.store,
      events(1_000, (index) => `key-${index}`),
    );
    let claimed: ClaimedDelivery[] = [];
    expectWithinBudget(
      'postgres.claim.distinctKeysCold.statements',
      await fixture.executor.count(async () => {
        claimed = await claim(fixture.store, 200);
      }),
    );
    expect(claimed).toHaveLength(200);
  });
});

describe('round trips per publish and completion', () => {
  it('publishes fresh events with one statement per call, whatever the batch size', async () => {
    const fixture = await createFixture();
    // A publish within 10 ms of the one before leaves the wake-up to one trailing notification per
    // interval, a round trip of its own; waiting the interval out keeps it out of the next count.
    const count = async (operation: () => Promise<unknown>): Promise<number> => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return fixture.executor.count(operation);
    };
    expectWithinBudget(
      'postgres.publish.unkeyedSingle.statements',
      await count(() => publish(fixture.store, events(1))),
    );
    expectWithinBudget(
      'postgres.publish.keyedSingle.statements',
      await count(() =>
        publish(
          fixture.store,
          events(1, () => 'k'),
        ),
      ),
    );
    expectWithinBudget(
      'postgres.publish.keyedBatch100.statements',
      await count(() =>
        publish(
          fixture.store,
          events(100, (index) => `k${index % 7}`),
        ),
      ),
    );
    const [event] = events(1);
    await publish(fixture.store, [event!], ['h']);
    expectWithinBudget(
      'postgres.publish.repeatedEventNewHandler.statements',
      await count(() => publish(fixture.store, [event!], ['h', 'other'])),
    );
  });

  it('completes one lease or fifty with one statement', async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(51));
    const claimed = await claim(fixture.store, 51);
    expectWithinBudget(
      'postgres.complete.single.statements',
      await fixture.executor.count(() => fixture.store.completeMany(claimed.slice(0, 1), at(1))),
    );
    expectWithinBudget(
      'postgres.complete.batch50.statements',
      await fixture.executor.count(() => fixture.store.completeMany(claimed.slice(1), at(1))),
    );
  });
});

describe('tuples written per delivery', () => {
  it('inserts and deletes each delivery once and updates lanes only in place', async () => {
    const fixture = await createFixture();
    const total = 500;
    await publishMany(fixture.store, events(total));
    const before = await tupleCounters(fixture);
    expect(await drain(fixture.store, 50)).toBe(total);
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget(
      'postgres.tuples.deliveries.updatesPerDelivery',
      written.deliveries.updated / total,
    );
    expectWithinBudget(
      'postgres.tuples.deliveries.deletesPerDelivery',
      written.deliveries.deleted / total,
    );
    expectWithinBudget('postgres.tuples.events.updatesPerEvent', written.events.updated / total);
    expectWithinBudget(
      'postgres.tuples.lanes.nonHotUpdatesPercent',
      ((written.lanes.updated - written.lanes.hotUpdated) / written.lanes.updated) * 100,
    );
    expectWithinBudget('postgres.tuples.lanes.updatesPerDelivery', written.lanes.updated / total);
    expectWithinBudget(
      'postgres.tuples.eventCompletions.updatesPerEvent.oneHandler',
      written.event_completions.updated / total,
    );
  });

  it('stamps an event with five handlers without rewriting its completion row per handler', async () => {
    const fixture = await createFixture();
    const total = 200;
    const handlers = ['a', 'b', 'c', 'd', 'e'];
    await publishMany(fixture.store, events(total), handlers);
    const before = await tupleCounters(fixture);
    // Completions arrive in small groups and a little later each, as a dispatcher with a short
    // completion window writes them.
    let completedAt = 1;
    for (;;) {
      const claimed = await claim(fixture.store, 50, handlers);
      if (claimed.length === 0) {
        break;
      }
      for (let offset = 0; offset < claimed.length; offset += 3) {
        completedAt += 1;
        await fixture.store.completeMany(claimed.slice(offset, offset + 3), at(completedAt));
      }
    }
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget(
      'postgres.tuples.eventCompletions.updatesPerEvent.fiveHandlers',
      written.event_completions.updated / total,
    );
  });

  it('updates the counters of keys in place', async () => {
    const fixture = await createFixture();
    const before = await tupleCounters(fixture);
    for (let index = 0; index < 200; index += 1) {
      await publish(
        fixture.store,
        events(1, () => `k${index % 20}`),
      );
    }
    const written = difference(await tupleCounters(fixture), before);
    expectWithinBudget(
      'postgres.tuples.keySequences.nonHotUpdatesPercent',
      ((written.key_sequences.updated - written.key_sequences.hotUpdated) /
        written.key_sequences.updated) *
        100,
    );
  });
});

describe('WAL bytes', () => {
  it('writes little WAL per delivery when dispatching', async () => {
    for (const batch of [10, 200]) {
      const fixture = await createFixture();
      const total = 1_000;
      await publishMany(fixture.store, events(total));
      const start = await walPosition();
      expect(await drain(fixture.store, batch)).toBe(total);
      expectWithinBudget(
        `postgres.wal.dispatchBatch${batch}.bytesPerDelivery`,
        Math.round((await walBytesSince(start)) / total),
      );
    }
  });

  it('writes little WAL per published event', async () => {
    const fixture = await createFixture();
    const total = 1_000;
    const all = events(total);
    const start = await walPosition();
    for (let offset = 0; offset < total; offset += 10) {
      await publish(fixture.store, all.slice(offset, offset + 10));
    }
    expectWithinBudget(
      'postgres.wal.publish.bytesPerEvent',
      Math.round((await walBytesSince(start)) / total),
    );
  });
});

describe('schema', () => {
  it('keeps the number of indexes every delivery pays for small', async () => {
    const fixture = await createFixture();
    const { rows } = await database.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM pg_index
       WHERE indrelid = (quote_ident($1) || '.relaybus_deliveries')::regclass`,
      [fixture.schema],
    );
    expectWithinBudget('postgres.schema.deliveryIndexes', rows[0]!.count);
  });
});

interface PlanNode {
  readonly 'Node Type': string;
  readonly 'Relation Name'?: string;
  readonly 'Shared Hit Blocks': number;
  readonly 'Shared Read Blocks': number;
  readonly 'Total Cost': number;
  readonly Plans?: readonly PlanNode[];
}

function planNodes(node: PlanNode): PlanNode[] {
  return [node, ...(node.Plans ?? []).flatMap(planNodes)];
}

/**
 * Runs `operation` while every statement it sends is explained first (see {@link CountingExecutor}), and
 * returns the buffers they touched together and their plan nodes.
 */
async function explain(
  fixture: Fixture,
  operation: () => Promise<unknown>,
): Promise<{ readonly buffers: number; readonly nodes: PlanNode[]; readonly maximumCost: number }> {
  const plans: PlanNode[] = [];
  fixture.executor.explaining = plans;
  try {
    await operation();
  } finally {
    fixture.executor.explaining = undefined;
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
function expectPlanWithinBudget(
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

/** What autovacuum would have done by now: the planner needs statistics to choose like in production. */
async function analyze(fixture: Fixture): Promise<void> {
  for (const table of TABLES) {
    await database.pool.query(`ANALYZE "${fixture.schema}".relaybus_${table}`);
  }
}

describe('plan shape and buffers', () => {
  /** A store with `pending` deliveries over 1,000 keys and every lane, analyzed. */
  async function seeded(pending: number): Promise<Fixture> {
    const fixture = await createFixture();
    await publishMany(
      fixture.store,
      events(pending, (index) => `key-${index % 1_000}`),
    );
    await analyze(fixture);
    return fixture;
  }

  it('claims, completes and renews through indexes, with buffers that do not grow with the backlog', async () => {
    const small = await seeded(5_000);
    const large = await seeded(50_000);
    const claimBuffers: number[] = [];
    for (const fixture of [small, large]) {
      // Warm: the worker already holds the lanes it claims from.
      const held = await claim(fixture.store, 50);
      await fixture.store.completeMany(held, at(1));
      const warm = await explain(fixture, () => claim(fixture.store, 50));
      claimBuffers.push(warm.buffers);
      if (fixture === large) {
        expectPlanWithinBudget('claimBacklogWarm', warm);
      }
    }
    // What ten times the backlog adds to a claim: a claim must not pay for the depth of the queue.
    expectWithinBudget(
      'postgres.explain.claimBacklogWarm.extraBuffers50kVs5k',
      Math.max(0, claimBuffers[1]! - claimBuffers[0]!),
    );

    const claimed = await claim(large.store, 50);
    expect(claimed).toHaveLength(50);
    const completion = await explain(large, () => large.store.completeMany(claimed, at(1)));
    expectPlanWithinBudget('complete50', completion);

    const [running] = await claim(large.store, 1);
    const renewal = await explain(large, () => large.store.extendLease(running!, at(LEASE_MS * 2)));
    expectPlanWithinBudget('extendLease', renewal);

    const publication = await explain(large, () =>
      publish(
        large.store,
        events(1, () => 'k'),
      ),
    );
    expectPlanWithinBudget('publishKeyedSingle', publication);
  });

  it('claims cheaply on an idle store and on one whose due work is all in flight', async () => {
    const idle = await createFixture();
    // Idle, but not empty: retries and delayed events wait for later.
    await publishMany(idle.store, events(5_000), ['h']);
    await idle.store.transaction((transaction) =>
      idle.store.append(
        transaction,
        events(5_000).map((event) => ({
          event,
          deliveries: [{ ...createDelivery('h', at(3_600_000)), id: nextId('delivery') }],
        })),
        at(0),
      ),
    );
    await drain(idle.store, 500);
    // What autovacuum does after such a drain: the dead index entries of the completed deliveries go.
    await database.pool.query(`VACUUM "${idle.schema}".relaybus_deliveries`);
    await analyze(idle);
    const nothing = await explain(idle, () => claim(idle.store, 10));
    expectPlanWithinBudget('claimIdle', nothing);

    const busy = await createFixture();
    await publishMany(
      busy.store,
      events(1_000, (index) => `hot-${index % 10}`),
    );
    await analyze(busy);
    expect(await claim(busy.store, 10)).toHaveLength(10);
    const blocked = await explain(busy, () => claim(busy.store, 10));
    expectPlanWithinBudget('claimHotKeysAllInFlight', blocked);
  });
});

describe('planner cost estimates', () => {
  // PostgreSQL compiles a statement with JIT when its estimated cost passes jit_above_cost (100,000 by
  // default). For these short statements compiling took about 100 ms, while running them takes 1-3 ms.
  const EIGHT_HANDLERS = ['h', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'h8'];

  it('keeps every claim statement far below the JIT threshold, unkeyed or keyed, whatever the backlog', async () => {
    // Unkeyed only: the key column is all null in the statistics, which is what misled the estimates.
    const unkeyed = await createFixture();
    await publishMany(unkeyed.store, events(50_000));
    await analyze(unkeyed);
    const cold = await explain(unkeyed, () => claim(unkeyed.store, 10, EIGHT_HANDLERS));
    const warm = await explain(unkeyed, () => claim(unkeyed.store, 10, EIGHT_HANDLERS));

    // Hot keys whose heads are all in flight: the claim looks behind the blocked followers.
    const keyed = await createFixture();
    await publishMany(
      keyed.store,
      events(20_000, (index) => `hot-${index % 10}`),
    );
    await analyze(keyed);
    expect(await claim(keyed.store, 10, EIGHT_HANDLERS)).toHaveLength(10);
    const blocked = await explain(keyed, () => claim(keyed.store, 10, EIGHT_HANDLERS));
    const other = await explain(keyed, () => claim(keyed.store, 10, EIGHT_HANDLERS, 'worker-2'));

    expectWithinBudget(
      'postgres.explain.claim.maximumEstimatedCost',
      Math.max(cold.maximumCost, warm.maximumCost, blocked.maximumCost, other.maximumCost),
    );
  });
});

describe('lock waits', () => {
  /** A store whose connections give up on any lock wait longer than 50 ms (SQLSTATE 55P03). */
  function impatientStore(schema: string): { store: PostgresEventStore; pool: Pool } {
    const pool = createTestPool(`${database.connectionString}?options=-c%20lock_timeout%3D50`, 4);
    pools.push(pool);
    return { store: new PostgresEventStore({ pool, schema }), pool };
  }

  it('lets claimers of disjoint lanes and their completions run without waiting for each other', async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(2_000));
    const first = impatientStore(fixture.schema);
    const second = impatientStore(fixture.schema);
    let handled = 0;
    const work = async (store: PostgresEventStore, owner: string) => {
      for (;;) {
        const claimed = await claim(store, 25, ['h'], owner);
        if (claimed.length === 0) {
          return;
        }
        expect(await store.completeMany(claimed, at(1))).toEqual(claimed.map(() => true));
        handled += claimed.length;
      }
    };
    await Promise.all([work(first.store, 'worker-a'), work(second.store, 'worker-b')]);
    expect(handled).toBe(2_000);
  });

  it("does not make a worker's claim and renewal wait for its own completion in progress", async () => {
    const fixture = await createFixture();
    await publishMany(fixture.store, events(20));
    const { store, pool } = impatientStore(fixture.schema);
    const claimed = await claim(store, 10);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // A transactional handler's completion holds its lane locks until the transaction ends.
      expect(await store.complete(claimed[0]!, at(1), client)).toBe(true);
      expect(await store.extendLease(claimed[1]!, at(LEASE_MS * 2))).toBe(true);
      expect((await claim(store, 10)).length).toBeGreaterThan(0);
      await client.query('COMMIT');
    } finally {
      client.release();
    }
  });

  it('lets publishers of different keys run side by side and serializes publishers of one key', async () => {
    const fixture = await createFixture();
    const { store, pool } = impatientStore(fixture.schema);
    const holder = await pool.connect();
    const other = await pool.connect();
    try {
      await holder.query('BEGIN');
      await store.append(
        holder,
        [{ event: createEnvelope({ key: 'a' }), deliveries: [createDelivery('h')] }],
        at(0),
      );
      await other.query('BEGIN');
      // Another key: no wait.
      await store.append(
        other,
        [{ event: createEnvelope({ key: 'b' }), deliveries: [createDelivery('h')] }],
        at(0),
      );
      // The same key: waits for the first publisher to commit, which is what keeps commit order.
      await expect(
        store.append(
          other,
          [{ event: createEnvelope({ key: 'a' }), deliveries: [createDelivery('h')] }],
          at(0),
        ),
      ).rejects.toMatchObject({ code: '55P03' });
    } finally {
      await holder.query('ROLLBACK');
      await other.query('ROLLBACK');
      holder.release();
      other.release();
    }
  });
});

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

describe('publish-to-handler latency at default settings', () => {
  const quietLogger = { log: () => undefined, warn: () => undefined, error: () => undefined };

  it('runs a handler soon after another process committed its event', async () => {
    const schema = uniqueSchema();
    const publisherPool = createTestPool(database.connectionString, 4);
    const consumerPool = createTestPool(database.connectionString, 10);
    pools.push(publisherPool, consumerPool);
    const publisher = new RelayBus<PoolClient>({
      store: new PostgresEventStore({ pool: publisherPool, schema }),
      dispatcher: { enabled: false },
      logger: quietLogger,
    });
    const consumer = new RelayBus<PoolClient>({
      store: new PostgresEventStore({ pool: consumerPool, schema }),
      logger: quietLogger,
    });
    const committedAt = new Map<string, number>();
    const startedAt = new Map<string, number>();
    const handler = (event: EventEnvelope<{ orderId: string }>) => {
      startedAt.set(event.payload.orderId, Date.now());
    };
    publisher.subscribe(OrderPlaced, handler, { id: 'h' });
    consumer.subscribe(OrderPlaced, handler, { id: 'h' });
    await consumer.start();
    await publisher.start();
    // Let the consumer fall asleep between two polls.
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    const total = 30;
    try {
      for (let index = 0; index < total; index += 1) {
        if (index === total / 2) {
          // A lost listening connection must not cost more than one poll interval.
          await database.pool.query(
            `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ILIKE 'LISTEN %'`,
          );
        }
        const orderId = String(index);
        await publisher.transaction(async (transaction) => {
          await publisher.publish(new OrderPlaced({ orderId }), { transaction });
        });
        committedAt.set(orderId, Date.now());
        const deadline = Date.now() + 5_000;
        while (!startedAt.has(orderId) && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        // Space the events out, so each one finds the consumer idle again.
        await new Promise((resolve) => setTimeout(resolve, 100 + ((index * 37) % 200)));
      }
    } finally {
      await publisher.stop();
      await consumer.stop();
    }
    expect(startedAt.size).toBe(total);
    // A handler can start a moment before the publisher saw its commit acknowledged: that is 0 ms.
    const sorted = [...committedAt]
      .map(([orderId, committed]) => Math.max(0, startedAt.get(orderId)! - committed))
      .sort((left, right) => left - right);
    expectWithinBudget('postgres.latency.defaults.p95Ms', sorted[Math.ceil(total * 0.95) - 1]!);
    expectWithinBudget('postgres.latency.defaults.maxMs', sorted[total - 1]!);
  });
});
