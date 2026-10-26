import { PrismaPg } from '@prisma/adapter-pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Kysely, PostgresDialect } from 'kysely';
import type { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { at } from '@relaybus/testing';
import { expectWithinBudget } from '../../../test-support/perf-budget.js';
import { PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import { createDrizzleExecutor } from '../src/drizzle/index.js';
import { createKyselyExecutor } from '../src/kysely/index.js';
import { createPrismaExecutor } from '../src/prisma/index.js';
import { PrismaClient } from './prisma/generated/client.js';
import { createTestPool, uniqueSchema, type TestDatabase } from './support/postgres-container.js';
import { claim, events, publish, startPerfDatabase } from './support/perf-design.js';

/**
 * Whether an ORM executor gets the prepared statements `PgExecutor` gives the store. An ORM sends
 * every raw statement unnamed, so the server parses and plans each one again. With `pool` next to
 * `executor`, the statements the store sends outside the application's transactions (the dispatcher's
 * claims and completions) run on that `pg` pool as prepared statements. Counted on the wire, like the
 * other design tests.
 */

const TRANSACTION_CONTROL = new Set(['BEGIN', 'COMMIT', 'ROLLBACK']);

/** Counts the statements the server parses from their text on every connection of `pool`. */
function countParses(pool: Pool): { parsed: number } {
  const counter = { parsed: 0 };
  pool.on('connect', (client) => {
    const prepared = new Set<string>();
    const query = client.query.bind(client) as (...arguments_: unknown[]) => unknown;
    (client as { query: unknown }).query = (...arguments_: unknown[]) => {
      const [first] = arguments_;
      const name = typeof first === 'object' ? (first as { name?: string }).name : undefined;
      const text = typeof first === 'string' ? first : (first as { text: string }).text;
      if (name === undefined ? !TRANSACTION_CONTROL.has(text.trim()) : !prepared.has(name)) {
        counter.parsed += 1;
      }
      if (name !== undefined) {
        prepared.add(name);
      }
      return query(...arguments_);
    };
  });
  return counter;
}

interface Orm {
  readonly name: string;
  create(pool: Pool): { executor: PostgresExecutor<unknown>; close(): Promise<void> };
}

const orms: readonly Orm[] = [
  {
    name: 'Prisma',
    create(pool) {
      const prisma = new PrismaClient({ adapter: new PrismaPg(pool) });
      return {
        executor: createPrismaExecutor(prisma),
        close: () => prisma.$disconnect(),
      };
    },
  },
  {
    name: 'Kysely',
    create(pool) {
      const db = new Kysely<unknown>({ dialect: new PostgresDialect({ pool }) });
      return {
        executor: createKyselyExecutor(db),
        close: async () => undefined,
      };
    },
  },
  {
    name: 'Drizzle',
    create(pool) {
      return {
        executor: createDrizzleExecutor(drizzle(pool)),
        close: async () => undefined,
      };
    },
  },
];

let database: TestDatabase;
const pools: Pool[] = [];

beforeAll(async () => {
  database = await startPerfDatabase();
});

afterAll(async () => {
  await Promise.all(pools.map((pool) => pool.end()));
  await database.stop();
});

/** Statements parsed for a warm claim of 10 and the completion of what it claimed. */
async function parsedPerDispatchBatch(orm: Orm, withPool: boolean): Promise<number> {
  const pool = createTestPool(database.connectionString, 1);
  pools.push(pool);
  const counter = countParses(pool);
  const { executor, close } = orm.create(pool);
  const store = new PostgresEventStore({
    executor,
    schema: uniqueSchema(),
    ...(withPool ? { pool } : {}),
  }) as unknown as PostgresEventStore;
  await store.setup();
  await publish(store, events(20));
  // Warm: every statement of a batch ran once on the connections before.
  await store.completeMany(await claim(store, 10), at(1));
  const before = counter.parsed;
  const claimed = await claim(store, 10);
  expect(claimed).toHaveLength(10);
  await store.completeMany(claimed, at(1));
  const parsed = counter.parsed - before;
  await close();
  return parsed;
}

describe.each(orms)('$name executor', (orm) => {
  it('parses both statements of a dispatch batch (claim, complete) without a pool', async () => {
    expect(await parsedPerDispatchBatch(orm, false)).toBe(2);
  });

  it('parses none once warm with the pool for statements outside the application transaction', async () => {
    expectWithinBudget(
      'postgres.dispatch.ormParsedStatementsPerBatch',
      await parsedPerDispatchBatch(orm, true),
    );
  });
});
