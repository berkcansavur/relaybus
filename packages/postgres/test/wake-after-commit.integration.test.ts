import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { PrismaPg } from '@prisma/adapter-pg';
import type { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RelayBus, defineEvent } from '@relaybus/core';
import { PostgresEventStore } from '../src/index.js';
import { createPrismaExecutor, type PrismaQueryable } from '../src/prisma/index.js';
import { PrismaClient } from './prisma/generated/client.js';
import {
  createTestPool,
  dropSchema,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

/**
 * Latency design test of the wake-up after a commit of the application's transaction: the bus cannot
 * see that commit, so with `pool` next to the executor the store relies on the `pg_notify` its publish
 * statement sends inside that transaction, which PostgreSQL delivers at the commit. The commit is held
 * back 200 ms after the publish, longer than any fixed wake-up delay, and the poll interval is 10 s, so
 * only a wake-up at the commit can start the handler in time.
 */

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

/** The last fixed wake-up after a publish in a caller-owned transaction (core publish-wake-ups.ts). */
const LAST_FIXED_WAKE_UP_MS = 250;
/**
 * How long each measured transaction stays open after its publish. Longer than the last fixed wake-up,
 * so every fixed wake-up fires before the commit: a lost notification then waits for the poll (10 s)
 * and the round times out instead of passing on a fixed wake-up.
 */
const COMMIT_DELAY_MS = LAST_FIXED_WAKE_UP_MS + 150;
/** Median handler start after the commit returned to the application, at most, once the claim's plan is cached. */
const AFTER_COMMIT_BUDGET_MS = 10;
/**
 * Median handler start after the commit in the first rounds of a fresh setup, at most. Postgres plans
 * the prepared claim again for its first 5 executions on each connection, measured at 4-8 ms per claim
 * on top of the steady-state latency; CI (ubuntu-latest, node 24) measured an 11.43 ms median there.
 */
const COLD_START_BUDGET_MS = 15;
/**
 * Slowest round in the first rounds of a fresh setup, at most. A claim that lands on a connection
 * preparing it for the first time pays parse and planning on a backend with cold catalog caches:
 * measured at 4-11 ms parse plus 13-63 ms bind on the server and up to 92 ms for the round, with the
 * notification and the claim start within 2 ms of the commit; one local run reached 151 ms. A lost
 * wake-up cannot pass: the commit comes after every fixed wake-up, so only the poll (10 s) could start it.
 */
const COLD_START_CEILING_MS = 250;
/** Slowest round, at most: a claim on a machine loaded by the rest of the suite. The poll would take 10 s. */
const AFTER_COMMIT_CEILING_MS = 50;
const ROUNDS = 9;
/** Connections of the pool the store claims on. */
const POOL_SIZE = 10;
/** Executions of a prepared statement Postgres plans one by one before it may keep a cached plan. */
const CUSTOM_PLAN_EXECUTIONS = 5;
/**
 * Rounds before the steady-state measurement. Each round runs at least one claim; with this many, the
 * claims cover every connection of the pool past its custom-plan executions even when they are spread
 * evenly over all of them, so no measured claim pays for planning.
 */
const WARM_UP_ROUNDS = POOL_SIZE * CUSTOM_PLAN_EXECUTIONS;

function median(values: readonly number[]): number {
  return [...values].sort((left, right) => left - right)[Math.floor(values.length / 2)]!;
}

let database: TestDatabase;

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

interface Setup {
  readonly bus: RelayBus<PrismaQueryable>;
  readonly prisma: PrismaClient;
  readonly started: Map<string, number>;
  close(): Promise<void>;
}

async function setUp(withPool: boolean): Promise<Setup> {
  const schema = uniqueSchema();
  const pool: Pool = createTestPool(database.connectionString, POOL_SIZE);
  const prisma = new PrismaClient({ adapter: new PrismaPg(pool) });
  const store = new PostgresEventStore({
    executor: createPrismaExecutor(prisma),
    schema,
    ...(withPool ? { pool } : {}),
  });
  const bus = new RelayBus({ store, dispatcher: { pollInterval: '10s' } });
  const started = new Map<string, number>();
  bus.subscribe(
    OrderPlaced,
    (event) => void started.set(event.payload.orderId, performance.now()),
    {
      id: 'timed',
    },
  );
  await bus.start();
  return {
    bus,
    prisma,
    started,
    async close() {
      await bus.stop();
      await prisma.$disconnect();
      await pool.end();
      await dropSchema(database.pool, schema);
    },
  };
}

/** Publishes in a Prisma transaction that commits `COMMIT_DELAY_MS` later; resolves with the time the commit returned. */
async function publishWithSlowCommit({ bus, prisma }: Setup, orderId: string): Promise<number> {
  await prisma.$transaction(async (transaction) => {
    await bus.publish(new OrderPlaced({ orderId }), { transaction });
    await sleep(COMMIT_DELAY_MS);
  });
  return performance.now();
}

/** Publishes in a Prisma transaction that commits at once and waits for the handler: warms the claim's plan. */
async function warmUp(setup: Setup): Promise<void> {
  for (let round = 0; round < WARM_UP_ROUNDS; round += 1) {
    await setup.prisma.$transaction(async (transaction) => {
      await setup.bus.publish(new OrderPlaced({ orderId: `warm-up-${round}` }), { transaction });
    });
    await startOf(setup, `warm-up-${round}`);
  }
}

/** Latency of each of `ROUNDS` slow commits: handler start minus the time the commit returned. */
async function measureRounds(setup: Setup): Promise<number[]> {
  const latencies: number[] = [];
  for (let round = 0; round < ROUNDS; round += 1) {
    const committedAt = await publishWithSlowCommit(setup, `order-${round}`);
    latencies.push((await startOf(setup, `order-${round}`)) - committedAt);
  }
  return latencies;
}

async function startOf(setup: Setup, orderId: string, timeoutMs = 2_000): Promise<number> {
  const deadline = performance.now() + timeoutMs;
  while (!setup.started.has(orderId)) {
    if (performance.now() > deadline) {
      throw new Error(`handler for ${orderId} did not start within ${timeoutMs} ms`);
    }
    await sleep(1);
  }
  return setup.started.get(orderId)!;
}

describe('wake-up after a commit of the application transaction', () => {
  it(`starts the handler a median ${AFTER_COMMIT_BUDGET_MS} ms after a commit held back ${COMMIT_DELAY_MS} ms once the claim plan is cached (Prisma with pool)`, async () => {
    const setup = await setUp(true);
    await warmUp(setup);
    const latencies = await measureRounds(setup);
    expect(median(latencies)).toBeLessThanOrEqual(AFTER_COMMIT_BUDGET_MS);
    expect(Math.max(...latencies)).toBeLessThanOrEqual(AFTER_COMMIT_CEILING_MS);
    await setup.close();
  });

  it(`starts the handler a median ${COLD_START_BUDGET_MS} ms after a commit in the first rounds of a fresh setup (Prisma with pool)`, async () => {
    const setup = await setUp(true);
    const latencies = await measureRounds(setup);
    expect(median(latencies)).toBeLessThanOrEqual(COLD_START_BUDGET_MS);
    expect(Math.max(...latencies)).toBeLessThanOrEqual(COLD_START_CEILING_MS);
    await setup.close();
  });

  it('without pool waits past the last fixed wake-up until the application calls bus.wake()', async () => {
    const setup = await setUp(false);
    await setup.prisma.$transaction(async (transaction) => {
      await setup.bus.publish(new OrderPlaced({ orderId: 'late' }), { transaction });
      // Past the wake-ups 25, 100 and 250 ms after the publish.
      await sleep(400);
    });
    await sleep(100);
    expect(setup.started.has('late')).toBe(false);
    const wokenAt = performance.now();
    setup.bus.wake();
    expect((await startOf(setup, 'late')) - wokenAt).toBeLessThanOrEqual(AFTER_COMMIT_CEILING_MS);
    await setup.close();
  });
});
