import { afterAll, beforeAll, describe, it } from 'vitest';
import type { Pool, PoolClient } from 'pg';
import { runStoreStress } from '../../../test-support/store-stress.js';
import { PgExecutor, PostgresEventStore, type PostgresExecutor } from '../src/index.js';
import {
  createTestPool,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

let database: TestDatabase;
const pools: Pool[] = [];

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await Promise.all(pools.map((pool) => pool.end()));
  await database.stop();
});

describe('many dispatchers on shared lanes', () => {
  it('never deadlocks, loses, duplicates or reorders a delivery', async () => {
    const schema = uniqueSchema();
    let lose: () => boolean = () => false;
    const lost = new Error('claim answer lost after the database ran it');
    await runStoreStress({
      async createStore() {
        const pool = createTestPool(database.connectionString, 4);
        pools.push(pool);
        const inner = new PgExecutor(pool);
        const executor: PostgresExecutor<PoolClient> = {
          transaction: (work) => inner.transaction(work),
          async query(sql, parameters, transaction) {
            const rows = await inner.query(sql, parameters, transaction);
            // The one-statement claim: the lanes recorded the batch, the caller never hears of it.
            if (sql.startsWith('WITH held AS MATERIALIZED') && lose()) {
              throw lost;
            }
            return rows;
          },
        };
        const store = new PostgresEventStore({ executor, schema });
        await store.setup();
        return store;
      },
      loseClaimAnswers(next) {
        lose = next;
        return lost;
      },
      async openTransactions() {
        const { rows } = await database.pool.query<{ open: number }>(
          `SELECT count(*)::int AS open FROM pg_stat_activity
           WHERE datname = current_database() AND state LIKE 'idle in transaction%'`,
        );
        return rows[0]!.open;
      },
    });
  });
});
