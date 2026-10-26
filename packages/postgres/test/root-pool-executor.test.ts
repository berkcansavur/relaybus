import type { PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import type { PostgresExecutor } from '../src/index.js';
import { withRootPool } from '../src/root-pool-executor.js';

function root(): PostgresExecutor<PoolClient> {
  return {
    transaction: vi.fn(),
    query: vi.fn(async () => [{ on: 'root' }]),
  };
}

describe('withRootPool', () => {
  it('runs statements outside a transaction on the root pool, with the query options', async () => {
    const pool = root();
    const orm: PostgresExecutor<string> = {
      transaction: (work) => work('orm-transaction'),
      query: vi.fn(async () => [{ on: 'orm' }]),
    };
    const executor = withRootPool(orm, pool);
    await expect(executor.query('SELECT $1', ['a'], undefined, { prepare: true })).resolves.toEqual(
      [{ on: 'root' }],
    );
    expect(pool.query).toHaveBeenCalledWith('SELECT $1', ['a'], undefined, { prepare: true });
    await expect(
      executor.transaction((transaction) => executor.query('SELECT 1', [], transaction)),
    ).resolves.toEqual([{ on: 'orm' }]);
    expect(orm.query).toHaveBeenCalledWith('SELECT 1', [], 'orm-transaction', undefined);
    expect(executor.sessionTimeouts).toBeUndefined();
    expect(executor.transactionWithBudget).toBeUndefined();
  });

  it('keeps the session timeouts and the budgeted transaction of the executor', async () => {
    const orm: PostgresExecutor<string> = {
      sessionTimeouts: true,
      transaction: (work) => work('plain'),
      transactionWithBudget: vi.fn((work: (transaction: string) => Promise<unknown>) =>
        work('budgeted'),
      ) as NonNullable<PostgresExecutor<string>['transactionWithBudget']>,
      query: async () => [],
    };
    const executor = withRootPool(orm, root());
    expect(executor.sessionTimeouts).toBe(true);
    await expect(
      executor.transactionWithBudget!(async (transaction) => transaction, 50),
    ).resolves.toBe('budgeted');
    expect(orm.transactionWithBudget).toHaveBeenCalledWith(expect.any(Function), 50);
  });
});
