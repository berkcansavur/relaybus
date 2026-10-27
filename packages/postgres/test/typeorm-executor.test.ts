import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import { createTypeOrmExecutor, type TypeOrmQueryable } from '../src/typeorm/index.js';

function createFakeDataSource() {
  const manager: TypeOrmQueryable = { query: vi.fn(async () => [{ on: 'manager' }]) };
  // Two named parameters, like TypeORM's own transaction(isolationLevelOrWork, work).
  const transaction = vi.fn(async function (_levelOrWork: unknown, work?: unknown) {
    return ((work ?? _levelOrWork) as (manager: TypeOrmQueryable) => Promise<unknown>)(manager);
  });
  const dataSource = {
    query: vi.fn(async () => [{ on: 'root' }]),
    transaction,
  } as unknown as Parameters<typeof createTypeOrmExecutor>[0];
  return { dataSource, manager, transaction };
}

describe('createTypeOrmExecutor transactionWithIsolation (structural fake; adapters.integration.test.ts runs TypeORM)', () => {
  it('opens the transaction through TypeORM at the level, then sets the budget', async () => {
    const { dataSource, manager, transaction } = createFakeDataSource();
    const executor = createTypeOrmExecutor(dataSource);

    const result = await executor.transactionWithIsolation!(async (handle) => handle, {
      isolationLevel: 'repeatable read',
      timeoutMs: 400,
    });

    expect(result).toBe(manager);
    expect(transaction).toHaveBeenCalledWith('REPEATABLE READ', expect.any(Function));
    expect(manager.query).toHaveBeenCalledTimes(1);
    expect(manager.query).toHaveBeenCalledWith(
      expect.stringMatching(/^SELECT set_config\('statement_timeout', \$1, true\)/),
      ['400ms'],
    );
  });

  it('leaves a wrapper written for the one-argument contract to the generic path', () => {
    const wrapper = {
      query: vi.fn(),
      transaction: vi.fn(async (work: (manager: TypeOrmQueryable) => Promise<unknown>) =>
        work({ query: vi.fn() }),
      ),
    };

    const executor = createTypeOrmExecutor(
      wrapper as unknown as Parameters<typeof createTypeOrmExecutor>[0],
    );

    expect(executor.transactionWithIsolation).toBeUndefined();
  });

  it('refuses an unknown level', () => {
    const { dataSource, transaction } = createFakeDataSource();
    const executor = createTypeOrmExecutor(dataSource);

    expect(() =>
      executor.transactionWithIsolation!(async () => undefined, {
        isolationLevel: 'snapshot' as never,
        timeoutMs: 400,
      }),
    ).toThrow(ConfigurationError);
    expect(transaction).not.toHaveBeenCalled();
  });
});
