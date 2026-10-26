import { describe, expect, it, vi } from 'vitest';
import {
  createPrismaExecutor,
  type PrismaClientLike,
  type PrismaQueryable,
} from '../src/prisma/index.js';

function createFakePrisma() {
  const transactionClient: PrismaQueryable = {
    $queryRawUnsafe: vi.fn(async () => [{ source: 'transaction' }]),
  };
  const client: PrismaClientLike = {
    $queryRawUnsafe: vi.fn(async () => [{ source: 'client' }]),
    $transaction: vi.fn(async (work: (client: PrismaQueryable) => Promise<unknown>) =>
      work(transactionClient),
    ) as unknown as PrismaClientLike['$transaction'],
  };
  return { client, transactionClient };
}

describe('createPrismaExecutor (structural fake; adapters.integration.test.ts runs a real client)', () => {
  it('spreads parameters into $queryRawUnsafe on the client', async () => {
    const { client } = createFakePrisma();
    const executor = createPrismaExecutor(client);
    await expect(executor.query('SELECT $1, $2', ['a', 2])).resolves.toEqual([
      { source: 'client' },
    ]);
    expect(client.$queryRawUnsafe).toHaveBeenCalledWith('SELECT $1, $2', 'a', 2);
  });

  it('runs on the interactive transaction client and forwards transaction options', async () => {
    const { client, transactionClient } = createFakePrisma();
    const executor = createPrismaExecutor(client, { timeout: 20_000, maxWait: 5_000 });
    const result = await executor.transaction(async (transaction) => {
      expect(transaction).toBe(transactionClient);
      return executor.query('SELECT 1', [], transaction);
    });
    expect(result).toEqual([{ source: 'transaction' }]);
    expect(client.$transaction).toHaveBeenCalledWith(expect.any(Function), {
      timeout: 20_000,
      maxWait: 5_000,
    });
  });

  it('passes empty options by default', async () => {
    const { client } = createFakePrisma();
    await createPrismaExecutor(client).transaction(async () => undefined);
    expect(client.$transaction).toHaveBeenCalledWith(expect.any(Function), {});
  });
});
