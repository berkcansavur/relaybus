import type { TransactionIsolationLevel } from '@relaybus/core';
import { adapterExecutor } from '../adapter-executor.js';
import type { PostgresExecutor, PostgresRow } from '../executor.js';
import { isolationKeywords } from '../transaction-isolation.js';

export type PrismaIsolationLevel = 'ReadCommitted' | 'RepeatableRead' | 'Serializable';

const PRISMA_ISOLATION_LEVELS: Readonly<Record<TransactionIsolationLevel, PrismaIsolationLevel>> = {
  'read committed': 'ReadCommitted',
  'repeatable read': 'RepeatableRead',
  serializable: 'Serializable',
};

/** What a `PrismaClient` and an interactive-transaction client have in common. */
export interface PrismaQueryable {
  $queryRawUnsafe(query: string, ...values: unknown[]): PromiseLike<unknown>;
}

/** The part of a `PrismaClient` the executor uses. */
export interface PrismaClientLike extends PrismaQueryable {
  $transaction<T>(
    work: (transaction: PrismaQueryable) => Promise<T>,
    options?: PrismaTransactionOptions & { readonly isolationLevel?: PrismaIsolationLevel },
  ): Promise<T>;
}

/** Prisma interactive transaction limits. Prisma's defaults (2 s wait, 5 s run) are short for handlers. */
export interface PrismaTransactionOptions {
  readonly maxWait?: number;
  readonly timeout?: number;
}

/**
 * Executor for Prisma, typed structurally so the package has no Prisma
 * dependency. The transaction handle is the client of an interactive
 * transaction: pass `tx` from `prisma.$transaction(async (tx) => ...)`.
 *
 * Integration-tested with generated Prisma Clients 5, 6 and 7 on the `@prisma/adapter-pg` driver adapter.
 */
export function createPrismaExecutor(
  client: PrismaClientLike,
  transactionOptions: PrismaTransactionOptions = {},
): PostgresExecutor<PrismaQueryable> {
  return adapterExecutor<PrismaQueryable>({
    root: client,
    transaction: (work) => client.$transaction(work, transactionOptions),
    transactionAt: (isolationLevel, work) => {
      isolationKeywords(isolationLevel);
      return client.$transaction(work, {
        ...transactionOptions,
        isolationLevel: PRISMA_ISOLATION_LEVELS[isolationLevel],
      });
    },
    async run(queryable, { sql, parameters }) {
      return (await queryable.$queryRawUnsafe(sql, ...parameters)) as PostgresRow[];
    },
  });
}
