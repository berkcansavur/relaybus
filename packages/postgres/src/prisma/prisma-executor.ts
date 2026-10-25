import { adapterExecutor } from '../adapter-executor.js';
import type { PostgresExecutor, PostgresRow } from '../executor.js';

/** What a `PrismaClient` and an interactive-transaction client have in common. */
export interface PrismaQueryable {
  $queryRawUnsafe(query: string, ...values: unknown[]): PromiseLike<unknown>;
}

/** The part of a `PrismaClient` the executor uses. */
export interface PrismaClientLike extends PrismaQueryable {
  $transaction<T>(
    work: (transaction: PrismaQueryable) => Promise<T>,
    options?: PrismaTransactionOptions,
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
 * Not yet verified against a real Prisma client and database: it is covered by
 * unit tests with a structural fake only.
 */
export function createPrismaExecutor(
  client: PrismaClientLike,
  transactionOptions: PrismaTransactionOptions = {},
): PostgresExecutor<PrismaQueryable> {
  return adapterExecutor<PrismaQueryable>({
    root: client,
    transaction: (work) => client.$transaction(work, transactionOptions),
    async run(queryable, { sql, parameters }) {
      return (await queryable.$queryRawUnsafe(sql, ...parameters)) as PostgresRow[];
    },
  });
}
