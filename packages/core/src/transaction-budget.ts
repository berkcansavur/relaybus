import type { Clock } from './clock.js';
import { TransactionTimeoutError, toErrorInfo } from './errors.js';
import type { EventStore } from './event-store.js';

/** One `bus.transaction` with the time it may take. */
export type BudgetedTransaction<TTransaction, T> = {
  readonly clock: Clock;
  readonly timeoutMs: number;
  readonly work: (transaction: TTransaction) => Promise<T>;
};

/**
 * Runs `work` in a store transaction held to `timeoutMs`. The store makes the database enforce the
 * budget where it can; whatever runs past it fails with {@link TransactionTimeoutError} and rolls
 * back: a `work` that resolved too late is rejected before the commit, and an error raised once the
 * budget ran out (a statement the server cancelled, a session it ended) becomes the timeout's cause.
 */
export async function runBudgetedTransaction<TTransaction, T>(
  store: EventStore<TTransaction>,
  { clock, timeoutMs, work }: BudgetedTransaction<TTransaction, T>,
): Promise<T> {
  const startedAt = clock.now().getTime();
  const overrun = (): boolean => clock.now().getTime() - startedAt >= timeoutMs;
  try {
    return await store.transaction(
      async (transaction) => {
        const result = await work(transaction);
        if (overrun()) {
          throw createTimeoutError(timeoutMs, undefined);
        }
        return result;
      },
      { timeoutMs },
    );
  } catch (error) {
    if (error instanceof TransactionTimeoutError || !overrun()) {
      throw error;
    }
    throw createTimeoutError(timeoutMs, error);
  }
}

function createTimeoutError(timeoutMs: number, cause: unknown): TransactionTimeoutError {
  const message =
    `The transaction ran past its ${timeoutMs} ms budget (transactionTimeout) and was rolled back. ` +
    'The usual cause is a slow call to another service (HTTP, gRPC, a broker, a lock) inside the ' +
    'transaction: make external calls before the transaction starts or after it commits, or raise ' +
    'transactionTimeout';
  return cause === undefined
    ? new TransactionTimeoutError(message)
    : new TransactionTimeoutError(`${message} | cause=${toErrorInfo(cause).message}`, { cause });
}
