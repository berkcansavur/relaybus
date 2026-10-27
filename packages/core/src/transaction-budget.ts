import type { Clock } from './clock.js';
import { TransactionTimeoutError, toErrorInfo } from './errors.js';
import type { EventStore, TransactionIsolationLevel } from './event-store.js';

/** One `bus.transaction` with the time it may take. */
export type BudgetedTransaction<TTransaction, T> = {
  readonly clock: Clock;
  readonly timeoutMs: number;
  readonly work: (transaction: TTransaction) => Promise<T>;
  /** Passed to the store when set; a transaction without it is opened exactly as before. */
  readonly isolationLevel?: TransactionIsolationLevel;
};

/**
 * Runs `work` in a store transaction held to `timeoutMs`. The store makes the database enforce the
 * budget where it can; whatever runs past it fails with {@link TransactionTimeoutError} and rolls
 * back: a `work` that resolved too late is rejected before the commit, and an error `work` raised
 * once the budget ran out (a statement the server cancelled, a session it ended) becomes the
 * timeout's cause. A failed commit keeps its own error, since the commit may have taken effect.
 */
export async function runBudgetedTransaction<TTransaction, T>(
  store: EventStore<TTransaction>,
  { clock, timeoutMs, work, isolationLevel }: BudgetedTransaction<TTransaction, T>,
): Promise<T> {
  const startedAt = clock.now().getTime();
  const overrun = (): boolean => clock.now().getTime() - startedAt >= timeoutMs;
  // Set once `work` succeeded within the budget: a later failure belongs to the commit, whose
  // outcome may be unknown (a lost response), so it is never reported as a rollback.
  let committing = false;
  try {
    return await store.transaction(
      async (transaction) => {
        committing = false;
        const result = await work(transaction);
        if (overrun()) {
          throw createTimeoutError(timeoutMs, undefined);
        }
        committing = true;
        return result;
      },
      isolationLevel === undefined ? { timeoutMs } : { timeoutMs, isolationLevel },
    );
  } catch (error) {
    if (error instanceof TransactionTimeoutError || committing || !overrun()) {
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
