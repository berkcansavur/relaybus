import type { Clock } from './clock.js';
import { ConfigurationError, TransactionTimeoutError } from './errors.js';
import {
  TRANSACTION_ISOLATION_LEVELS,
  type EventStore,
  type TransactionIsolationLevel,
} from './event-store.js';
import { runInScope } from './execution-scope.js';
import type { TransactionOptions } from './relay-bus-options.js';
import { runBudgetedTransaction } from './transaction-budget.js';

/** Attempts of a transaction that fails with a serialization failure when `maxAttempts` is not set. */
export const DEFAULT_ISOLATED_ATTEMPTS = 3;

/** The levels at which a conflict with a concurrent transaction can roll a transaction back. */
const RERUN_LEVELS: ReadonlySet<TransactionIsolationLevel> = new Set([
  'repeatable read',
  'serializable',
]);

/** The options of a `bus.transaction` that asked for an isolation level. */
export type IsolatedOptions = TransactionOptions & {
  readonly isolationLevel: TransactionIsolationLevel;
};

/** What a transaction needs from the bus. */
export type IsolatedTransactionParts<TTransaction> = {
  readonly store: EventStore<TTransaction>;
  readonly clock: Clock;
  readonly transactionTimeoutMs: number;
};

/** One `bus.transaction` with an isolation level. */
export type IsolatedTransaction<TTransaction, T> = {
  readonly options: IsolatedOptions;
  readonly work: (transaction: TTransaction) => Promise<T>;
};

/**
 * Runs `work` in a store transaction at `options.isolationLevel`. At `repeatable read` and
 * `serializable` a serialization failure (the store's `isSerializationFailure`) rolls the attempt
 * back and `work` runs again in a new transaction, up to `maxAttempts` attempts in all; the last
 * failure is rethrown unchanged. Events a failed attempt published went with its rollback, and what
 * their `publish` calls announce (`onPublished`, the dispatcher wake-up) is dropped with it.
 */
export async function runIsolatedTransaction<TTransaction, T>(
  { store, clock, transactionTimeoutMs }: IsolatedTransactionParts<TTransaction>,
  { options, work }: IsolatedTransaction<TTransaction, T>,
): Promise<T> {
  const { isolationLevel } = options;
  assertSupported(store, options);
  const maxAttempts = attemptsOf(options);
  for (let attempt = 1; ; attempt += 1) {
    const afterCommit: Array<() => void> = [];
    let committed: { readonly value: T };
    try {
      committed = {
        value: await runBudgetedTransaction(store, {
          clock,
          timeoutMs: transactionTimeoutMs,
          isolationLevel,
          work: (transaction) =>
            runInScope(
              { transaction, isolation: { transaction, level: isolationLevel, afterCommit } },
              () => work(transaction),
            ),
        }),
      };
    } catch (error) {
      // A timeout wraps whatever the server raised when it ended the transaction, 40001 included.
      if (
        attempt >= maxAttempts ||
        error instanceof TransactionTimeoutError ||
        store.isSerializationFailure?.(error) !== true
      ) {
        throw error;
      }
      continue;
    }
    // Only the attempt that committed announces its events.
    for (const announce of afterCommit) {
      announce();
    }
    return committed.value;
  }
}

/** How many times `work` may run: once at `read committed`, whose conflicts a rerun does not cure. */
function attemptsOf({ isolationLevel, maxAttempts }: IsolatedOptions): number {
  return RERUN_LEVELS.has(isolationLevel) ? (maxAttempts ?? DEFAULT_ISOLATED_ATTEMPTS) : 1;
}

function assertSupported<TTransaction>(
  store: EventStore<TTransaction>,
  { isolationLevel, maxAttempts }: IsolatedOptions,
): void {
  if (!TRANSACTION_ISOLATION_LEVELS.includes(isolationLevel)) {
    throw new ConfigurationError(
      `Unknown isolationLevel ${JSON.stringify(isolationLevel)}: use ${TRANSACTION_ISOLATION_LEVELS.map((level) => `'${level}'`).join(', ')}`,
    );
  }
  if (maxAttempts !== undefined && !(Number.isInteger(maxAttempts) && maxAttempts >= 1)) {
    throw new ConfigurationError(
      `maxAttempts must be a positive integer, got ${String(maxAttempts)}`,
    );
  }
  if (store.isolationLevels?.includes(isolationLevel) !== true) {
    const supported = store.isolationLevels ?? [];
    throw new ConfigurationError(
      `The event store does not support isolationLevel '${isolationLevel}' (supported: ${supported.length === 0 ? 'none' : supported.join(', ')}). ` +
        'PostgreSQL stores support all levels; MongoDB and the in-memory store run transactions at their own level only',
    );
  }
}
