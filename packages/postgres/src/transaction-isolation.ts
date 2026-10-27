import { ConfigurationError, type TransactionIsolationLevel } from '@relaybus/core';
import type { PostgresStoreState } from './postgres-store-state.js';
import { budgetSettingsSql, budgetValue } from './transaction-timeouts.js';

/** The SQL keywords of each level. Only these literals ever reach SQL text. */
const ISOLATION_KEYWORDS: Readonly<Record<TransactionIsolationLevel, string>> = {
  'read committed': 'READ COMMITTED',
  'repeatable read': 'REPEATABLE READ',
  serializable: 'SERIALIZABLE',
};

/** The keywords of `isolationLevel` as SQL; an unknown level (a store called around the bus) is refused. */
export function isolationKeywords(isolationLevel: TransactionIsolationLevel): string {
  const keywords = Object.hasOwn(ISOLATION_KEYWORDS, isolationLevel)
    ? ISOLATION_KEYWORDS[isolationLevel]
    : undefined;
  if (keywords === undefined) {
    throw new ConfigurationError(`Unknown isolation level ${JSON.stringify(isolationLevel)}`);
  }
  return keywords;
}

/**
 * Runs `work` in a transaction at `isolationLevel` that the server holds to `timeoutMs`. An executor
 * with `transactionWithIsolation` (`PgExecutor`) sends both with its `BEGIN`; any other executor gets
 * `SET TRANSACTION ISOLATION LEVEL` as the first statement of the transaction and the budget as the
 * second: PostgreSQL refuses the level once a query ran, and the budget's `set_config` is a query.
 */
export function inIsolatedTransaction<TTransaction, T>(
  state: PostgresStoreState<TTransaction>,
  {
    work,
    timeoutMs,
    isolationLevel,
  }: {
    readonly work: (transaction: TTransaction) => Promise<T>;
    readonly timeoutMs: number;
    readonly isolationLevel: TransactionIsolationLevel;
  },
): Promise<T> {
  const { executor } = state;
  if (executor.transactionWithIsolation !== undefined) {
    return executor.transactionWithIsolation(work, { isolationLevel, timeoutMs });
  }
  const keywords = isolationKeywords(isolationLevel);
  const budget = budgetSettingsSql({
    value: '$1',
    sessionTimeouts: executor.sessionTimeouts === true,
    scope: 'transaction',
  });
  return executor.transaction(async (transaction) => {
    await executor.query(`SET TRANSACTION ISOLATION LEVEL ${keywords}`, [], transaction);
    await state.query({ sql: budget, parameters: [budgetValue(timeoutMs)], transaction });
    return work(transaction);
  });
}
