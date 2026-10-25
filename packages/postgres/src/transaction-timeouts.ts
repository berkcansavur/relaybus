import type { PostgresStoreState } from './postgres-store-state.js';

/** Cancels any statement of the transaction that runs longer than the budget. Safe with every executor. */
const statementTimeoutSql = (value: string): string =>
  `SELECT set_config('statement_timeout', ${value}, true)`;

/**
 * Also ends the session when the transaction sits idle (waiting on the application) longer than the
 * budget, and on PostgreSQL 17+ when the whole transaction does. The server then closes the
 * connection, so only executors that survive that ({@link PostgresExecutor.sessionTimeouts}) get it.
 */
const sessionTimeoutsSql = (value: string): string =>
  `SELECT set_config('statement_timeout', ${value}, true), ` +
  `set_config('idle_in_transaction_session_timeout', ${value}, true), ` +
  "CASE WHEN current_setting('server_version_num')::int >= 170000 " +
  `THEN set_config('transaction_timeout', ${value}, true) END`;

/** The settings statement for `state`'s executor, with `value` as its parameter or literal. */
function timeoutSql<TTransaction>(state: PostgresStoreState<TTransaction>, value: string): string {
  return state.executor.sessionTimeouts === true
    ? sessionTimeoutsSql(value)
    : statementTimeoutSql(value);
}

/**
 * Runs `work` in a transaction of `state`'s executor that the server holds to `timeoutMs`: `SET LOCAL`
 * semantics, so the settings end with the transaction and never leak to the pooled connection. An
 * executor with `transactionWithSettings` gets them in the round trip of its `BEGIN`; the value is
 * then a literal (whole milliseconds, nothing else reaches the text). Any other executor runs them as
 * the first statement of the transaction.
 */
export function inBudgetedTransaction<TTransaction, T>(
  state: PostgresStoreState<TTransaction>,
  {
    work,
    timeoutMs,
  }: {
    readonly work: (transaction: TTransaction) => Promise<T>;
    readonly timeoutMs: number;
  },
): Promise<T> {
  const value = `${Math.ceil(timeoutMs)}ms`;
  const { executor } = state;
  if (executor.transactionWithSettings !== undefined) {
    return executor.transactionWithSettings(work, timeoutSql(state, `'${value}'`));
  }
  return executor.transaction(async (transaction) => {
    await state.query({ sql: timeoutSql(state, '$1'), parameters: [value], transaction });
    return work(transaction);
  });
}
