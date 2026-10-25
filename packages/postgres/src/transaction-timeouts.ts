import type { PostgresStoreState } from './postgres-store-state.js';

/** What {@link budgetSettingsSql} sets and how long the settings last. */
export type BudgetSettings = {
  /** The value as SQL: a parameter (`$1`) or a quoted literal (`'5000ms'`). */
  readonly value: string;
  /**
   * Also `idle_in_transaction_session_timeout` and, on PostgreSQL 17+, `transaction_timeout`: the
   * server then ends the session of an overrun, so only executors that survive that get them
   * ({@link PostgresExecutor.sessionTimeouts}).
   */
  readonly sessionTimeouts: boolean;
  /** `transaction`: `SET LOCAL` semantics, gone with the transaction. `session`: kept by the connection. */
  readonly scope: 'transaction' | 'session';
};

/** The statement that holds a transaction to a time budget: `statement_timeout` and, see {@link BudgetSettings}, more. */
export function budgetSettingsSql({ value, sessionTimeouts, scope }: BudgetSettings): string {
  const local = scope === 'transaction';
  const statementTimeout = `SELECT set_config('statement_timeout', ${value}, ${local})`;
  if (!sessionTimeouts) {
    return statementTimeout;
  }
  return (
    `${statementTimeout}, set_config('idle_in_transaction_session_timeout', ${value}, ${local}), ` +
    "CASE WHEN current_setting('server_version_num')::int >= 170000 " +
    `THEN set_config('transaction_timeout', ${value}, ${local}) END`
  );
}

/** Puts the settings {@link budgetSettingsSql} keeps at session level back to the defaults of the session. */
export const SESSION_DEFAULTS_SQL =
  'SELECT set_config(name, reset_val, false) FROM pg_settings ' +
  "WHERE name IN ('statement_timeout', 'idle_in_transaction_session_timeout', 'transaction_timeout')";

/** The budget as a setting value: whole milliseconds, so nothing but digits reaches SQL text. */
export function budgetValue(timeoutMs: number): string {
  return `${Math.ceil(timeoutMs)}ms`;
}

/**
 * Runs `work` in a transaction of `state`'s executor that the server holds to `timeoutMs`. An executor
 * with `transactionWithBudget` (`PgExecutor`) sets the budget in the round trip of its `BEGIN`; any
 * other executor runs it with `SET LOCAL` semantics as the first statement of the transaction.
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
  const { executor } = state;
  if (executor.transactionWithBudget !== undefined) {
    return executor.transactionWithBudget(work, timeoutMs);
  }
  const sql = budgetSettingsSql({
    value: '$1',
    sessionTimeouts: executor.sessionTimeouts === true,
    scope: 'transaction',
  });
  return executor.transaction(async (transaction) => {
    await state.query({ sql, parameters: [budgetValue(timeoutMs)], transaction });
    return work(transaction);
  });
}
