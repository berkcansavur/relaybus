import type { PostgresStoreState } from './postgres-store-state.js';

/** Cancels any statement of the transaction that runs longer than the budget. Safe with every executor. */
const STATEMENT_TIMEOUT_SQL = "SELECT set_config('statement_timeout', $1, true)";

/**
 * Also ends the session when the transaction sits idle (waiting on the application) longer than the
 * budget, and on PostgreSQL 17+ when the whole transaction does. The server then closes the
 * connection, so only executors that survive that ({@link PostgresExecutor.sessionTimeouts}) get it.
 */
const SESSION_TIMEOUTS_SQL =
  "SELECT set_config('statement_timeout', $1, true), " +
  "set_config('idle_in_transaction_session_timeout', $1, true), " +
  "CASE WHEN current_setting('server_version_num')::int >= 170000 " +
  "THEN set_config('transaction_timeout', $1, true) END";

/**
 * Makes the server hold the open transaction to `timeoutMs`: `SET LOCAL` semantics, so the settings
 * end with the transaction and never leak to the pooled connection.
 */
export async function applyTransactionTimeout<TTransaction>(
  state: PostgresStoreState<TTransaction>,
  { transaction, timeoutMs }: { readonly transaction: TTransaction; readonly timeoutMs: number },
): Promise<void> {
  await state.query({
    sql: state.executor.sessionTimeouts === true ? SESSION_TIMEOUTS_SQL : STATEMENT_TIMEOUT_SQL,
    parameters: [`${Math.ceil(timeoutMs)}ms`],
    transaction,
  });
}
