import { createHash } from 'node:crypto';
import type { RelayBusLogger } from '@relaybus/core';
import type { Pool, PoolClient } from 'pg';
import type {
  PostgresExecutor,
  PostgresIsolatedTransactionOptions,
  PostgresParameter,
  PostgresQueryOptions,
  PostgresRow,
} from './executor.js';
import { isolationKeywords } from './transaction-isolation.js';
import { SESSION_DEFAULTS_SQL, budgetSettingsSql, budgetValue } from './transaction-timeouts.js';

/**
 * SQLSTATEs of a named statement the server does not know (26000) or already has under that name
 * (42P05). Both mean the connection the statement ran on is not the session that prepared it, which is
 * what a pooler in transaction mode without prepared statement support (PgBouncer before 1.21) does.
 */
const FOREIGN_SESSION_ERRORS = new Set(['26000', '42P05']);

export interface PgExecutorOptions {
  /** Receives the warning sent when prepared statements are switched off. Default `console`. */
  readonly logger?: RelayBusLogger;
  /**
   * The pool is this executor's alone (the store sets it for the pool it creates from
   * `connectionString`). A transaction budget then becomes a session setting of the connection, sent
   * with `BEGIN` only when the connection does not have it yet, and a transaction without a budget on
   * such a connection puts the session defaults back in its `BEGIN`. Statements outside a
   * transaction run under the budget the connection last got. Never set it for a pool other code
   * uses, or behind a pooler in transaction mode (session settings would reach other clients); the
   * executor stops it by itself when it notices a pooler (see prepared statements). Default `false`.
   */
  readonly sessionBudget?: boolean;
}

/** How a transaction starts on one connection, and what its commit tells about the session. */
type TransactionStart = {
  readonly begin: string;
  /** The budget the session keeps once the transaction committed; `null` for the defaults. */
  readonly sessionValue?: string | null;
};

/** Hex digits of the statement hash in a prepared statement name. */
const STATEMENT_NAME_DIGITS = 24;

/**
 * Executor for a `pg` {@link Pool}. A transaction is a dedicated pool client
 * between `BEGIN` and `COMMIT`; it is handed to callers as the transaction
 * handle, so they can run their own queries on it.
 *
 * A statement sent with `prepare: true` is a named prepared statement: each
 * connection parses and plans its text once and from then on only binds the
 * parameters. Its name is derived from the text, so stores with different
 * tables never share one. When the server reports that a prepared statement
 * does not exist on the connection, or already does (a pooler such as
 * PgBouncer before 1.21 in transaction mode hands every transaction another
 * server session), the executor stops preparing for good, logs a warning, and
 * runs the statement again unprepared unless it belonged to a transaction,
 * which has failed by then and is the caller's to retry.
 */
export class PgExecutor implements PostgresExecutor<PoolClient> {
  readonly #pool: Pool;
  readonly #logger: RelayBusLogger;
  readonly #names = new Map<string, string>();
  #preparing = true;
  readonly #sessionBudget: boolean;
  /** The budget each connection keeps at session level (`sessionBudget`); absent for the defaults. */
  readonly #sessionValues = new WeakMap<PoolClient, string>();

  constructor(pool: Pool, options: PgExecutorOptions = {}) {
    this.#pool = pool;
    this.#logger = options.logger ?? console;
    this.#sessionBudget = options.sessionBudget === true;
  }

  /** The connection listens for `error` during the transaction, so a server-ended session fails the transaction instead of the process. */
  readonly sessionTimeouts = true;

  transaction<T>(work: (transaction: PoolClient) => Promise<T>): Promise<T> {
    // Once a pooler was noticed, which session a connection reaches is unknown: every transaction resets.
    return this.#transaction(work, (client) =>
      this.#sessionBudget && (!this.#preparing || this.#sessionValues.has(client))
        ? { begin: `BEGIN; ${SESSION_DEFAULTS_SQL}`, sessionValue: null }
        : { begin: 'BEGIN' },
    );
  }

  /**
   * Sends `BEGIN` and the budget settings as one simple query (the value as a literal: the simple
   * query protocol has no parameters), so the budget costs no round trip. With `sessionBudget` a
   * connection that already keeps this budget only gets `BEGIN`.
   */
  transactionWithBudget<T>(
    work: (transaction: PoolClient) => Promise<T>,
    timeoutMs: number,
  ): Promise<T> {
    return this.#transaction(work, this.#budgetStart(timeoutMs, 'BEGIN'));
  }

  /**
   * Like {@link PgExecutor.transactionWithBudget} with `BEGIN ISOLATION LEVEL ...`: the level is set
   * with the `BEGIN` of the same simple query, before the budget's first statement.
   */
  transactionWithIsolation<T>(
    work: (transaction: PoolClient) => Promise<T>,
    { isolationLevel, timeoutMs }: PostgresIsolatedTransactionOptions,
  ): Promise<T> {
    return this.#transaction(
      work,
      this.#budgetStart(timeoutMs, `BEGIN ISOLATION LEVEL ${isolationKeywords(isolationLevel)}`),
    );
  }

  /** How a transaction starts on a connection when it is held to `timeoutMs`; `begin` is the `BEGIN` statement. */
  #budgetStart(timeoutMs: number, begin: string): (client: PoolClient) => TransactionStart {
    const value = budgetValue(timeoutMs);
    const settings = (scope: 'transaction' | 'session'): string =>
      budgetSettingsSql({ value: `'${value}'`, sessionTimeouts: true, scope });
    return (client) => {
      if (!this.#keepsSessionBudget()) {
        return { begin: `${begin}; ${settings('transaction')}` };
      }
      return this.#sessionValues.get(client) === value
        ? { begin }
        : { begin: `${begin}; ${settings('session')}`, sessionValue: value };
    };
  }

  #recordSession(client: PoolClient, sessionValue: string | null | undefined): void {
    if (sessionValue === null) {
      this.#sessionValues.delete(client);
    } else if (sessionValue !== undefined) {
      this.#sessionValues.set(client, sessionValue);
    }
  }

  /** A pooler that hands out other server sessions (noticed by prepared statements) shares session settings. */
  #keepsSessionBudget(): boolean {
    return this.#sessionBudget && this.#preparing;
  }

  async #transaction<T>(
    work: (transaction: PoolClient) => Promise<T>,
    start: (client: PoolClient) => TransactionStart,
  ): Promise<T> {
    const client = await this.#pool.connect();
    let released = false;
    const release = (destroy: boolean): void => {
      if (released) {
        return;
      }
      released = true;
      client.removeListener('error', onSessionEnded);
      if (destroy) {
        client.release(true);
      } else {
        client.release();
      }
    };
    // The pool only listens while a client is idle: an error event of a checked-out client (the
    // server ending the session, for example at idle_in_transaction_session_timeout) would be
    // unhandled. The connection is dead, so it goes back to the pool at once, destroyed; the
    // transaction fails at its next statement or its commit.
    const onSessionEnded = (error: Error): void => {
      this.#logger.warn(
        `[PgExecutor](transaction) The server ended the session of an open transaction, the transaction fails | error=${error.message}`,
      );
      release(true);
    };
    client.on('error', onSessionEnded);
    const { begin, sessionValue } = start(client);
    try {
      await client.query(begin);
      const result = await work(client);
      const { command } = await client.query('COMMIT');
      // Settings changed in a transaction that rolls back (a failed one answers COMMIT with ROLLBACK)
      // return to what the session had before.
      if (command === 'COMMIT') {
        this.#recordSession(client, sessionValue);
      }
      release(false);
      return result;
    } catch (error) {
      await this.#rollbackAndRelease(client, release);
      throw error;
    }
  }

  // eslint-disable-next-line relaybus/max-two-params -- PostgresExecutor port signature
  async query(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: PoolClient,
    options?: PostgresQueryOptions,
  ): Promise<readonly PostgresRow[]> {
    const queryable = transaction ?? this.#pool;
    const values = [...parameters];
    if (options?.prepare !== true || !this.#preparing) {
      return (await queryable.query<PostgresRow>(sql, values)).rows;
    }
    try {
      return (await queryable.query<PostgresRow>({ name: this.#nameOf(sql), text: sql, values }))
        .rows;
    } catch (error) {
      if (!FOREIGN_SESSION_ERRORS.has((error as { code?: unknown }).code as string)) {
        throw error;
      }
      if (this.#preparing) {
        this.#preparing = false;
        this.#logger.warn(
          `[PgExecutor](query) The server does not keep prepared statements per connection, statements are sent unprepared from now on; set preparedStatements: false to skip this check | code=${String((error as { code?: unknown }).code)}`,
        );
      }
      if (transaction !== undefined) {
        throw error;
      }
      return (await queryable.query<PostgresRow>(sql, values)).rows;
    }
  }

  /** `relaybus_` and 24 hex digits of the SHA-256 of the text: well under the 63 bytes of an identifier. */
  #nameOf(sql: string): string {
    let name = this.#names.get(sql);
    if (name === undefined) {
      name = `relaybus_${createHash('sha256').update(sql).digest('hex').slice(0, STATEMENT_NAME_DIGITS)}`;
      this.#names.set(sql, name);
    }
    return name;
  }

  /** A client whose rollback fails is in an unknown state, so it is destroyed instead of reused. */
  async #rollbackAndRelease(
    client: PoolClient,
    release: (destroy: boolean) => void,
  ): Promise<void> {
    try {
      await client.query('ROLLBACK');
      release(false);
    } catch {
      release(true);
    }
  }
}
