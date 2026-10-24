import { createHash } from 'node:crypto';
import type { RelayBusLogger } from '@relaybus/core';
import type { Pool, PoolClient } from 'pg';
import type {
  PostgresExecutor,
  PostgresParameter,
  PostgresQueryOptions,
  PostgresRow,
} from './executor.js';

/**
 * SQLSTATEs of a named statement the server does not know (26000) or already has under that name
 * (42P05). Both mean the connection the statement ran on is not the session that prepared it, which is
 * what a pooler in transaction mode without prepared statement support (PgBouncer before 1.21) does.
 */
const FOREIGN_SESSION_ERRORS = new Set(['26000', '42P05']);

export interface PgExecutorOptions {
  /** Receives the warning sent when prepared statements are switched off. Default `console`. */
  readonly logger?: RelayBusLogger;
}

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

  constructor(pool: Pool, options: PgExecutorOptions = {}) {
    this.#pool = pool;
    this.#logger = options.logger ?? console;
  }

  /** The connection listens for `error` during the transaction, so a server-ended session fails the transaction instead of the process. */
  readonly sessionTimeouts = true;

  async transaction<T>(work: (transaction: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.#pool.connect();
    // The pool only listens while a client is idle: an error event of a checked-out client (the
    // server ending the session, for example at idle_in_transaction_session_timeout) would be unhandled.
    const onSessionEnded = (error: Error): void => {
      this.#logger.warn(
        `[PgExecutor](transaction) The server ended the session of an open transaction, the transaction fails | error=${error.message}`,
      );
    };
    client.on('error', onSessionEnded);
    const release = (destroy: boolean): void => {
      client.removeListener('error', onSessionEnded);
      if (destroy) {
        client.release(true);
      } else {
        client.release();
      }
    };
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
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
