import type { Pool, PoolClient } from 'pg';
import type { PostgresExecutor, PostgresParameter, PostgresRow } from './executor.js';

/**
 * Executor for a `pg` {@link Pool}. A transaction is a dedicated pool client
 * between `BEGIN` and `COMMIT`; it is handed to callers as the transaction
 * handle, so they can run their own queries on it.
 */
export class PgExecutor implements PostgresExecutor<PoolClient> {
  readonly #pool: Pool;

  constructor(pool: Pool) {
    this.#pool = pool;
  }

  async transaction<T>(work: (transaction: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.#pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      client.release();
      return result;
    } catch (error) {
      await this.#rollbackAndRelease(client);
      throw error;
    }
  }

  async query(
    sql: string,
    parameters: readonly PostgresParameter[],
    transaction?: PoolClient,
  ): Promise<readonly PostgresRow[]> {
    const result = await (transaction ?? this.#pool).query<PostgresRow>(sql, [...parameters]);
    return result.rows;
  }

  /** A client whose rollback fails is in an unknown state, so it is destroyed instead of reused. */
  async #rollbackAndRelease(client: PoolClient): Promise<void> {
    try {
      await client.query('ROLLBACK');
      client.release();
    } catch {
      client.release(true);
    }
  }
}
