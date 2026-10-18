import type { PostgresExecutor, PostgresParameter, PostgresRow } from '../executor.js';
import { splitPositionalSql } from '../positional-sql.js';

/** What a Knex instance and a Knex transaction have in common. */
export interface KnexQueryable {
  raw(sql: string, bindings: readonly unknown[]): PromiseLike<{ readonly rows: PostgresRow[] }>;
}

/** The part of a `Knex` instance the executor uses. */
export interface KnexLike extends KnexQueryable {
  transaction<T>(work: (transaction: KnexQueryable) => Promise<T>): Promise<T>;
}

/**
 * Executor for Knex with the `pg` client. The transaction handle is a Knex
 * transaction: pass `trx` from `knex.transaction(async (trx) => ...)`.
 */
export function createKnexExecutor(knex: KnexLike): PostgresExecutor<KnexQueryable> {
  return {
    transaction: (work) => knex.transaction(work),
    async query(sql, parameters: readonly PostgresParameter[], transaction) {
      const parts = splitPositionalSql(sql);
      const knexSql = parts.map((part) => (typeof part === 'string' ? part : '?')).join('');
      const bindings = parts.flatMap((part) =>
        typeof part === 'string' ? [] : [parameters[part.parameterIndex]],
      );
      const result = await (transaction ?? knex).raw(knexSql, bindings);
      return result.rows;
    },
  };
}
