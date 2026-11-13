import { adapterExecutor } from '../adapter-executor.js';
import { is, sql as drizzleSql, type SQL } from 'drizzle-orm';
import { NodePgSession, type NodePgSessionOptions } from 'drizzle-orm/node-postgres';
import { PgDatabase } from 'drizzle-orm/pg-core';
import pg, { type Pool, type PoolClient } from 'pg';
import type { PostgresExecutor } from '../executor.js';
import { splitPositionalSql } from '../positional-sql.js';

/** What a Drizzle database and a Drizzle transaction have in common. */
export interface DrizzleQueryable {
  execute(query: SQL): PromiseLike<{ readonly rows: Record<string, unknown>[] }>;
}

/** The part of a Drizzle `node-postgres` database the executor uses. */
export interface DrizzleLike extends DrizzleQueryable {
  transaction<T>(work: (transaction: DrizzleQueryable) => Promise<T>): Promise<T>;
}

type SessionArguments = ConstructorParameters<typeof NodePgSession>;

type PoolSession = {
  readonly client: Pool;
  readonly dialect: SessionArguments[1];
  readonly schema: SessionArguments[2];
  readonly options: NodePgSessionOptions;
};

function namedPool(name: unknown): boolean {
  return typeof name === 'string' && name.includes('Pool');
}

function findPoolSession(drizzleLike: DrizzleLike): PoolSession | undefined {
  const { session } = drizzleLike as { session?: { transaction?: unknown } };
  if (
    drizzleLike.transaction !== PgDatabase.prototype.transaction ||
    session?.transaction !== NodePgSession.prototype.transaction ||
    !is(session, NodePgSession)
  ) {
    return undefined;
  }
  const poolSession = session as unknown as PoolSession;
  const client = poolSession.client as {
    readonly constructor?: { readonly name?: unknown };
    readonly Client?: { readonly name?: unknown };
  };
  const pool = client instanceof pg.Pool || namedPool(client.constructor?.name);
  return pool && !namedPool(client.Client?.name) ? poolSession : undefined;
}

type Lease = {
  readonly release: (destroy: boolean) => void;
  readonly released: () => boolean;
};

function leaseOf(poolClient: PoolClient): Lease {
  let released = false;
  const release = (destroy: boolean): void => {
    if (released) {
      return;
    }
    released = true;
    poolClient.removeListener('error', onSessionEnded);
    poolClient.release(destroy);
  };
  const onSessionEnded = (): void => release(true);
  poolClient.on('error', onSessionEnded);
  return { release, released: () => released };
}

async function transactionOnPool<T>(
  poolSession: PoolSession,
  work: (drizzleQueryable: DrizzleQueryable) => Promise<T>,
): Promise<T> {
  const poolClient: PoolClient = await poolSession.client.connect();
  const lease = leaseOf(poolClient);
  let workFailed = false;
  let workError: unknown;
  try {
    const result = await new NodePgSession(
      poolClient,
      poolSession.dialect,
      poolSession.schema,
      poolSession.options,
    ).transaction(async (drizzleQueryable) => {
      try {
        return await work(drizzleQueryable);
      } catch (error) {
        workFailed = true;
        workError = error;
        throw error;
      }
    });
    lease.release(false);
    return result;
  } catch (error) {
    if (workFailed && error === workError) {
      lease.release(false);
    } else if (!lease.released()) {
      await rollbackAndRelease(poolClient, lease.release);
    }
    throw error;
  }
}

async function rollbackAndRelease(
  poolClient: PoolClient,
  release: (destroy: boolean) => void,
): Promise<void> {
  try {
    await poolClient.query('ROLLBACK');
    release(false);
  } catch {
    release(true);
  }
}

/**
 * Executor for Drizzle with the `node-postgres` driver. The transaction handle
 * is a Drizzle transaction: pass `tx` from `db.transaction(async (tx) => ...)`.
 *
 * On a `pg` pool the executor checks the connection of a transaction out itself and runs Drizzle's
 * transaction on it, so the connection always goes back to the pool and the server may end the
 * session of an overrun (`sessionTimeouts`). Which way the executor goes is decided when it is created.
 */
export function createDrizzleExecutor(database: DrizzleLike): PostgresExecutor<DrizzleQueryable> {
  const session = findPoolSession(database);
  const executor = adapterExecutor<DrizzleQueryable>({
    root: database,
    transaction: (work) =>
      session === undefined ? database.transaction(work) : transactionOnPool(session, work),
    async run(queryable, { sql, parameters }) {
      const chunks = splitPositionalSql(sql).map((part) =>
        typeof part === 'string'
          ? drizzleSql.raw(part)
          : drizzleSql.param(parameters[part.parameterIndex]),
      );
      const result = await queryable.execute(drizzleSql.join(chunks));
      return result.rows;
    },
  });
  return session === undefined ? executor : { ...executor, sessionTimeouts: true };
}
