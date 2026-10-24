import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { inject } from 'vitest';
import { Pool } from 'pg';
import { POSTGRES_IMAGES, SUPPORTED_POSTGRES_IMAGES } from './postgres-server.js';

export { POSTGRES_IMAGES };

export interface TestDatabase {
  readonly connectionString: string;
  /** Pool shared by the helpers of one test file. */
  readonly pool: Pool;
  stop(): Promise<void>;
}

let databaseCounter = 0;

/**
 * Gives the calling test file its own database on the Postgres server that the global
 * setup started for this run (one server per image, shared by every file). A database
 * per file keeps catalog-wide state such as advisory locks and activity views apart.
 * With `settings` (server settings, `name=value`) the file gets a dedicated container
 * instead, for example to keep WAL and statistics free of other files' noise in the
 * performance design tests.
 */
export async function startDatabase(
  image: string = SUPPORTED_POSTGRES_IMAGES[0]!,
  settings: readonly string[] = [],
): Promise<TestDatabase> {
  if (settings.length > 0) {
    return startDedicatedDatabase(image, settings);
  }
  const serverUri = inject('postgresServers')[image];
  if (serverUri === undefined) {
    throw new Error(
      `No shared Postgres server for ${image}: the global setup started ${Object.keys(inject('postgresServers')).join(', ') || 'none'}`,
    );
  }
  databaseCounter += 1;
  const name = `t${process.pid}_${databaseCounter}`;
  const admin = createTestPool(serverUri, 1);
  try {
    await admin.query(`CREATE DATABASE "${name}"`);
  } finally {
    await admin.end();
  }
  const url = new URL(serverUri);
  url.pathname = `/${name}`;
  const connectionString = url.toString();
  const pool = createTestPool(connectionString, 20);
  return {
    connectionString,
    pool,
    async stop() {
      await pool.end();
    },
  };
}

async function startDedicatedDatabase(
  image: string,
  settings: readonly string[],
): Promise<TestDatabase> {
  const container = await new PostgreSqlContainer(image)
    .withCommand(['postgres', ...settings.flatMap((setting) => ['-c', setting])])
    .start();
  const connectionString = container.getConnectionUri();
  const pool = createTestPool(connectionString, 20);
  return {
    connectionString,
    pool,
    async stop() {
      await pool.end();
      await container.stop();
    },
  };
}

/**
 * A pool whose idle-client errors are swallowed: when a test container stops,
 * Postgres terminates idle connections (57P01) and `pg` would otherwise raise
 * them as unhandled errors. Tests still end their pools before stopping the container.
 */
export function createTestPool(connectionString: string, max: number): Pool {
  const pool = new Pool({ connectionString, max });
  pool.on('error', () => undefined);
  return pool;
}

let schemaCounter = 0;

/** A schema name no other test in this process uses. */
export function uniqueSchema(): string {
  schemaCounter += 1;
  return `t${process.pid}_${schemaCounter}`;
}

export async function dropSchema(pool: Pool, schema: string): Promise<void> {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
}
