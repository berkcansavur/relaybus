import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';

const SUPPORTED_IMAGES = ['postgres:17-alpine', 'postgres:13-alpine'];

/** Images the contract runs against. `RELAYBUS_POSTGRES_IMAGES` (comma separated) overrides them. */
export const POSTGRES_IMAGES: readonly string[] =
  process.env['RELAYBUS_POSTGRES_IMAGES']?.split(',') ?? SUPPORTED_IMAGES;

export interface TestDatabase {
  readonly connectionString: string;
  readonly container: StartedPostgreSqlContainer;
  /** Pool shared by the helpers of one test file. */
  readonly pool: Pool;
  stop(): Promise<void>;
}

export async function startDatabase(image: string = SUPPORTED_IMAGES[0]!): Promise<TestDatabase> {
  const container = await new PostgreSqlContainer(image).start();
  const connectionString = container.getConnectionUri();
  const pool = createTestPool(connectionString, 20);
  return {
    connectionString,
    container,
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
