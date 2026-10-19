import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';

/** Images the contract runs against. `RELAYBUS_POSTGRES_IMAGES` (comma separated) overrides them. */
export const SUPPORTED_POSTGRES_IMAGES = ['postgres:17-alpine', 'postgres:13-alpine'];

export const POSTGRES_IMAGES: readonly string[] =
  process.env['RELAYBUS_POSTGRES_IMAGES']?.split(',') ?? SUPPORTED_POSTGRES_IMAGES;

/**
 * Settings that trade crash safety for speed. No test asserts durability, WAL or crash
 * recovery, so the test servers run without them. `RELAYBUS_TEST_DURABLE_POSTGRES=1`
 * starts the servers with the image defaults instead.
 */
const NON_DURABLE_SETTINGS = ['fsync=off', 'synchronous_commit=off', 'full_page_writes=off'];

export interface PostgresServer {
  readonly image: string;
  /** Superuser connection URI of the server's default database. */
  readonly connectionUri: string;
  stop(): Promise<void>;
}

/**
 * Starts one Postgres server for a whole test run: data directory on tmpfs, non-durable
 * settings, and enough connections for every test file to hold its own pools in parallel.
 */
export async function startPostgresServer(image: string): Promise<PostgresServer> {
  const durable = process.env['RELAYBUS_TEST_DURABLE_POSTGRES'] === '1';
  const settings = durable ? [] : NON_DURABLE_SETTINGS;
  let container = new PostgreSqlContainer(image).withCommand([
    'postgres',
    '-c',
    'max_connections=500',
    ...settings.flatMap((setting) => ['-c', setting]),
  ]);
  if (!durable) {
    container = container.withTmpFs({ '/var/lib/postgresql/data': 'rw' });
  }
  const started: StartedPostgreSqlContainer = await container.start();
  return {
    image,
    connectionUri: started.getConnectionUri(),
    stop: async () => {
      await started.stop();
    },
  };
}
