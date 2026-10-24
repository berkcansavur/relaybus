import type * as Pg from 'pg';
import type { Pool, Client } from 'pg';
import { latencyQuantiles, type Backend, type RunMonitor } from './backends.ts';
import type { DbWindow } from './db-metrics.ts';
import { loadContainers, type SideModules } from './deps.ts';
import { DockerSampler } from './docker-stats.ts';
import type { ValidationData } from './validate.ts';
import { harnessRoot } from './deps.ts';

export const PG_IMAGE = process.env['BENCH_PG_IMAGE'] ?? 'postgres:17-alpine';

/** PostgreSQL settings used for every run. Part of the baseline fingerprint. */
export const PG_SETTINGS: Readonly<Record<string, string>> = {
  shared_buffers: '1GB',
  effective_cache_size: '3GB',
  max_connections: '600',
  max_wal_size: '4GB',
  min_wal_size: '1GB',
  checkpoint_timeout: '10min',
  random_page_cost: '1.1',
  synchronous_commit: process.env['BENCH_SYNC_COMMIT'] ?? 'on',
  shared_preload_libraries: 'pg_stat_statements',
  'pg_stat_statements.max': '10000',
  'pg_stat_statements.track': 'all',
  compute_query_id: 'on',
  track_io_timing: 'on',
  track_wal_io_timing: 'on',
};

const STATS_FLUSH_POLL_MS = 100;
const STATS_FLUSH_MAX_ATTEMPTS = 30;
const WAIT_SAMPLE_INTERVAL_MS = 100;

interface PgServer {
  readonly containerId: string;
  readonly driver: typeof Pg;
  readonly admin: Pool;
  urlFor(database: string): string;
  stop(): Promise<void>;
}

async function startServer(driver: typeof Pg): Promise<PgServer> {
  const { PostgreSqlContainer } = loadContainers(harnessRoot);
  const command = ['postgres'];
  for (const [key, value] of Object.entries(PG_SETTINGS)) command.push('-c', `${key}=${value}`);
  const container = await new PostgreSqlContainer(PG_IMAGE)
    .withDatabase('bench')
    .withUsername('bench')
    .withPassword('bench')
    .withCommand(command)
    .withSharedMemorySize(1024 * 1024 * 1024)
    .start();
  const host = container.getHost();
  const port = container.getPort();
  const urlFor = (database: string): string => `postgres://bench:bench@${host}:${port}/${database}`;
  const admin = new driver.Pool({ connectionString: urlFor('bench'), max: 4 });
  admin.on('error', () => undefined);
  await admin.query('CREATE EXTENSION IF NOT EXISTS pg_stat_statements');
  await admin.query('CREATE EXTENSION IF NOT EXISTS pg_prewarm');
  return {
    containerId: container.getId(),
    driver,
    admin,
    urlFor,
    async stop() {
      await admin.end().catch(() => undefined);
      await container.stop({ timeout: 10 });
    },
  };
}

async function withClient<T>(
  server: PgServer,
  database: string,
  work: (client: Client) => Promise<T>,
): Promise<T> {
  const client = new server.driver.Client({ connectionString: server.urlFor(database) });
  client.on('error', () => undefined);
  await client.connect();
  try {
    return await work(client);
  } finally {
    await client.end();
  }
}

async function withPool<T>(
  server: PgServer,
  database: string,
  work: (pool: Pool) => Promise<T>,
): Promise<T> {
  const pool = new server.driver.Pool({ connectionString: server.urlFor(database), max: 2 });
  pool.on('error', () => undefined);
  try {
    return await work(pool);
  } finally {
    await pool.end();
  }
}

interface RowCounts {
  readonly events: number;
  readonly deliveries: number;
  readonly open: number;
}

interface TableRow {
  table: string;
  ins: string;
  upd: string;
  hot_upd: string;
  del: string;
}

/** Database-side counters of one window: statements, WAL, tuple traffic per table, plus wait events. */
class PgMonitor implements RunMonitor {
  readonly #server: PgServer;
  readonly #database: string;
  readonly #sampler: DockerSampler;
  #client: Client | undefined;
  #waitClient: Client | undefined;
  #waitTimer: NodeJS.Timeout | undefined;
  #waitBusy = false;
  #clientPid = 0;
  #lsn = '';
  #walRecords = 0;
  #walFpi = 0;
  #before: RowCounts = { events: 0, deliveries: 0, open: 0 };
  readonly #waits = new Map<string, number>();
  #waitSamples = 0;

  constructor(server: PgServer, database: string) {
    this.#server = server;
    this.#database = database;
    this.#sampler = new DockerSampler(server.containerId);
  }

  /** Exact row counts. `open` are the deliveries that are not completed (completed rows may be deleted by retention). */
  async #rowCounts(client: Client): Promise<RowCounts> {
    const row = (
      await client.query<{ events: string; deliveries: string; open: string }>(
        `SELECT (SELECT count(*) FROM relaybus_events) AS events,
                (SELECT count(*) FROM relaybus_deliveries) AS deliveries,
                (SELECT count(*) FROM relaybus_deliveries WHERE status <> 'completed') AS open`,
      )
    ).rows[0];
    return {
      events: Number(row?.events ?? 0),
      deliveries: Number(row?.deliveries ?? 0),
      open: Number(row?.open ?? 0),
    };
  }

  async #walCounters(): Promise<{ records: number; fpi: number }> {
    const row = (
      await this.#server.admin.query<{ wal_records: string; wal_fpi: string }>(
        'SELECT wal_records, wal_fpi FROM pg_stat_wal',
      )
    ).rows[0];
    return { records: Number(row?.wal_records ?? 0), fpi: Number(row?.wal_fpi ?? 0) };
  }

  async begin(): Promise<void> {
    const client = new this.#server.driver.Client({
      connectionString: this.#server.urlFor(this.#database),
    });
    client.on('error', () => undefined);
    await client.connect();
    this.#client = client;
    this.#clientPid = Number(
      (await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0]?.pid ?? 0,
    );
    this.#before = await this.#rowCounts(client);
    await this.#server.admin.query(
      'SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = $1), 0)',
      [this.#database],
    );
    await client.query('SELECT pg_stat_reset()');
    this.#lsn =
      (await this.#server.admin.query<{ lsn: string }>('SELECT pg_current_wal_lsn()::text AS lsn'))
        .rows[0]?.lsn ?? '';
    const wal = await this.#walCounters();
    this.#walRecords = wal.records;
    this.#walFpi = wal.fpi;
    this.#sampler.start();
    // The wait sampler connects to the admin database, so its queries are not counted as statements of the measured database.
    const waitClient = new this.#server.driver.Client({
      connectionString: this.#server.urlFor('bench'),
    });
    waitClient.on('error', () => undefined);
    await waitClient.connect();
    this.#waitClient = waitClient;
    this.#waitTimer = setInterval(
      () => void this.#sampleWaits(waitClient),
      WAIT_SAMPLE_INTERVAL_MS,
    );
  }

  /** Backends flush their statistics when they exit; the workers closed their pools, so this normally takes milliseconds. */
  async #waitForBackendsToExit(): Promise<void> {
    for (let attempt = 0; attempt < STATS_FLUSH_MAX_ATTEMPTS; attempt += 1) {
      const row = (
        await this.#server.admin.query<{ n: string }>(
          `SELECT count(*) AS n FROM pg_stat_activity
           WHERE datname = $1 AND backend_type = 'client backend' AND pid <> $2`,
          [this.#database, this.#clientPid],
        )
      ).rows[0];
      if (Number(row?.n ?? 0) === 0) break;
      await new Promise((resolve) => setTimeout(resolve, STATS_FLUSH_POLL_MS));
    }
    await new Promise((resolve) => setTimeout(resolve, STATS_FLUSH_POLL_MS));
  }

  async #sampleWaits(client: Client): Promise<void> {
    if (this.#waitBusy) return;
    this.#waitBusy = true;
    try {
      const rows = (
        await client.query<{ event: string }>(
          `SELECT coalesce(wait_event_type || ':' || wait_event, 'CPU/running') AS event
           FROM pg_stat_activity WHERE datname = $1 AND state = 'active' AND pid <> pg_backend_pid() AND backend_type = 'client backend'`,
          [this.#database],
        )
      ).rows;
      this.#waitSamples += rows.length;
      for (const row of rows) this.#waits.set(row.event, (this.#waits.get(row.event) ?? 0) + 1);
    } catch {
      // sampling is best effort
    } finally {
      this.#waitBusy = false;
    }
  }

  async end(): Promise<DbWindow> {
    clearInterval(this.#waitTimer);
    await this.#waitClient?.end().catch(() => undefined);
    const docker = await this.#sampler.stop();
    await this.#waitForBackendsToExit();
    const client = this.#client;
    if (client === undefined) throw new Error('[PgMonitor](end) begin() was not called');
    const lsn =
      (await this.#server.admin.query<{ lsn: string }>('SELECT pg_current_wal_lsn()::text AS lsn'))
        .rows[0]?.lsn ?? '';
    const walBytes = Number(
      (
        await this.#server.admin.query<{ d: string }>(
          'SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint AS d',
          [lsn, this.#lsn],
        )
      ).rows[0]?.d ?? 0,
    );
    const wal = await this.#walCounters();
    const statements = (
      await this.#server.admin.query<{ calls: string }>(
        `SELECT calls FROM pg_stat_statements
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = $1) AND query NOT LIKE '%pg_stat_reset%'`,
        [this.#database],
      )
    ).rows.reduce((total, row) => total + Number(row.calls), 0);
    const tables = (
      await client.query<TableRow>(
        `SELECT relname AS table, n_tup_ins AS ins, n_tup_upd AS upd, n_tup_hot_upd AS hot_upd, n_tup_del AS del
         FROM pg_stat_user_tables ORDER BY relname`,
      )
    ).rows;
    const after = await this.#rowCounts(client);
    await client.end();
    const counters: Record<string, number> = {
      statements,
      walBytes,
      walRecords: wal.records - this.#walRecords,
      walFpi: wal.fpi - this.#walFpi,
    };
    let deliveriesHotUpdates: number | null = null;
    for (const row of tables) {
      const name = row.table.replace(/^relaybus_/, '');
      counters[`${name}.ins`] = Number(row.ins);
      counters[`${name}.upd`] = Number(row.upd);
      counters[`${name}.del`] = Number(row.del);
      if (name === 'deliveries') deliveriesHotUpdates = Number(row.hot_upd);
    }
    const waits = [...this.#waits]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(
        ([event, count]) =>
          `${event} ${Math.round((1000 * count) / Math.max(1, this.#waitSamples)) / 10}%`,
      )
      .join(', ');
    return {
      eventsInserted: after.events - this.#before.events,
      deliveriesInserted: after.deliveries - this.#before.deliveries,
      deliveriesCompleted: this.#before.open - after.open,
      counters,
      deliveriesHotUpdates,
      dockerCpuAvgPct: docker.cpuAvgPct,
      waits,
    };
  }
}

interface EventFactRow {
  w: number;
  n: number;
  key: string | null;
  sequence: number | null;
}

interface OpenRow {
  w: number;
  n: number;
  handler: string;
  status: string;
}

/** Starts the PostgreSQL container and returns the backend. `driver` is the `pg` module used for administration. */
export async function createPgBackend(driver: typeof Pg): Promise<Backend> {
  const server = await startServer(driver);
  const { admin } = server;
  return {
    name: 'pg',
    containerId: server.containerId,
    async describe() {
      const version = (await admin.query<{ server_version: string }>('SHOW server_version')).rows[0]
        ?.server_version;
      const settings = (
        await admin.query<{ name: string; setting: string; unit: string | null }>(
          `SELECT name, setting, unit FROM pg_settings WHERE name IN
           ('shared_buffers','max_wal_size','synchronous_commit','fsync','wal_level','max_connections','autovacuum','wal_compression','full_page_writes','checkpoint_timeout')`,
        )
      ).rows;
      return { server: `PostgreSQL ${version ?? 'unknown'}`, image: PG_IMAGE, settings };
    },
    async createFresh(side: SideModules, database) {
      await admin.query(`CREATE DATABASE ${database}`);
      await withPool(server, database, (pool) =>
        new side.postgres.PostgresEventStore({ pool }).setup(),
      );
    },
    async clone(_side, template, database) {
      await admin.query('CHECKPOINT');
      await admin.query(`CREATE DATABASE ${database} TEMPLATE ${template} STRATEGY FILE_COPY`);
      // Warm shared_buffers with every relation of the copy, so a cloned run starts like a warm one.
      await withClient(server, database, async (client) => {
        await client.query('CREATE EXTENSION IF NOT EXISTS pg_prewarm');
        await client.query(
          `SELECT pg_prewarm(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'public' AND c.relkind IN ('r','i')`,
        );
      });
    },
    async drop(database) {
      await admin.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    },
    connection: (database) => ({ url: server.urlFor(database), database }),
    monitor: (database) => new PgMonitor(server, database),
    async afterLoad(database) {
      await withClient(server, database, async (client) => {
        await client.query('VACUUM (ANALYZE)');
      });
      await admin.query('CHECKPOINT');
    },
    async countByStatus(database) {
      const rows = await withPool(server, database, (pool) =>
        pool.query<{ status: string; n: number }>(
          'SELECT status, count(*)::int AS n FROM relaybus_deliveries GROUP BY status',
        ),
      );
      return Object.fromEntries(rows.rows.map((row) => [row.status, row.n]));
    },
    async validationData(database): Promise<ValidationData> {
      return withPool(server, database, async (pool) => {
        const events = await pool.query<EventFactRow>(
          `SELECT (payload->>'w')::int AS w, (payload->>'n')::int AS n, key, sequence::int AS sequence FROM relaybus_events`,
        );
        const open = await pool.query<OpenRow>(
          `SELECT (e.payload->>'w')::int AS w, (e.payload->>'n')::int AS n, d.handler, d.status
           FROM relaybus_deliveries d JOIN relaybus_events e ON e.id = d.event_id WHERE d.status <> 'completed'`,
        );
        return { events: events.rows, open: open.rows };
      });
    },
    async roundTrip() {
      const samples: number[] = [];
      for (let index = 0; index < 300; index += 1) {
        const started = performance.now();
        await admin.query('SELECT 1');
        samples.push(performance.now() - started);
      }
      return latencyQuantiles(samples.slice(20));
    },
    stop: () => server.stop(),
  };
}
