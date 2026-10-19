import { loadContainers, pg } from './deps.ts';
import { DockerSampler } from './docker-stats.ts';

const IMAGE = process.env['BENCH_PG_IMAGE'] ?? 'postgres:17-alpine';

/** PostgreSQL settings used for every run. Printed in env.json and the README. */
export const PG_SETTINGS: Record<string, string> = {
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

export interface PgHandle {
  readonly containerId: string;
  readonly host: string;
  readonly port: number;
  readonly admin: any;
  urlFor(database: string): string;
  createDatabase(name: string, template?: string): Promise<void>;
  dropDatabase(name: string): Promise<void>;
  checkpoint(): Promise<void>;
  stop(): Promise<void>;
}

export async function startPostgres(): Promise<PgHandle> {
  const { PostgreSqlContainer } = loadContainers();
  const command = ['postgres'];
  for (const [key, value] of Object.entries(PG_SETTINGS)) command.push('-c', `${key}=${value}`);
  const container = await new PostgreSqlContainer(IMAGE)
    .withDatabase('bench')
    .withUsername('bench')
    .withPassword('bench')
    .withCommand(command)
    .withSharedMemorySize(1024 * 1024 * 1024)
    .start();
  const host = container.getHost();
  const port = container.getPort();
  const urlFor = (database: string): string => `postgres://bench:bench@${host}:${port}/${database}`;
  const admin = new pg.Pool({ connectionString: urlFor('bench'), max: 4 });
  admin.on('error', () => undefined);
  await admin.query('CREATE EXTENSION IF NOT EXISTS pg_stat_statements');
  await admin.query('CREATE EXTENSION IF NOT EXISTS pgstattuple');
  await admin.query('CREATE EXTENSION IF NOT EXISTS pg_prewarm');
  return {
    containerId: container.getId(),
    host,
    port,
    admin,
    urlFor,
    async createDatabase(name, template) {
      await admin.query(
        template === undefined
          ? `CREATE DATABASE ${name}`
          : `CREATE DATABASE ${name} TEMPLATE ${template} STRATEGY FILE_COPY`,
      );
    },
    async dropDatabase(name) {
      await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    },
    async checkpoint() {
      await admin.query('CHECKPOINT');
    },
    async stop() {
      await admin.end().catch(() => undefined);
      await container.stop({ timeout: 10 });
    },
  };
}

/** Warms shared_buffers with every relation of a cloned database, so a cloned run starts like a warm one. */
export async function prewarm(handle: PgHandle, database: string): Promise<void> {
  const client = new pg.Client({ connectionString: handle.urlFor(database) });
  await client.connect();
  try {
    await client.query('CREATE EXTENSION IF NOT EXISTS pg_prewarm');
    await client.query(
      `SELECT pg_prewarm(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'public' AND c.relkind IN ('r','i')`,
    );
  } finally {
    await client.end();
  }
}

export async function vacuumAnalyze(handle: PgHandle, database: string): Promise<void> {
  const client = new pg.Client({ connectionString: handle.urlFor(database) });
  await client.connect();
  try {
    await client.query('VACUUM (ANALYZE)');
  } finally {
    await client.end();
  }
}

// pg_stat_statements replaces string constants with $n, so the patterns avoid literals.
const KIND_PATTERNS: Array<[string, RegExp]> = [
  // Baseline (0.1.0) statements.
  ['claim (fast)', /WITH due_window/],
  ['claim (slow, blocked keys)', /WITH heads AS/],
  ['complete', /SET status = \$\d+, completed_at/],
  ['extendLease', /SET lease_expires_at = \$\d+::timestamptz, updated_at/],
  ['reschedule', /SET status = \$\d+, available_at/],
  ['release', /attempts = attempts - /],
  ['bury', /last_error = \$\d+::jsonb, updated_at/],
  // Lane-lease statements. The slow claim is the only one with the `heads` CTE, so it comes first.
  ['claim (slow, blocked keys)', /heads AS MATERIALIZED/],
  ['claim (fast)', /due_window AS MATERIALIZED/],
  ['claim: take lanes', /AS free\s+CROSS JOIN LATERAL/],
  ['claim: lock held lanes', /lease_owner = \$\d+::text AND lease_expires_at > /],
  ['claim: record batch', /still_running/],
  ['complete', /jsonb_to_recordset\(\$1::jsonb\) AS lease/],
  ['extendLease', /GREATEST\(lane\.lease_expires_at/],
  ['reschedule', /available_at = \$\d+::timestamptz, last_error/],
  ['bury', /SET status = \$\d+, last_error/],
  ['release', /SET available_at = \$\d+::timestamptz, updated_at/],
  ['append: reserveSequences', /key_sequences/],
  ['append: insertEvents', /INSERT INTO [\w."]*relaybus_events/i],
  ['append: insertDeliveries', /INSERT INTO [\w."]*relaybus_deliveries/i],
  ['BEGIN', /^BEGIN/i],
  ['COMMIT', /^COMMIT/i],
];

export interface PgRunMetrics {
  walBytes: number;
  walRecords: number;
  walFpi: number;
  statements: Array<Record<string, number | string>>;
  tables: Array<Record<string, number | string | null>>;
  indexes: Array<Record<string, number | string | null>>;
  docker: { cpuAvgPct: number; cpuMaxPct: number; memMaxMiB: number; samples: number };
  /** Share of active-backend samples per wait event (pg_stat_activity polled every 100 ms). */
  waits: Array<{ event: string; pct: number }>;
  activeBackendsAvg: number;
}

export class PgMonitor {
  readonly #handle: PgHandle;
  readonly #database: string;
  readonly #sampler: DockerSampler;
  #client: any;
  #lsn = '';
  #wal: any;
  #waitClient: any;
  #waitTimer: NodeJS.Timeout | undefined;
  readonly #waitCounts = new Map<string, number>();
  #waitSamples = 0;
  #activeTotal = 0;
  #waitBusy = false;

  constructor(handle: PgHandle, database: string) {
    this.#handle = handle;
    this.#database = database;
    this.#sampler = new DockerSampler(handle.containerId);
  }

  async begin(): Promise<void> {
    this.#client = new pg.Client({ connectionString: this.#handle.urlFor(this.#database) });
    this.#client.on('error', () => undefined);
    await this.#client.connect();
    await this.#handle.admin.query(
      `SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = $1), 0)`,
      [this.#database],
    );
    await this.#client.query('SELECT pg_stat_reset()');
    this.#lsn = (await this.#client.query('SELECT pg_current_wal_lsn()::text AS lsn')).rows[0].lsn;
    this.#wal = (await this.#client.query('SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_wal')).rows[0];
    this.#sampler.start();
    this.#waitClient = new pg.Client({ connectionString: this.#handle.urlFor(this.#database) });
    this.#waitClient.on('error', () => undefined);
    await this.#waitClient.connect();
    this.#waitTimer = setInterval(() => void this.#sampleWaits(), 100);
  }

  async #sampleWaits(): Promise<void> {
    if (this.#waitBusy) return;
    this.#waitBusy = true;
    try {
      const rows = (
        await this.#waitClient.query(
          `SELECT coalesce(wait_event_type || ':' || wait_event, 'CPU/running') AS event
           FROM pg_stat_activity WHERE datname = $1 AND state = 'active' AND pid <> pg_backend_pid() AND backend_type = 'client backend'`,
          [this.#database],
        )
      ).rows;
      this.#waitSamples += 1;
      this.#activeTotal += rows.length;
      for (const row of rows) this.#waitCounts.set(row.event, (this.#waitCounts.get(row.event) ?? 0) + 1);
    } catch {
      // sampling is best effort
    } finally {
      this.#waitBusy = false;
    }
  }

  async end(deep = false): Promise<PgRunMetrics> {
    clearInterval(this.#waitTimer);
    await this.#waitClient?.end().catch(() => undefined);
    const docker = await this.#sampler.stop();
    await new Promise((resolve) => setTimeout(resolve, 1800)); // let backends flush their statistics
    const c = this.#client;
    const lsn = (await c.query('SELECT pg_current_wal_lsn()::text AS lsn')).rows[0].lsn;
    const walBytes = Number(
      (await c.query('SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint AS d', [lsn, this.#lsn])).rows[0].d,
    );
    const wal = (await c.query('SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_wal')).rows[0];
    const raw = await this.#handle.admin.query(
      `SELECT query, calls, total_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
              wal_bytes, wal_records, wal_fpi, shared_blk_read_time AS blk_read_time, shared_blk_write_time AS blk_write_time
       FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = $1)`,
      [this.#database],
    );
    const byKind = new Map<string, Record<string, number | string>>();
    for (const row of raw.rows) {
      const kind = KIND_PATTERNS.find(([, pattern]) => pattern.test(row.query.trim()))?.[0] ?? 'other';
      const entry =
        byKind.get(kind) ??
        { kind, calls: 0, totalMs: 0, rows: 0, blksHit: 0, blksRead: 0, blksDirtied: 0, walBytes: 0, walRecords: 0, walFpi: 0, readMs: 0, writeMs: 0 };
      entry['calls'] = (entry['calls'] as number) + Number(row.calls);
      entry['totalMs'] = (entry['totalMs'] as number) + Number(row.total_exec_time);
      entry['rows'] = (entry['rows'] as number) + Number(row.rows);
      entry['blksHit'] = (entry['blksHit'] as number) + Number(row.shared_blks_hit);
      entry['blksRead'] = (entry['blksRead'] as number) + Number(row.shared_blks_read);
      entry['blksDirtied'] = (entry['blksDirtied'] as number) + Number(row.shared_blks_dirtied);
      entry['walBytes'] = (entry['walBytes'] as number) + Number(row.wal_bytes);
      entry['walRecords'] = (entry['walRecords'] as number) + Number(row.wal_records);
      entry['walFpi'] = (entry['walFpi'] as number) + Number(row.wal_fpi);
      entry['readMs'] = (entry['readMs'] as number) + Number(row.blk_read_time);
      entry['writeMs'] = (entry['writeMs'] as number) + Number(row.blk_write_time);
      byKind.set(kind, entry);
    }
    const statements = [...byKind.values()]
      .map((e) => ({ ...e, meanMs: (e['totalMs'] as number) / Math.max(1, e['calls'] as number) }))
      .sort((a, b) => (b['totalMs'] as number) - (a['totalMs'] as number));
    const tables = (
      await c.query(
        `SELECT s.relname AS table, s.n_tup_ins AS ins, s.n_tup_upd AS upd, s.n_tup_hot_upd AS hot_upd, s.n_tup_del AS del,
                s.n_live_tup AS live, s.n_dead_tup AS dead, s.autovacuum_count AS autovacuums,
                pg_table_size(s.relid) AS table_bytes, pg_indexes_size(s.relid) AS index_bytes
         FROM pg_stat_user_tables s ORDER BY s.relname`,
      )
    ).rows.map(numberize);
    const indexes = (
      await c.query(
        `SELECT s.indexrelname AS index, s.idx_scan AS scans, pg_relation_size(s.indexrelid) AS bytes
         FROM pg_stat_user_indexes s ORDER BY pg_relation_size(s.indexrelid) DESC`,
      )
    ).rows.map(numberize);
    if (deep) {
      for (const index of indexes) {
        try {
          const row = (await c.query('SELECT avg_leaf_density, leaf_fragmentation FROM pgstatindex($1)', [index['index']])).rows[0];
          index['leafDensity'] = Number(row.avg_leaf_density);
        } catch {
          // partial indexes that are empty or non-btree
        }
      }
    }
    await c.end();
    return {
      walBytes,
      walRecords: Number(wal.wal_records) - Number(this.#wal.wal_records),
      walFpi: Number(wal.wal_fpi) - Number(this.#wal.wal_fpi),
      statements,
      tables,
      indexes,
      docker,
      waits: [...this.#waitCounts]
        .map(([event, count]) => ({ event, pct: Math.round((1000 * count) / Math.max(1, this.#activeTotal)) / 10 }))
        .sort((a, b) => b.pct - a.pct)
        .slice(0, 8),
      activeBackendsAvg: Math.round((10 * this.#activeTotal) / Math.max(1, this.#waitSamples)) / 10,
    };
  }
}

function numberize(row: Record<string, unknown>): Record<string, number | string | null> {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, typeof value === 'string' && /^\d+(\.\d+)?$/.test(value) ? Number(value) : (value as any)]),
  );
}
