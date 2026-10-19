import { mongodb, mongoPkg, pg, postgresPkg } from './deps.ts';
import { cloneDatabase, dropDatabase, MongoMonitor, startMongo, type MongoHandle } from './mongo-backend.ts';
import type { ValidationData } from './validate.ts';
import { PgMonitor, prewarm, startPostgres, vacuumAnalyze, type PgHandle } from './pg-backend.ts';

export type Counts = Record<'pending' | 'processing' | 'completed' | 'dead', number>;

export interface Backend {
  readonly name: 'pg' | 'mongo';
  readonly containerId: string;
  describe(): Promise<Record<string, unknown>>;
  createFresh(database: string): Promise<void>;
  clone(template: string, database: string): Promise<void>;
  drop(database: string): Promise<void>;
  connection(database: string): { url: string; database: string };
  monitor(database: string): { begin(): Promise<void>; end(deep?: boolean): Promise<any> };
  afterLoad(database: string): Promise<void>;
  counts(database: string): Promise<Counts>;
  /** Ground truth for exactness validation: every committed event and every delivery that is not completed. */
  validationData(database: string): Promise<ValidationData>;
  rtt(): Promise<{ p50: number; p99: number }>;
  stop(): Promise<void>;
}

const quantiles = (samples: number[]): { p50: number; p99: number } => {
  samples.sort((a, b) => a - b);
  return { p50: samples[Math.floor(samples.length * 0.5)]!, p99: samples[Math.floor(samples.length * 0.99)]! };
};

export async function createPgBackend(): Promise<Backend> {
  const handle: PgHandle = await startPostgres();
  const withPool = async <T>(database: string, work: (pool: any) => Promise<T>): Promise<T> => {
    const pool = new pg.Pool({ connectionString: handle.urlFor(database), max: 2 });
    pool.on('error', () => undefined);
    try {
      return await work(pool);
    } finally {
      await pool.end();
    }
  };
  return {
    name: 'pg',
    containerId: handle.containerId,
    async describe() {
      const version = (await handle.admin.query('SHOW server_version')).rows[0].server_version;
      const settings = (
        await handle.admin.query(
          `SELECT name, setting, unit FROM pg_settings WHERE name IN
           ('shared_buffers','max_wal_size','synchronous_commit','fsync','wal_level','max_connections','autovacuum','wal_compression','full_page_writes','checkpoint_timeout')`,
        )
      ).rows;
      return { server: `PostgreSQL ${version}`, image: process.env['BENCH_PG_IMAGE'] ?? 'postgres:17-alpine', settings };
    },
    async createFresh(database) {
      await handle.createDatabase(database);
      await withPool(database, (pool) => new postgresPkg.PostgresEventStore({ pool }).setup());
    },
    async clone(template, database) {
      await handle.checkpoint();
      await handle.createDatabase(database, template);
      await prewarm(handle, database);
    },
    drop: (database) => handle.dropDatabase(database),
    connection: (database) => ({ url: handle.urlFor(database), database }),
    monitor: (database) => new PgMonitor(handle, database),
    async afterLoad(database) {
      await vacuumAnalyze(handle, database);
      await handle.checkpoint();
    },
    async counts(database) {
      const rows = await withPool(database, (pool) =>
        pool.query('SELECT status, count(*)::int AS n FROM relaybus_deliveries GROUP BY status'),
      );
      const counts: Counts = { pending: 0, processing: 0, completed: 0, dead: 0 };
      for (const row of rows.rows) counts[row.status as keyof Counts] = row.n;
      return counts;
    },
    async validationData(database) {
      return withPool(database, async (pool) => {
        const events = await pool.query(
          `SELECT (payload->>'w')::int AS w, (payload->>'n')::int AS n, key, sequence::int AS sequence FROM relaybus_events`,
        );
        const open = await pool.query(
          `SELECT (e.payload->>'w')::int AS w, (e.payload->>'n')::int AS n, d.handler, d.status
           FROM relaybus_deliveries d JOIN relaybus_events e ON e.id = d.event_id WHERE d.status <> 'completed'`,
        );
        return { events: events.rows, open: open.rows };
      });
    },
    async rtt() {
      const samples: number[] = [];
      for (let i = 0; i < 300; i += 1) {
        const t = performance.now();
        await handle.admin.query('SELECT 1');
        samples.push(performance.now() - t);
      }
      return quantiles(samples.slice(20));
    },
    stop: () => handle.stop(),
  };
}

export async function createMongoBackend(): Promise<Backend> {
  const handle: MongoHandle = await startMongo();
  return {
    name: 'mongo',
    containerId: handle.containerId,
    async describe() {
      const info = await handle.client.db('admin').command({ buildInfo: 1 });
      const cfg = await handle.client.db('admin').command({ getCmdLineOpts: 1 });
      return { server: `MongoDB ${info.version}`, image: process.env['BENCH_MONGO_IMAGE'] ?? 'mongo:7', topology: 'single-node replica set', args: cfg.argv };
    },
    async createFresh(database) {
      await new mongoPkg.MongoEventStore({ client: handle.client, database }).setup();
    },
    async clone(template, database) {
      await cloneDatabase(handle, template, database);
      await new mongoPkg.MongoEventStore({ client: handle.client, database }).setup();
    },
    drop: (database) => dropDatabase(handle, database),
    connection: (database) => ({ url: handle.uri, database }),
    monitor: (database) => new MongoMonitor(handle, database),
    async afterLoad() {
      // Nothing to vacuum; give the checkpoint a moment so load writes do not bleed into the run.
      await handle.client.db('admin').command({ fsync: 1 }).catch(() => undefined);
    },
    async counts(database) {
      const rows = await handle.client
        .db(database)
        .collection('relaybus_deliveries')
        .aggregate([{ $group: { _id: '$status', n: { $sum: 1 } } }])
        .toArray();
      const counts: Counts = { pending: 0, processing: 0, completed: 0, dead: 0 };
      for (const row of rows) counts[row._id as keyof Counts] = row.n;
      return counts;
    },
    async validationData(database) {
      const db = handle.client.db(database);
      const events = await db
        .collection('relaybus_events')
        .find({}, { projection: { 'payload.w': 1, 'payload.n': 1, key: 1, sequence: 1 } })
        .toArray();
      const byId = new Map<string, any>(events.map((event: any) => [event._id, event]));
      const open = await db
        .collection('relaybus_deliveries')
        .find({ status: { $ne: 'completed' } }, { projection: { eventId: 1, handler: 1, status: 1 } })
        .toArray();
      return {
        events: events.map((event: any) => ({ w: event.payload.w, n: event.payload.n, key: event.key ?? null, sequence: event.sequence ?? null })),
        open: open.map((delivery: any) => {
          const event = byId.get(delivery.eventId);
          return { w: event?.payload.w ?? -1, n: event?.payload.n ?? -1, handler: delivery.handler, status: delivery.status };
        }),
      };
    },
    async rtt() {
      const samples: number[] = [];
      for (let i = 0; i < 300; i += 1) {
        const t = performance.now();
        await handle.client.db('admin').command({ ping: 1 });
        samples.push(performance.now() - t);
      }
      return quantiles(samples.slice(20));
    },
    stop: () => handle.stop(),
  };
}

void mongodb;
