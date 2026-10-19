import { loadContainers, mongodb } from './deps.ts';
import { DockerSampler } from './docker-stats.ts';

const IMAGE = process.env['BENCH_MONGO_IMAGE'] ?? 'mongo:7';

export interface MongoHandle {
  readonly containerId: string;
  readonly uri: string;
  readonly client: any;
  stop(): Promise<void>;
}

export async function startMongo(): Promise<MongoHandle> {
  const { MongoDBContainer } = loadContainers();
  const container = await new MongoDBContainer(IMAGE).start();
  const uri = container.getConnectionString();
  const client = await new mongodb.MongoClient(uri, { directConnection: true, maxPoolSize: 10 }).connect();
  return {
    containerId: container.getId(),
    uri,
    client,
    async stop() {
      await client.close().catch(() => undefined);
      await container.stop({ timeout: 10 });
    },
  };
}

const COLLECTIONS = ['relaybus_events', 'relaybus_deliveries', 'relaybus_key_sequences'];

/** Copies the three relaybus collections of a template database into a fresh one (indexes are rebuilt by store.setup()). */
export async function cloneDatabase(handle: MongoHandle, from: string, to: string): Promise<void> {
  const source = handle.client.db(from);
  for (const name of COLLECTIONS) {
    await source.collection(name).aggregate([{ $match: {} }, { $out: { db: to, coll: name } }]).toArray();
  }
}

export async function dropDatabase(handle: MongoHandle, name: string): Promise<void> {
  await handle.client.db(name).dropDatabase();
}

async function serverStatus(client: any): Promise<any> {
  return client.db('admin').command({ serverStatus: 1 });
}

async function oplogStats(client: any): Promise<{ bytes: number; entries: number }> {
  const stats = await client.db('local').command({ collStats: 'oplog.rs' });
  return { bytes: Number(stats.size), entries: Number(stats.count) };
}

export interface MongoRunMetrics {
  opcounters: Record<string, number>;
  txnCommitted: number;
  txnAborted: number;
  writeConflicts: number;
  journalBytes: number;
  oplogBytes: number;
  oplogEntries: number;
  cacheDirtyMiB: number;
  dbStats: { dataMiB: number; indexMiB: number; storageMiB: number };
  collections: Array<{ name: string; count: number; sizeMiB: number; indexMiB: number }>;
  docker: { cpuAvgPct: number; cpuMaxPct: number; memMaxMiB: number; samples: number };
}

export class MongoMonitor {
  readonly #handle: MongoHandle;
  readonly #database: string;
  readonly #sampler: DockerSampler;
  #before: any;
  #oplogBefore = { bytes: 0, entries: 0 };

  constructor(handle: MongoHandle, database: string) {
    this.#handle = handle;
    this.#database = database;
    this.#sampler = new DockerSampler(handle.containerId);
  }

  async begin(): Promise<void> {
    this.#before = await serverStatus(this.#handle.client);
    this.#oplogBefore = await oplogStats(this.#handle.client);
    this.#sampler.start();
  }

  async end(): Promise<MongoRunMetrics> {
    const docker = await this.#sampler.stop();
    const after = await serverStatus(this.#handle.client);
    const oplogAfter = await oplogStats(this.#handle.client);
    const diff = (get: (s: any) => number): number => get(after) - get(this.#before);
    const opcounters: Record<string, number> = {};
    for (const key of ['insert', 'query', 'update', 'delete', 'getmore', 'command']) {
      opcounters[key] = diff((s) => Number(s.opcounters[key]));
    }
    const db = this.#handle.client.db(this.#database);
    const stats = await db.command({ dbStats: 1, scale: 1048576 });
    const collections = [];
    for (const name of COLLECTIONS) {
      try {
        const s = await db.command({ collStats: name, scale: 1048576 });
        collections.push({ name, count: Number(s.count), sizeMiB: Number(s.size), indexMiB: Number(s.totalIndexSize) });
      } catch {
        // collection missing
      }
    }
    return {
      opcounters,
      txnCommitted: diff((s) => Number(s.transactions?.totalCommitted ?? 0)),
      txnAborted: diff((s) => Number(s.transactions?.totalAborted ?? 0)),
      writeConflicts: diff((s) => Number(s.metrics?.operation?.writeConflicts ?? 0)),
      journalBytes: diff((s) => Number(s.wiredTiger?.log?.['log bytes written'] ?? 0)),
      oplogBytes: oplogAfter.bytes - this.#oplogBefore.bytes,
      oplogEntries: oplogAfter.entries - this.#oplogBefore.entries,
      cacheDirtyMiB: Number(after.wiredTiger?.cache?.['tracked dirty bytes in the cache'] ?? 0) / 1048576,
      dbStats: { dataMiB: Number(stats.dataSize), indexMiB: Number(stats.indexSize), storageMiB: Number(stats.storageSize) },
      collections,
      docker,
    };
  }
}
