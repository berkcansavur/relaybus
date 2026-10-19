import type { Collection, Db, Document, MongoClient } from 'mongodb';
import { latencyQuantiles, type Backend, type RunMonitor } from './backends.ts';
import type { DbWindow } from './db-metrics.ts';
import { harnessRoot, loadContainers, type SideModules } from './deps.ts';
import { DockerSampler } from './docker-stats.ts';
import type { ValidationData } from './validate.ts';

export const MONGO_IMAGE = process.env['BENCH_MONGO_IMAGE'] ?? 'mongo:7';

const COLLECTIONS = ['relaybus_events', 'relaybus_deliveries', 'relaybus_key_sequences'];

interface MongoServer {
  readonly containerId: string;
  readonly uri: string;
  readonly client: MongoClient;
  stop(): Promise<void>;
}

async function startServer(side: SideModules): Promise<MongoServer> {
  const { MongoDBContainer } = loadContainers(harnessRoot);
  const container = await new MongoDBContainer(MONGO_IMAGE).start();
  const uri = container.getConnectionString();
  const client = await new side.mongoDriver.MongoClient(uri, {
    directConnection: true,
    maxPoolSize: 10,
  }).connect();
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

interface ServerStatus {
  opcounters: Record<string, number>;
  transactions?: { totalCommitted?: number };
  wiredTiger?: { log?: Record<string, number> };
}

const serverStatus = async (client: MongoClient): Promise<ServerStatus> =>
  (await client.db('admin').command({ serverStatus: 1 })) as ServerStatus;

interface Totals {
  readonly oplogEntries: number;
  readonly oplogBytes: number;
  readonly events: number;
  readonly deliveries: number;
  /** Deliveries that are not completed (completed rows may be deleted by retention). */
  readonly open: number;
}

/** Counts that must be read before `serverStatus` at the start of a window and after it at the end, so they are not counted themselves. */
async function totals(client: MongoClient, database: string): Promise<Totals> {
  const oplog = (await client.db('local').command({ collStats: 'oplog.rs' })) as {
    size: number;
    count: number;
  };
  const db: Db = client.db(database);
  const events: Collection<Document> = db.collection('relaybus_events');
  const deliveries: Collection<Document> = db.collection('relaybus_deliveries');
  return {
    oplogEntries: Number(oplog.count),
    oplogBytes: Number(oplog.size),
    events: await events.estimatedDocumentCount(),
    deliveries: await deliveries.estimatedDocumentCount(),
    open: await deliveries.countDocuments({ status: { $ne: 'completed' } }),
  };
}

class MongoMonitor implements RunMonitor {
  readonly #server: MongoServer;
  readonly #database: string;
  readonly #sampler: DockerSampler;
  #before: ServerStatus | undefined;
  #totalsBefore: Totals | undefined;

  constructor(server: MongoServer, database: string) {
    this.#server = server;
    this.#database = database;
    this.#sampler = new DockerSampler(server.containerId);
  }

  async begin(): Promise<void> {
    this.#totalsBefore = await totals(this.#server.client, this.#database);
    this.#before = await serverStatus(this.#server.client);
    this.#sampler.start();
  }

  async end(): Promise<DbWindow> {
    const docker = await this.#sampler.stop();
    const after = await serverStatus(this.#server.client);
    const totalsAfter = await totals(this.#server.client, this.#database);
    const before = this.#before;
    const totalsBefore = this.#totalsBefore;
    if (before === undefined || totalsBefore === undefined)
      throw new Error('[MongoMonitor](end) begin() was not called');
    const ops = (name: string): number =>
      Number(after.opcounters[name] ?? 0) - Number(before.opcounters[name] ?? 0);
    const journal = (status: ServerStatus): number =>
      Number(status.wiredTiger?.log?.['log bytes written'] ?? 0);
    const counters: Record<string, number> = {
      insert: ops('insert'),
      update: ops('update'),
      delete: ops('delete'),
      ops: ops('insert') + ops('update') + ops('delete') + ops('query') + ops('getmore'),
      commands: ops('command'),
      oplogEntries: totalsAfter.oplogEntries - totalsBefore.oplogEntries,
      oplogBytes: totalsAfter.oplogBytes - totalsBefore.oplogBytes,
      journalBytes: journal(after) - journal(before),
    };
    return {
      eventsInserted: totalsAfter.events - totalsBefore.events,
      deliveriesInserted: totalsAfter.deliveries - totalsBefore.deliveries,
      deliveriesCompleted: totalsBefore.open - totalsAfter.open,
      counters,
      deliveriesHotUpdates: null,
      dockerCpuAvgPct: docker.cpuAvgPct,
      waits: '',
    };
  }
}

interface EventDocument {
  _id: string;
  key?: string | null;
  sequence?: number | null;
  payload: { w: number; n: number };
}

interface DeliveryDocument {
  eventId: string;
  handler: string;
  status: string;
}

/** Starts the MongoDB container (single-node replica set) and returns the backend. */
export async function createMongoBackend(side: SideModules): Promise<Backend> {
  const server = await startServer(side);
  const { client } = server;
  return {
    name: 'mongo',
    containerId: server.containerId,
    async describe() {
      const info = (await client.db('admin').command({ buildInfo: 1 })) as { version: string };
      const options = (await client.db('admin').command({ getCmdLineOpts: 1 })) as {
        argv: string[];
      };
      return {
        server: `MongoDB ${info.version}`,
        image: MONGO_IMAGE,
        topology: 'single-node replica set',
        args: options.argv,
      };
    },
    async createFresh(sideModules, database) {
      await new sideModules.mongoPackage.MongoEventStore({ client, database }).setup();
    },
    async clone(sideModules, template, database) {
      const source = client.db(template);
      for (const name of COLLECTIONS) {
        await source
          .collection(name)
          .aggregate([{ $match: {} }, { $out: { db: database, coll: name } }])
          .toArray();
      }
      await new sideModules.mongoPackage.MongoEventStore({ client, database }).setup();
    },
    async drop(database) {
      await client.db(database).dropDatabase();
    },
    connection: (database) => ({ url: server.uri, database }),
    monitor: (database) => new MongoMonitor(server, database),
    async afterLoad() {
      // Nothing to vacuum; flush so load writes do not bleed into the first run.
      await client
        .db('admin')
        .command({ fsync: 1 })
        .catch(() => undefined);
    },
    async countByStatus(database) {
      const rows = await client
        .db(database)
        .collection('relaybus_deliveries')
        .aggregate<{ _id: string; n: number }>([{ $group: { _id: '$status', n: { $sum: 1 } } }])
        .toArray();
      return Object.fromEntries(rows.map((row) => [row._id, row.n]));
    },
    async validationData(database): Promise<ValidationData> {
      const db = client.db(database);
      const events = await db
        .collection<EventDocument>('relaybus_events')
        .find({}, { projection: { 'payload.w': 1, 'payload.n': 1, key: 1, sequence: 1 } })
        .toArray();
      const byId = new Map(events.map((event) => [event._id, event]));
      const open = await db
        .collection<DeliveryDocument>('relaybus_deliveries')
        .find(
          { status: { $ne: 'completed' } },
          { projection: { eventId: 1, handler: 1, status: 1 } },
        )
        .toArray();
      return {
        events: events.map((event) => ({
          w: event.payload.w,
          n: event.payload.n,
          key: event.key ?? null,
          sequence: event.sequence ?? null,
        })),
        open: open.map((delivery) => {
          const event = byId.get(delivery.eventId);
          return {
            w: event?.payload.w ?? -1,
            n: event?.payload.n ?? -1,
            handler: delivery.handler,
            status: delivery.status,
          };
        }),
      };
    },
    async roundTrip() {
      const samples: number[] = [];
      for (let index = 0; index < 300; index += 1) {
        const started = performance.now();
        await client.db('admin').command({ ping: 1 });
        samples.push(performance.now() - started);
      }
      return latencyQuantiles(samples.slice(20));
    },
    stop: () => server.stop(),
  };
}
