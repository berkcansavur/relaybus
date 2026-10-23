import type { TestProject } from 'vitest/node';
import {
  SUPPORTED_POSTGRES_IMAGES,
  POSTGRES_IMAGES,
  startPostgresServer,
  type PostgresServer,
} from './packages/postgres/test/support/postgres-server.js';
import { startMongoServer, type MongoServer } from './packages/mongodb/test/mongo-server.js';
import {
  startRabbitMqServer,
  type RabbitMqServer,
} from './packages/rabbitmq/test/support/broker-server.js';
import { startKafkaServer, type KafkaServer } from './packages/kafka/test/support/kafka-server.js';

type Backend = 'postgres' | 'mongodb' | 'rabbitmq' | 'kafka';
const ALL_BACKENDS: readonly Backend[] = ['postgres', 'mongodb', 'rabbitmq', 'kafka'];

interface Servers {
  postgres: PostgresServer[];
  mongodb?: MongoServer;
  rabbitmq?: RabbitMqServer;
  kafka?: KafkaServer;
}

/**
 * The integration projects share one server per backend for the whole run. Vitest calls
 * this setup once per project, so the servers live in a process-wide slot: the first call
 * starts them, the last teardown stops them.
 */
interface Slot {
  users: number;
  started: Promise<Servers>;
}
const SLOT_KEY = Symbol.for('relaybus.test.servers');
const slots = globalThis as unknown as Record<symbol, Slot | undefined>;

/**
 * Which backends the run needs. `RELAYBUS_TEST_BACKENDS` (comma separated, or `all`)
 * wins. Otherwise path filters on the command line such as `packages/postgres` narrow it,
 * and anything else starts every backend.
 */
function selectBackends(): readonly Backend[] {
  const requested = process.env['RELAYBUS_TEST_BACKENDS'];
  if (requested !== undefined && requested !== 'all') {
    return ALL_BACKENDS.filter((backend) => requested.split(',').includes(backend));
  }
  if (requested === 'all') {
    return ALL_BACKENDS;
  }
  const filters = process.argv.slice(2).filter((argument) => !argument.startsWith('-'));
  const matched = ALL_BACKENDS.filter((backend) =>
    filters.some((filter) => filter.includes(backend)),
  );
  return matched.length === 0 ? ALL_BACKENDS : matched;
}

async function startServers(backends: readonly Backend[]): Promise<Servers> {
  const servers: Servers = { postgres: [] };
  const wanted = (backend: Backend): boolean => backends.includes(backend);
  const images = [...new Set([SUPPORTED_POSTGRES_IMAGES[0]!, ...POSTGRES_IMAGES])];
  const results = await Promise.allSettled([
    wanted('postgres')
      ? Promise.all(images.map((image) => startPostgresServer(image))).then((started) => {
          servers.postgres = started;
        })
      : undefined,
    wanted('mongodb')
      ? startMongoServer().then((server) => {
          servers.mongodb = server;
        })
      : undefined,
    wanted('rabbitmq')
      ? startRabbitMqServer().then((server) => {
          servers.rabbitmq = server;
        })
      : undefined,
    wanted('kafka')
      ? startKafkaServer().then((server) => {
          servers.kafka = server;
        })
      : undefined,
  ]);
  const failure = results.find((result) => result.status === 'rejected');
  if (failure !== undefined) {
    await stopServers(servers);
    throw failure.reason;
  }
  return servers;
}

async function stopServers(servers: Servers): Promise<void> {
  await Promise.allSettled([
    ...servers.postgres.map((server) => server.stop()),
    servers.mongodb?.stop(),
    servers.rabbitmq?.stop(),
    servers.kafka?.stop(),
  ]);
}

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
  const slot = (slots[SLOT_KEY] ??= { users: 0, started: startServers(selectBackends()) });
  slot.users += 1;
  const servers = await slot.started;

  project.provide(
    'postgresServers',
    Object.fromEntries(servers.postgres.map((server) => [server.image, server.connectionUri])),
  );
  project.provide('mongoConnectionString', servers.mongodb?.connectionString ?? '');
  project.provide('rabbitmqBroker', {
    amqpUrl: servers.rabbitmq?.amqpUrl ?? '',
    amqpPort: servers.rabbitmq?.amqpPort ?? 0,
    containerId: servers.rabbitmq?.containerId ?? '',
  });
  project.provide('kafkaBrokers', servers.kafka?.brokers ?? []);

  return async () => {
    slot.users -= 1;
    if (slot.users === 0) {
      slots[SLOT_KEY] = undefined;
      await stopServers(servers);
    }
  };
}
