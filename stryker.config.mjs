// @ts-check
import { readFileSync } from 'node:fs';
import { URL } from 'node:url';

/**
 * The Stryker configuration: stryker.config.json, with `mutate` narrowed to one shard when
 * RELAYBUS_MUTATION_SHARD names one (mutation.yml runs one job per shard). The globs live here
 * because the `--mutate` option splits its value on every comma, also inside `{a,b}`.
 */
const base = JSON.parse(readFileSync(new URL('./stryker.config.json', import.meta.url), 'utf8'));

/** Postgres files of the shards that name their files; `postgres-store` takes the rest. */
const postgresShards = {
  'postgres-claim': [
    'postgres-claim',
    'statements-claim',
    'statements-lanes',
    'statements-candidates',
    'claim-rows',
    'statement-fragments',
  ],
  'postgres-write': [
    'postgres-append',
    'statements-append',
    'postgres-completion',
    'statements-completion',
    'notification-listener',
    'sql-fragments',
  ],
  'postgres-executors': [
    'pg-executor',
    'root-pool-executor',
    'adapter-executor',
    'owned-connections',
    'executor',
    'transaction-isolation',
    'transaction-timeouts',
    'serialization-failure',
    'positional-sql',
    '*/*-executor',
  ],
};
const postgresFiles = (names) => names.map((name) => `packages/postgres/src/${name}.ts`);

/** Core files of the dispatching shard; `core-internal` takes `internal/`, `core-bus` the rest. */
const coreDispatch = [
  'dispatch/**/*',
  'in-memory/**/*',
  'store-kit/**/*',
  'transport-kit/**/*',
  'dispatcher',
  'dispatch-observation',
  'delivery-timers',
  'lease-heartbeat',
  'completion-batcher',
  'retry-policy',
  'notifier',
  'in-memory-event-store',
  'in-memory-transaction',
  'execution-scope',
  'handler-registry',
  'shutdown-deadline',
  'isolated-transaction',
  'transaction-budget',
  'clock',
  'duration',
].map((name) => `packages/core/src/${name}.ts`);
const coreExcluded = ['!packages/core/src/**/index.ts', '!packages/core/src/standard-schema.ts'];

/** @type {Record<string, string[]>} */
const shards = {
  'core-dispatch': [...coreDispatch, ...coreExcluded],
  'core-internal': ['packages/core/src/internal/**/*.ts', ...coreExcluded],
  'core-bus': [
    'packages/core/src/**/*.ts',
    ...coreExcluded,
    '!packages/core/src/internal/**/*.ts',
    ...coreDispatch.map((file) => `!${file}`),
  ],
  ...Object.fromEntries(
    Object.entries(postgresShards).map(([shard, names]) => [shard, postgresFiles(names)]),
  ),
  'postgres-store': [
    'packages/postgres/src/**/*.ts',
    '!packages/postgres/src/**/index.ts',
    ...postgresFiles(Object.values(postgresShards).flat()).map((file) => `!${file}`),
  ],
  kafka: ['packages/kafka/src/**/*.ts', '!packages/kafka/src/**/index.ts'],
  rabbitmq: ['packages/rabbitmq/src/**/*.ts', '!packages/rabbitmq/src/**/index.ts'],
  testing: [
    'packages/testing/src/**/*.ts',
    '!packages/testing/src/**/index.ts',
    '!packages/testing/src/event-store-contract.ts',
  ],
  packages: [
    ...['nestjs', 'otel', 'eslint-plugin'].map((name) => `packages/${name}/src/**/*.ts`),
    '!packages/*/src/**/index.ts',
  ],
};

const shard = process.env['RELAYBUS_MUTATION_SHARD'];
if (shard !== undefined && shard !== '' && shards[shard] === undefined) {
  throw new Error(
    `Unknown RELAYBUS_MUTATION_SHARD ${shard}: one of ${Object.keys(shards).join(', ')}`,
  );
}

export default shard === undefined || shard === '' ? base : { ...base, mutate: shards[shard] };
