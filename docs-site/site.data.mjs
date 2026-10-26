// Single source of truth for the documentation structure. Used by the VitePress
// config (navigation) and by scripts/build-docs-assets.mjs (llms.txt, llms-full.txt,
// package pages and the Markdown copies served next to every page).

export const siteUrl = 'https://berkcansavur.github.io/relaybus/';
export const repoUrl = 'https://github.com/berkcansavur/relaybus';

export const siteTitle = 'relaybus';
export const siteDescription =
  'relaybus is a reliable, storage-agnostic event bus for NestJS and Node.js: a transactional outbox with per-handler retries, leases, dead letters and ordering keys for PostgreSQL and MongoDB, with Kafka and RabbitMQ transports.';

export const packages = [
  { name: 'core', blurb: 'The bus, dispatcher, retry policies and the in-memory store.' },
  {
    name: 'nestjs',
    blurb: 'RelayBusModule, the @RelayBusConsumer decorator, lifecycle and health.',
  },
  {
    name: 'postgres',
    blurb: 'PostgreSQL store for pg, Prisma, TypeORM, Drizzle, Knex and Kysely.',
  },
  {
    name: 'mongodb',
    blurb: 'MongoDB store on multi-document transactions, with Mongoose sessions.',
  },
  { name: 'kafka', blurb: 'Kafka transport with CloudEvents binary mode and an idempotent inbox.' },
  {
    name: 'rabbitmq',
    blurb: 'RabbitMQ transport with publisher confirms, quorum queues and dead letters.',
  },
  { name: 'testing', blurb: 'The store contract suite, FakeClock and an in-memory transport.' },
  {
    name: 'eslint-plugin',
    blurb: 'ESLint rules: no external calls inside a transaction, no publish outside one.',
  },
  {
    name: 'otel',
    blurb: 'OpenTelemetry metrics, backlog gauges and traces across services.',
  },
];

/** Ordered sections. `path` is the page path without extension, relative to the site root. */
export const sections = [
  {
    title: 'Start here',
    items: [
      {
        title: 'Getting started',
        path: 'getting-started',
        description:
          'Add a reliable transactional outbox event bus to a NestJS and PostgreSQL service in five minutes.',
      },
      {
        title: 'Guarantees',
        path: 'guarantees',
        description:
          'Exactly what relaybus guarantees (atomic publish, at-least-once delivery, per-key ordering, fencing) and what it does not.',
      },
    ],
  },
  {
    title: 'Concepts',
    items: [
      {
        title: 'Transactional outbox',
        path: 'concepts/outbox',
        description:
          'How the transactional outbox pattern avoids dual writes, and how relaybus implements it with deliveries per handler.',
      },
      {
        title: 'Deliveries',
        path: 'concepts/deliveries',
        description:
          'A delivery is one event for one handler or route, with its own status, attempts and retry schedule.',
      },
      {
        title: 'Leases and fencing',
        path: 'concepts/leases',
        description:
          'How competing workers claim deliveries with leases and why a stalled worker cannot overwrite the worker that took over.',
      },
      {
        title: 'Retries and backoff',
        path: 'concepts/retries',
        description:
          'Capped exponential backoff with full jitter, timeouts and non-retryable errors in relaybus.',
      },
      {
        title: 'Ordering keys',
        path: 'concepts/ordering-keys',
        description:
          'Per-key ordering in commit order: how the sequence is assigned and what it does and does not promise.',
      },
      {
        title: 'Compile-time safety with event catalogs',
        path: 'concepts/event-catalogs',
        description:
          'Group events in a catalog so the TypeScript compiler rejects a missing handler, a wrong payload or an event published to the wrong catalog, and a boot check catches the rest.',
      },
      {
        title: 'Dead letters',
        path: 'concepts/dead-letters',
        description:
          'What happens to events that exhaust their retries, and how to inspect and requeue them.',
      },
      {
        title: 'Observability',
        path: 'concepts/observability',
        description:
          'OpenTelemetry metrics and traces for relaybus: the metrics, one trace across services, a collector and Prometheus setup, and alerts on backlog age and dead letters.',
      },
      {
        title: 'Error codes',
        path: 'concepts/error-codes',
        description:
          'The stable, machine-readable code of every relaybus error, stored with dead letters and reported to observers.',
      },
    ],
  },
  {
    title: 'Packages',
    items: [
      {
        title: 'All packages',
        path: 'packages/',
        description:
          'The nine @relaybus packages: core, nestjs, postgres, mongodb, kafka, rabbitmq, testing, eslint-plugin and otel.',
      },
      ...packages.map((p) => ({
        title: `@relaybus/${p.name}`,
        path: `packages/${p.name}`,
        generated: true,
      })),
    ],
  },
  {
    title: 'Recipes',
    items: [
      {
        title: 'Transactions in NestJS',
        path: 'recipes/transactions-in-nestjs',
        description:
          'Run NestJS service methods in an outbox transaction with @RelayBusTransaction(), share it with repositories, hold it to a time budget and lint external calls out of it.',
      },
      {
        title: 'Prisma',
        path: 'recipes/prisma',
        description:
          'Publish transactional outbox events from a Prisma interactive transaction with @relaybus/postgres.',
      },
      {
        title: 'TypeORM',
        path: 'recipes/typeorm',
        description:
          'Publish events in the same TypeORM transaction as your entities using the TypeORM executor.',
      },
      {
        title: 'Mongoose',
        path: 'recipes/mongoose',
        description:
          'Use the MongoDB transactional outbox with Mongoose sessions and replica sets.',
      },
      {
        title: 'Kafka between services',
        path: 'recipes/kafka-between-services',
        description:
          'Send events from one service to another through Kafka without losing events between the database and the broker.',
      },
      {
        title: 'RabbitMQ',
        path: 'recipes/rabbitmq',
        description:
          'Publish outbox events to RabbitMQ with publisher confirms and consume them with an idempotent inbox.',
      },
      {
        title: 'Interoperating with non-relaybus services',
        path: 'recipes/interoperating-with-other-services',
        description:
          'Exchange events with services that do not use relaybus over Kafka and RabbitMQ: the wire format, and decoding plain JSON and structured CloudEvents with exactly-once handling.',
      },
      {
        title: 'Testing with FakeClock',
        path: 'recipes/testing-with-fakeclock',
        description:
          'Test retries, delays and dead letters deterministically with FakeClock and the in-memory store.',
      },
    ],
  },
  {
    title: 'Reference',
    items: [
      {
        title: 'relaybus vs alternatives',
        path: 'comparison',
        description:
          'How relaybus compares with @nestjs/event-emitter, @nestjs/cqrs, @nestjs/outbox, BullMQ, pg-boss and using Kafka directly.',
      },
      {
        title: 'Tuning for your workload',
        path: 'tuning',
        description:
          'Recommended relaybus settings for a single-connection API, a high-traffic API, a worker fleet, PgBouncer in transaction mode and a MongoDB replica set.',
      },
      {
        title: 'Upgrading',
        path: 'upgrading',
        description:
          'Step-by-step upgrades: 0.2 to 0.3: route id change and draining the old route backlog, the 5 second transaction budget, the StopResult of stop(), decorator and codec renames, error codes and the optional new packages.',
      },
      {
        title: 'Stability',
        path: 'stability',
        description:
          'The relaybus API stability policy: semver for 0.x and 1.x, what is public API, deprecations, wire-format and database schema stability, supported runtimes.',
      },
      {
        title: 'Compatibility',
        path: 'compatibility',
        description:
          'The tested matrix of relaybus (Node.js, CommonJS and ESM, NestJS, PostgreSQL clients and ORMs, MongoDB, Kafka, RabbitMQ), what is unsupported and notes for serverless.',
      },
      {
        title: 'Performance: 0.2.0 vs 0.1.0',
        path: 'performance',
        description:
          'Measured throughput, latency and write amplification of relaybus 0.2.0 against 0.1.0 on PostgreSQL and MongoDB, with methodology, machine and honest sample counts.',
      },
      {
        title: 'Write amplification in the delivery path',
        path: 'performance/write-amplification',
        description:
          'Why relaybus 0.1.0 rewrote delivery rows, how lane leases and delete-on-complete remove it, which guarantees are preserved, and what it measured.',
      },
      {
        title: 'The publish path and prepared statements',
        path: 'performance/publish-path',
        description:
          'Per-event round trips, WAL bytes and plan times of the 0.2.0 publish and claim statements against 0.1.0, and the prepared-statement fix.',
      },
      {
        title: 'FAQ',
        path: 'faq',
        description:
          'Answers to common questions: publishing events reliably in NestJS, the transactional outbox pattern and avoiding lost events between a database and Kafka.',
      },
      {
        title: 'Releases',
        path: 'releases',
        description:
          'What is new in relaybus 0.4.0 and 0.3.0, breaking changes, deprecations, known issues and benchmarks, and how relaybus is versioned and released.',
      },
    ],
  },
];

export function pageUrl(path) {
  if (path === '' || path === 'index') return siteUrl;
  if (path.endsWith('/')) return `${siteUrl}${path}`;
  return `${siteUrl}${path}`;
}
