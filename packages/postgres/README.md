# @relaybus/postgres

@relaybus/postgres is the PostgreSQL event store of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it writes the transactional outbox in your own transaction, with `pg`, Prisma, TypeORM, Drizzle, Knex or Kysely.

PostgreSQL event store for [relaybus](https://github.com/berkcansavur/relaybus): a transactional outbox with
`FOR UPDATE SKIP LOCKED` leasing, per-key ordering and fenced state changes. PostgreSQL 13 or newer.
Works with `pg`, Kysely, Knex, TypeORM, Drizzle and Prisma, so events are written in the **same transaction as your
business data**.

## Install

```bash
npm install @relaybus/core @relaybus/postgres pg
npm install -D @types/pg
# plus the library you already use (optional peers): kysely | knex | typeorm | drizzle-orm
```

## Usage

### `pg` (default)

```ts
import { Pool } from 'pg';
import { RelayBus } from '@relaybus/core';
import { PostgresEventStore } from '@relaybus/postgres';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const bus = new RelayBus({ store: new PostgresEventStore({ pool }) });
bus.subscribe(
  OrderPlaced,
  async (event) => {
    /* ... */
  },
  { id: 'send-confirmation' },
);
await bus.start(); // creates tables unless setupStore: false

const client = await pool.connect();
try {
  await client.query('BEGIN');
  await client.query('INSERT INTO orders ...');
  await bus.publish(new OrderPlaced({ orderId }), { transaction: client, key: orderId });
  await client.query('COMMIT');
} catch (error) {
  await client.query('ROLLBACK');
  throw error;
} finally {
  client.release();
}
```

The transaction handle is a `pg` `PoolClient`. If you pass `connectionString` instead of `pool`, the store creates and,
on `close()`, ends its own pool. A pool or executor you pass in is never closed by the store.

### Other libraries

Each adapter lives in its own entry point, so `@relaybus/postgres` itself never imports an ORM.

| Library                   | Import                                                    | Transaction handle you pass to `publish`                        |
| ------------------------- | --------------------------------------------------------- | --------------------------------------------------------------- |
| Kysely                    | `createKyselyExecutor` from `@relaybus/postgres/kysely`   | `trx` of `db.transaction().execute(trx => ...)`                 |
| Knex                      | `createKnexExecutor` from `@relaybus/postgres/knex`       | `trx` of `knex.transaction(trx => ...)`                         |
| TypeORM                   | `createTypeOrmExecutor` from `@relaybus/postgres/typeorm` | the `EntityManager` of `dataSource.transaction(manager => ...)` |
| Drizzle (`node-postgres`) | `createDrizzleExecutor` from `@relaybus/postgres/drizzle` | `tx` of `db.transaction(tx => ...)`                             |
| Prisma                    | `createPrismaExecutor` from `@relaybus/postgres/prisma`   | `tx` of `prisma.$transaction(async tx => ...)`                  |

```ts
import { createKyselyExecutor } from '@relaybus/postgres/kysely';

const store = new PostgresEventStore({ executor: createKyselyExecutor(db) });
const bus = new RelayBus({ store });
await db.transaction().execute(async (trx) => {
  await trx.insertInto('orders').values(order).execute();
  await bus.publish(new OrderPlaced({ orderId: order.id }), { transaction: trx });
});
```

Use `transactionResolver` (for example from `@nestjs-cls/transactional`) to avoid passing the handle explicitly.
For another client, implement `PostgresExecutor` (`transaction` and `query`; about 20 lines).

**Prisma is not integration-tested yet.** The adapter is typed structurally (no Prisma dependency) and unit-tested
against a fake client only. Prisma interactive transactions default to a 5 s timeout, so pass
`createPrismaExecutor(prisma, { timeout, maxWait })` if handlers run longer.

## Options

| Option                                   | Default               | Meaning                                                                     |
| ---------------------------------------- | --------------------- | --------------------------------------------------------------------------- |
| `pool` / `connectionString` / `executor` | required, exactly one | How to reach the database.                                                  |
| `poolConfig`                             | none                  | Extra `pg` pool settings when `connectionString` is used.                   |
| `schema`                                 | `'public'`            | Schema of the tables. Created by `setup()` when it is not `public`.         |
| `tablePrefix`                            | `'relaybus_'`         | Tables are `<prefix>events`, `<prefix>deliveries`, `<prefix>key_sequences`. |
| `purgeBatchSize`                         | `1000`                | Deliveries deleted per transaction by `purge`.                              |
| `logger`                                 | `console`             | Receives idle-connection errors of a pool the store created itself.         |

**Pool errors.** A pool created from `connectionString` gets an `error` listener that logs through `logger`
(`[PostgresEventStore](constructor) Idle connection failed ...`); `pg` then replaces the dead connection, so a failover
or `pg_terminate_backend` of an idle connection does not crash the process. A `pool` you pass in is yours: attach
`pool.on('error', ...)` to it yourself, because without a listener `pg` re-throws the error as an uncaught exception.

`schema` and `tablePrefix` are validated (`[a-z_][a-z0-9_]*`, names at most 63 characters) before they are placed in SQL;
anything else throws `ConfigurationError`.

## Schema and migrations

`setup()` (called by `bus.start()`) creates everything with `IF NOT EXISTS` inside a transaction guarded by
`pg_advisory_xact_lock`, so any number of processes can start at once. The lock key is derived from the schema name, so
stores with different table prefixes in one schema (which race on `CREATE SCHEMA IF NOT EXISTS`) are serialized too. To own the schema yourself, set
`setupStore: false` and put the DDL into your migration tool:

```ts
import { createPostgresSchemaSql, createPostgresSchemaStatements } from '@relaybus/postgres';

const migration = createPostgresSchemaSql({ schema: 'app', tablePrefix: 'bus_' });
```

| Table           | Purpose                                                                                                                                                                                                                                         |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `events`        | One row per event: `id` (**text**, so any caller id works), `type`, `source`, `key`, `sequence`, `payload jsonb`, `headers jsonb`, correlation and causation ids, `occurred_at`, `created_at`.                                                  |
| `deliveries`    | One row per (event, handler): status, attempts, `available_at`, lease owner/token/expiry, `last_error jsonb`, key and sequence copied from the event, timestamps. `unique (event_id, handler)`, foreign key to events with `ON DELETE CASCADE`. |
| `key_sequences` | One counter row per ordering key.                                                                                                                                                                                                               |

Indexes on `deliveries`: a partial index on the due time of pending/processing rows (statistics), the same for unkeyed
rows only (claim), two partial indexes on `(handler, key, ...)` for the ordering check, `(status, created_at, id)` and
`(handler, created_at, id)` for `listDeliveries` filtered by status or handler, and partial indexes for purging
completed and dead rows; on `events`, `(created_at, id)` for the purge sweep. `listDeliveries({ eventType })` has no
index of its own (it filters through a join with `events`); combine it with `status` or `handler`, or add
`CREATE INDEX ON <prefix>events (type)` if you query by type often. `deliveries.id` uses the `C` collation so id tie-breaks are the
same on every database.

## Guarantees

- **Atomic publish.** Events and deliveries are inserted in your transaction; roll back and nothing exists.
- **Per-key order.** On append, a key's counter row in `key_sequences` is upserted
  (`INSERT ... ON CONFLICT (key) DO UPDATE SET sequence = sequence + n`). That row lock is held until the publishing
  transaction ends, so two transactions publishing the same key are serialized and the sequence order equals the commit
  order. Within one `append` (one `publishAll`) keys are locked in sorted order and events and deliveries are inserted in
  primary-key order, so concurrent single calls cannot deadlock each other. Claiming hands out a keyed delivery only when no earlier (same handler and key) delivery
  is pending or processing, so one key is processed one event at a time, in order. A retrying or leased head blocks its
  successors; a dead-lettered head releases them. A `limit` of zero or less claims nothing.
- **Safe parallel claiming.** One statement chooses candidates with `FOR UPDATE SKIP LOCKED`, applies `LIMIT` after the
  ordering filter, and leases them (`processing`, `attempts + 1`, a new `gen_random_uuid()` lease token). SKIP LOCKED
  alone is sufficient for ordering: only the head of a key is ever a candidate, and a racing claimer either skips the
  locked head or, if the other claim already committed, re-checks the row (now processing, not due) and drops it.
  Followers stay blocked because the head remains pending or processing in every snapshot until it is completed or
  buried. The stress test in `test/claim-concurrency.integration.test.ts` runs 12 claimers on 3 pools against 30 keys
  and fails if a key is ever in flight twice or out of order. A keyed delivery is also not claimed while another
  delivery of the same handler and key holds a **live** lease, which covers a requeued or newly attached delivery
  re-entering behind a running one. An expired lease does not block: if the running successor's worker crashed, the
  requeued head is claimed after the lease expires and the successor follows once it completes.
  Claiming first looks at the 256 earliest due deliveries (one cheap statement). Only when those are mostly followers of
  blocked keys and fewer than `limit` were leased does a second statement find the head of every key with
  `DISTINCT ON (handler, key)`, so a deep backlog behind one busy key costs one index-only scan instead of a probe per
  follower.
- **Fenced.** `complete`, `reschedule`, `bury`, `release` and `extendLease` apply only to
  `status = 'processing' AND lease_token = <token>`; a worker that lost its lease changes nothing. `complete` takes the
  handler's transaction, so a transactional handler's writes and its completion commit together.
- **App clock.** Every timestamp is the `now` the caller passes, never the database clock.
- **Driver independent reads.** Timestamps and JSON are read as text and parsed by the store, so custom `pg` type
  parsers (or Drizzle's) cannot change results.

## Operations

- **Isolation.** Claiming runs as autocommit statements and relies on `READ COMMITTED` (the PostgreSQL default). Do
  not change `default_transaction_isolation` for the relaybus pool. Your own publishing transaction may use a stricter
  level, but under `REPEATABLE READ` or `SERIALIZABLE` PostgreSQL can abort it with `40001` (serialization failure);
  retry the whole transaction.
- **Deadlocks.** Publishing a keyed event locks that key's counter row until your transaction ends. Two transactions
  that append keys in opposite order across **several** `append`/`publish` calls (A then B in one, B then A in the
  other) can deadlock; PostgreSQL aborts one with `40P01`. The aborted transaction rolls back completely, nothing is
  lost or half-written, and the caller should retry it. Publishing all keys of a transaction in one `publishAll` call
  avoids the problem because the store locks them in sorted order.
- **Retention.** `purge` deletes completed (`completed_at < cutoff`) and dead (`updated_at < cutoff`) deliveries in
  batches of `purgeBatchSize`, each batch in its own short transaction with `SKIP LOCKED`, then deletes the events left
  without deliveries, including events created before the earliest cutoff you passed that never had one (a publish
  whose delivery was purged in the same instant, or an event with no subscriber). Configure it through the bus `retention` option. Delete rows through `purge` only; events are
  removed by cascade or by purge, never by hand while publishers are active.
- **Lock duration.** A publisher holds its key's counter row until commit. Keep transactions that publish keyed events
  short; unkeyed events take no such lock.
- **Key counters.** `key_sequences` keeps one row per key ever published and rows are never deleted; the table grows
  with the number of distinct keys (a bigint counter each). Pick keys with bounded cardinality, or prune counters of keys
  you know are retired once nothing for them is pending.
- **Stored JSON.** Payloads and headers are stored as `jsonb`: object keys come back **reordered** (shorter keys first,
  then bytewise) and duplicate keys collapse, values are otherwise identical. A string containing `\u0000` (NUL) or a
  lone UTF-16 surrogate is rejected by PostgreSQL and fails the publish with a database error; strip or encode such
  text before publishing.
- **Sequence gaps.** Republishing an existing event id consumes a sequence number. Gaps are normal; only order matters.
- **Pooling.** Claim, complete, extend and purge need a free connection besides your transactions; size the pool for
  `dispatcher.concurrency` plus publishers. With PgBouncer use session or transaction pooling; no session state is used.
- **Handlers added later.** A delivery added to an old event (new handler, republish with the same id) keeps the
  event's position in its key; if later events of that handler already finished, it runs after them.

## Troubleshooting

- `relation "relaybus_events" does not exist`: `setup()` did not run; start the bus with `setupStore: true` or apply
  `createPostgresSchemaSql()`.
- `permission denied for schema`: the role needs `CREATE` on the schema for `setup()`; with `setupStore: false` it needs
  only DML rights.
- `invalid input syntax for type json` / `\u0000 cannot be converted to text`: payloads with NUL characters are not valid
  `jsonb`.
- Foreign key error on append: the only known cause is republishing an event id in the same instant its last delivery
  is purged. The publishing transaction rolls back and can simply be retried.
- A key looks stuck: a pending or processing head blocks it. Inspect `listDeliveries({ status: 'processing' })` and
  `requeue` dead heads; expired leases are reclaimed automatically.
- `deadlock detected` (`40P01`) or `could not serialize access` (`40001`) while publishing: see Deadlocks and
  Isolation under Operations; retry the transaction.

## Testing

Integration tests use Testcontainers against `postgres:17-alpine` and `postgres:13-alpine` (override with
`RELAYBUS_POSTGRES_IMAGES`). Every adapter except Prisma runs the full `@relaybus/testing` event store contract plus
transaction tests against the real database.

## When to use @relaybus/postgres, and when not to

Use it when your data lives in PostgreSQL 13 or newer and events must commit or roll back with it. It is not for MySQL or MongoDB (see `@relaybus/mongodb` for MongoDB), and the Prisma adapter is typed and unit-tested but not yet integration-tested.

Documentation: <https://berkcansavur.github.io/relaybus/packages/postgres>

### relaybus vs alternatives

<!-- relaybus:snippet:comparison -->

|                                                                      | What it is                                                                                                                                                | Survives a restart                | Written in your database transaction                                      | Choose it when                                                                                                       |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **relaybus**                                                         | Event bus built on the transactional outbox: PostgreSQL and MongoDB stores, Kafka and RabbitMQ transports, per-handler deliveries                         | Yes, events live in your database | Yes                                                                       | Events must never be lost or phantom, and handlers need retries, dead letters and ordering keys                      |
| [`@nestjs/event-emitter`](https://docs.nestjs.com/techniques/events) | In-process event emitter built on `eventemitter2`                                                                                                         | No persistence is documented      | No                                                                        | Simple in-process notifications where losing an event on restart is acceptable                                       |
| [`@nestjs/cqrs`](https://docs.nestjs.com/recipes/cqrs)               | CommandBus, QueryBus, EventBus and sagas; the docs say sagas and event handlers run in memory                                                             | No built-in persistence           | No                                                                        | You want CQRS structure; it can sit on top of an outbox                                                              |
| [`@nestjs/outbox`](https://github.com/nestjs/outbox)                 | Transactional outbox module from the NestJS organisation: PostgreSQL, MySQL and in-memory stores, relay with leases and retries, dead-letter table, inbox | Yes with the database stores      | Yes                                                                       | You need MySQL, or a NestJS-organisation module; compare features with relaybus before choosing                      |
| [BullMQ](https://docs.bullmq.io/)                                    | Job queue on Redis with retries, delayed jobs and flows; at-least-once in the worst case                                                                  | Yes, in Redis                     | No, jobs are stored in Redis and cannot join a SQL or MongoDB transaction | Your stack is Redis-based and background jobs matter more than event atomicity                                       |
| [pg-boss](https://github.com/timgit/pg-boss)                         | Job queue on PostgreSQL using `SKIP LOCKED`, with retries, dead letter queues, scheduling and pub/sub; jobs can be created in your own transaction        | Yes, in PostgreSQL                | Yes, through its database adapters                                        | You want a PostgreSQL job queue with scheduling; relaybus is an event bus with per-handler deliveries and transports |
| Kafka client directly                                                | A distributed log; the idempotent producer and transactions give exactly-once within Kafka                                                                | Yes, in Kafka                     | No, producing to Kafka is a second write                                  | You consume and produce only inside Kafka; add an outbox when a database write must be announced                     |

This table summarises the vendors' own documentation as of October 2026; check their docs for current details.
Full comparison and sources: <https://berkcansavur.github.io/relaybus/comparison>.

<!-- relaybus:end -->
