# @relaybus/postgres

@relaybus/postgres is the PostgreSQL event store of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it writes the transactional outbox in your own transaction, with `pg`, Prisma, TypeORM, Drizzle, Knex or Kysely.

PostgreSQL event store for [relaybus](https://github.com/berkcansavur/relaybus): a transactional outbox with
lane leases, batched claim and completion, per-key ordering and fenced state changes. PostgreSQL 13 or newer.
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

| Option                                   | Default               | Meaning                                                                                                            |
| ---------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `pool` / `connectionString` / `executor` | required, exactly one | How to reach the database.                                                                                         |
| `poolConfig`                             | none                  | Extra `pg` pool settings when `connectionString` is used.                                                          |
| `schema`                                 | `'public'`            | Schema of the tables. Created by `setup()` when it is not `public`.                                                |
| `tablePrefix`                            | `'relaybus_'`         | Tables are `<prefix>events`, `<prefix>deliveries`, `<prefix>key_sequences`, `<prefix>lanes`.                       |
| `completionHistory`                      | `false`               | Keep an append-only record of completed deliveries (`<prefix>completed_deliveries`). Same value in every process.  |
| `purgeBatchSize`                         | `1000`                | Deliveries deleted per transaction by `purge`.                                                                     |
| `logger`                                 | `console`             | Receives idle-connection errors of a pool the store created itself.                                                |
| `preparedStatements`                     | `true`                | Prepare the statements the store repeats once per connection. `false` behind PgBouncer < 1.21 in transaction mode. |
| `wakeUp`                                 | `'notify'`            | `'notify'`: coalesced `NOTIFY` + `LISTEN` wake other processes. `'poll'`: no notifications, other processes poll.  |

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

| Table                  | Purpose                                                                                                                                                                                                                                                                            |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `events`               | One row per event: `id` (**text**, so any caller id works), `type`, `source`, `key`, `sequence`, `payload jsonb`, `headers jsonb`, correlation and causation ids, `occurred_at`, `created_at`, and `handlers`, the handlers that ever got a delivery (it outlives the deliveries). |
| `deliveries`           | One row per (event, handler) that is not finished: `lane`, `status` (`pending` or `dead`), `attempts`, `available_at`, `last_error jsonb`, key and sequence copied from the event, timestamps. `unique (event_id, handler)`, foreign key to events with `ON DELETE CASCADE`.       |
| `lanes`                | 256 rows, one per lane: `epoch` (the fencing token), `lease_owner`, `lease_expires_at` and `in_flight` (ids of the deliveries the owner claimed). `fillfactor = 50` and no secondary index, so its updates are heap-only (HOT).                                                    |
| `event_completions`    | One row per event that had a delivery complete, with the time of its last completion. Retention of completed work (`retention.completed`) is measured from it, however old the event is; events that never had a delivery complete are measured from their creation.               |
| `key_sequences`        | One counter row per ordering key.                                                                                                                                                                                                                                                  |
| `completed_deliveries` | Only with `completionHistory`: one insert-only row per completed delivery.                                                                                                                                                                                                         |

A delivery row is inserted once and deleted once when it completes. Claiming, renewing a lease and completing never
update it: they update the lane row (claim, renewal) or delete the delivery (completion), so the indexed columns of a
delivery are written exactly once, at publish. Indexes on `deliveries`: `(lane, available_at, id) WHERE status = 'pending'`
(claim and statistics), `(lane, handler, key, sequence) WHERE key IS NOT NULL AND status = 'pending'` (ordering check
and the blocked-key claim), the unique `(event_id, handler)`, and partial indexes for purging dead deliveries (and
completed ones left by 0.1.0); on `events`, `(created_at, id)` for the purge sweep. `listDeliveries` has no index of its
own: it orders by `created_at`, so on a large `deliveries` table combine it with `status`/`handler`, which keeps the scan
to the dead or pending rows, or add the index you need. `deliveries.id` uses the `C` collation so id tie-breaks are the
same on every database. The tables are created with aggressive autovacuum settings
(`autovacuum_vacuum_scale_factor = 0.02`): deleted deliveries leave dead tuples that autovacuum reclaims.

## Upgrading from 0.1.0

The step-by-step guide, including `completionHistory` and PgBouncer settings, is [Upgrading from 0.1 to 0.2](https://berkcansavur.github.io/relaybus/upgrading).

`setup()` upgrades a 0.1.0 schema in place, in one transaction under the same advisory lock, and is safe to run again:
it adds `lane` to every delivery (hash of the key, or of the event id) and `handlers` to every event (from the
existing deliveries), turns `processing` deliveries into `pending` (their attempt stays counted, as a lost lease would
count it), drops the per-delivery lease columns and the indexes that no longer exist, and creates the lane table.
Deliveries that were `completed` stay as `completed` rows until `retention.completed` purges them (or forever with
`retention: { completed: false }`). **Stop every 0.1.0 worker before running it** (they cannot use the new tables), and
expect the one-off rewrite of `deliveries` (an `UPDATE` of every row plus `SET NOT NULL`) to take time proportional to
its size; purge old completed rows first to shorten it. If you apply `createPostgresSchemaSql()` with your own migration
tool, run it as one transaction with the workers stopped. Running 0.1.0 and the new version together is not supported.

## Guarantees

- **Atomic publish.** Events and deliveries are inserted in your transaction; roll back and nothing exists.
- **Per-key order.** On append, a key's counter row in `key_sequences` is upserted
  (`INSERT ... ON CONFLICT (key) DO UPDATE SET sequence = sequence + n`). That row lock is held until the publishing
  transaction ends, so two transactions publishing the same key are serialized and the sequence order equals the commit
  order. Within one `append` (one `publishAll`) keys are locked in sorted order and events and deliveries are inserted in
  primary-key order, so concurrent single calls cannot deadlock each other. Claiming hands out a keyed delivery only when no earlier (same handler and key) delivery
  is pending or processing, so one key is processed one event at a time, in order. A retrying or leased head blocks its
  successors; a dead-lettered head releases them. A `limit` of zero or less claims nothing.
- **Lane leases.** Every delivery belongs to one of 256 lanes: a hash of its ordering key, or of its event id when it
  has none, so all deliveries of a key share a lane. Workers lease lanes, not rows. A claim runs in one short
  transaction: it locks the lane rows this worker already holds, takes free lanes that have due work (four, then eight,
  ... lanes at a time, so a worker holds only as many lanes as its batch needs, oldest due delivery first), reads the due
  deliveries of those lanes and writes **one** lane row per lane to record the batch (`in_flight`). It writes no delivery
  row and takes no row lock on one: exclusivity comes from the lease. A lane taken from another worker gets a new
  `epoch`; lanes that yielded nothing are handed back at once. While a lane lease is live, other workers do not see the
  pending deliveries of that lane. The lease is renewed with the running deliveries (`extendLease` updates the lane
  row) and handed back at graceful shutdown (`relinquish`).
- **Per-key order under lanes.** A keyed delivery is claimed only when no earlier delivery of its handler and key is
  pending (a running one is still a pending row) and none of its handler and key is in flight, and the lane belongs to
  one worker, so a key is processed one event at a time, in order. A retrying or running head blocks its successors; a
  dead-lettered head releases them. Claiming first looks at the 256 earliest due deliveries of the lanes it holds
  (cheap); only when those are mostly followers of blocked keys and fewer than `limit` were picked does a second
  statement find the head of every key with `DISTINCT ON (handler, key)`. The stress test in
  `test/claim-concurrency.integration.test.ts` runs 12 claimers on 3 pools against 30 keys and fails if a key is ever in
  flight twice or out of order.
- **Fenced.** A lease token is `<lane>:<epoch>:<claims>`, where `claims` counts how many claims of that delivery ended before this one (every `reschedule`, `release`, `bury` and takeover bumps it), so a lease of an older attempt never matches a newer claim even when the same worker re-claims the delivery in the same lane epoch. `complete`, `completeMany`, `reschedule`, `bury`, `release` and
  `extendLease` apply only while the lane still has that epoch and lists the delivery as in flight; a worker that lost
  its lane changes nothing. Completion takes a `FOR KEY SHARE` lock on the lane row until the transaction ends, so a takeover
  (which locks the row `FOR UPDATE`) and a transactional handler's completion cannot both succeed: the handler's writes
  and the deletion of its delivery commit together, or the takeover redelivers it.
- **Attempts survive crashes.** An attempt is recorded when it ends: a failure adds one to `attempts` (`reschedule`,
  `bury`), a graceful `release` adds nothing, and a lane that is taken over charges one attempt to every delivery its
  previous holder still had in flight. A worker that dies on a poison message therefore still runs it into the retry
  limit and the dead-letter table.
- **No double run after completion.** An event lists every handler it ever had a delivery for, so republishing an
  event id (or a broker redelivering it) creates deliveries only for handlers that never had one, even though completed
  deliveries are deleted. This holds while the event row exists (`retention.completed`).
- **App clock.** Every timestamp is the `now` the caller passes, never the database clock.
- **Driver independent reads.** Timestamps and JSON are read as text and parsed by the store, so custom `pg` type
  parsers (or Drizzle's) cannot change results.

## Operations

- **Isolation.** Claiming runs as a short transaction and relies on `READ COMMITTED` (the PostgreSQL default). Do
  not change `default_transaction_isolation` for the relaybus pool. Your own publishing transaction may use a stricter
  level, but under `REPEATABLE READ` or `SERIALIZABLE` PostgreSQL can abort it with `40001` (serialization failure);
  retry the whole transaction.
- **Deadlocks.** Publishing a keyed event locks that key's counter row until your transaction ends. Two transactions
  that append keys in opposite order across **several** `append`/`publish` calls (A then B in one, B then A in the
  other) can deadlock; PostgreSQL aborts one with `40P01`. The aborted transaction rolls back completely, nothing is
  lost or half-written, and the caller should retry it. Publishing all keys of a transaction in one `publishAll` call
  avoids the problem because the store locks them in sorted order.
- **Retention.** Completed deliveries are deleted when they complete, so `purge` has two jobs: it deletes dead
  (`updated_at < cutoff`) deliveries, completion records (`completionHistory`) and `completed` rows left by 0.1.0 in
  batches of `purgeBatchSize`, each batch in its own short transaction with `SKIP LOCKED`, and it deletes the events left
  without deliveries or records, including events created before the earliest cutoff you passed that never had one (a publish
  whose delivery was purged in the same instant, or an event with no subscriber). Configure it through the bus `retention` option (`retention.completed` is therefore the horizon of event deduplication and of the events' storage). Delete rows through `purge` only; events are
  removed by cascade or by purge, never by hand while publishers are active.
- **Deduplication horizon.** An event remembers its handlers only as long as the event row exists. Once `purge` deletes
  the event (older than `retention.completed` and without deliveries), a broker redelivery or publish retry with the same
  id is a new event and runs its handlers again; keep `retention.completed` longer than any redelivery window.
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
- **Vacuum.** Completing a delivery deletes its row, which leaves a dead tuple until autovacuum runs. The tables are
  created with `autovacuum_vacuum_scale_factor = 0.02`; for sustained throughput above a few thousand deliveries per
  second also lower `autovacuum_vacuum_cost_delay` for this table (or run a periodic `VACUUM`), and watch
  `pg_stat_user_tables.n_dead_tup`. The lane table is vacuumed by HOT pruning alone.
- **Lost answers and stale reads.** A claim is one statement. If the connection fails after the database recorded the
  batch but before the answer arrived, the store gives back every delivery of its own lanes that it never handed out (it remembers what it
  handed out until the dispatcher settled it). A claim that waited for a lane lock re-runs itself when the lane changed meanwhile, so it
  never claims from a snapshot older than the lane it got. The dispatcher retries a completion, reschedule, bury or release that the store
  did not answer until it lands or the lease is gone, so a delivery cannot stay in flight under a lane that other deliveries keep renewing.
- **Lane exclusivity.** While a worker holds a lane under a live lease, other workers do not see its pending
  deliveries, even when they sit idle. A worker that dies delays its lanes by at most the lease duration
  (`dispatcher.leaseDuration`); a worker that stops gracefully gives them back immediately. Lanes are chosen
  per claim, so a busy backlog spreads over workers; a few hot keys live in few lanes and run on few workers by
  design (they are ordered).
- **Round trips.** A claim is normally one statement: it claims from the lanes the worker holds and takes free lanes in
  the same statement. Only taking over a lane whose previous holder died with deliveries running (their lost attempts
  are charged first) continues in a short transaction. Publishing new events is one statement per `append`, whatever
  the batch size; adding a handler to an event that already exists costs two more.
- **Wake-ups.** With `wakeUp: 'notify'` (the default) a publish that inserted a delivery due now notifies the
  dispatchers (none for delayed deliveries; PostgreSQL merges repeats within a transaction and drops them on
  rollback). A store sends at most one notification per 10 ms from its publishing statements; publishes in between
  leave one trailing notification that the store sends after their commit, so concurrent publishers do not queue
  behind PostgreSQL's notification lock. A dispatcher on a store built from `pool` or `connectionString` listens on
  that channel (`<schema>.<prefix>deliveries`, hashed when longer than 63 characters) on one connection of its own,
  opened with the pool's settings outside the pool, so an event published by any process runs within milliseconds
  instead of at the next poll. A lost listening connection is replaced after a second; polling remains the safety
  net, so a missed notification only costs latency. A store built on an `executor` cannot listen and relies on
  polling (and on the wake-up of its own process after a publish). `wakeUp: 'poll'` sends and listens for nothing.
- **Prepared statements.** With `preparedStatements: true` (the default) the publish, claim, completion and renewal
  statements are named prepared statements: each connection parses and plans them once. Only `PgExecutor` (the
  default `pool` / `connectionString` path) prepares; the library adapters send every statement unprepared. See
  `docs/performance/publish-path.md` for what that saves.
- **Pooling.** Claim, complete, extend and purge need a free connection besides your transactions; size the pool for
  `dispatcher.concurrency` plus publishers; the listening connection comes on top of the pool's `max`. Listening and
  prepared statements need a session: with PgBouncer in transaction pooling use `wakeUp: 'poll'`, and
  `preparedStatements: false` unless PgBouncer is 1.21 or newer with `max_prepared_statements` above 0.
- **Handlers added later.** A delivery added to an old event (new handler, republish with the same id) keeps the
  event's position in its key; if later events of that handler already finished, it runs after them.
- **Listing completed deliveries.** Without `completionHistory`, `listDeliveries({ status: 'completed' })` is empty and
  `statistics().completed` is 0 (rows left by 0.1.0 excepted). With it, every completion also inserts one row into
  `completed_deliveries` (one more heap and index write per delivery); use it for audits, not for throughput.

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
  `requeue` dead heads; expired lane leases are taken over automatically.
- Deliveries wait although workers are idle: another worker holds their lane under a live lease (look at
  `<prefix>lanes.lease_owner` and `lease_expires_at`); it is released when that worker finishes, stops, or the lease expires.
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
