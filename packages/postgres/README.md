# @relaybus/postgres

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

`schema` and `tablePrefix` are validated (`[a-z_][a-z0-9_]*`, names at most 63 characters) before they are placed in SQL;
anything else throws `ConfigurationError`.

## Schema and migrations

`setup()` (called by `bus.start()`) creates everything with `IF NOT EXISTS` inside a transaction guarded by
`pg_advisory_xact_lock`, so any number of processes can start at once. To own the schema yourself, set
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

Indexes (all on `deliveries`): a partial index on the due time of pending/processing rows (claim and statistics), two
partial indexes on `(handler, key, ...)` for the ordering check, `(status, created_at, id)` for `listDeliveries`, and
partial indexes for purging completed and dead rows. `deliveries.id` uses the `C` collation so id tie-breaks are the
same on every database.

## Guarantees

- **Atomic publish.** Events and deliveries are inserted in your transaction; roll back and nothing exists.
- **Per-key order.** On append, a key's counter row in `key_sequences` is upserted
  (`INSERT ... ON CONFLICT (key) DO UPDATE SET sequence = sequence + n`). That row lock is held until the publishing
  transaction ends, so two transactions publishing the same key are serialized and the sequence order equals the commit
  order. Keys are locked in sorted order, events and deliveries are inserted in primary-key order, so concurrent
  publishers cannot deadlock. Claiming hands out a keyed delivery only when no earlier (same handler and key) delivery
  is pending or processing, so one key is processed one event at a time, in order. A retrying or leased head blocks its
  successors; a dead-lettered head releases them.
- **Safe parallel claiming.** One statement chooses candidates with `FOR UPDATE SKIP LOCKED`, applies `LIMIT` after the
  ordering filter, and leases them (`processing`, `attempts + 1`, a new `gen_random_uuid()` lease token). SKIP LOCKED
  alone is sufficient for ordering: only the head of a key is ever a candidate, and a racing claimer either skips the
  locked head or, if the other claim already committed, re-checks the row (now processing, not due) and drops it.
  Followers stay blocked because the head remains pending or processing in every snapshot until it is completed or
  buried. The stress test in `test/claim-concurrency.integration.test.ts` runs 12 claimers on 3 pools against 30 keys
  and fails if a key is ever in flight twice or out of order. A keyed delivery is also not claimed while another
  delivery of the same handler and key is processing, which covers a requeued or newly attached delivery re-entering
  behind a running one.
- **Fenced.** `complete`, `reschedule`, `bury`, `release` and `extendLease` apply only to
  `status = 'processing' AND lease_token = <token>`; a worker that lost its lease changes nothing. `complete` takes the
  handler's transaction, so a transactional handler's writes and its completion commit together.
- **App clock.** Every timestamp is the `now` the caller passes, never the database clock.
- **Driver independent reads.** Timestamps and JSON are read as text and parsed by the store, so custom `pg` type
  parsers (or Drizzle's) cannot change results.

## Operations

- **Isolation.** Claiming runs as one autocommit statement and relies on `READ COMMITTED` (the PostgreSQL default). Do
  not change `default_transaction_isolation` for the relaybus pool.
- **Retention.** `purge` deletes completed (`completed_at < cutoff`) and dead (`updated_at < cutoff`) deliveries in
  batches of `purgeBatchSize`, each batch in its own short transaction with `SKIP LOCKED`, then deletes the events left
  without deliveries. Configure it through the bus `retention` option. Delete rows through `purge` only; events are
  removed by cascade or by purge, never by hand while publishers are active.
- **Lock duration.** A publisher holds its key's counter row until commit. Keep transactions that publish keyed events
  short; unkeyed events take no such lock.
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
- Events without any delivery (appended directly through the store) are never purged; `RelayBus` does not create them.

## Testing

Integration tests use Testcontainers against `postgres:17-alpine` and `postgres:13-alpine` (override with
`RELAYBUS_POSTGRES_IMAGES`). Every adapter except Prisma runs the full `@relaybus/testing` event store contract plus
transaction tests against the real database.
