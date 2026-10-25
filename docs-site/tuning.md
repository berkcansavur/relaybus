---
title: Tuning relaybus for your workload
description: Recommended relaybus settings for a single-connection API, a high-traffic API, a worker fleet, PgBouncer in transaction mode and a MongoDB replica set, and what each option trades.
---

# Tuning relaybus for your workload

The defaults are chosen to be fast for the common case: an API that publishes inside its own transactions and
runs the dispatcher in the same process, against PostgreSQL directly. Change a setting only when your
infrastructure or traffic differs from that, and change the one that matches the difference.

## The settings that matter

**PostgreSQL store** (`new PostgresEventStore({ ... })`)

| Option               | Default    | Trade-off                                                                                                                                                                                                                                                       |
| -------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `preparedStatements` | `true`     | The statements relaybus repeats (publish, claim, complete, renew) are parsed and planned once per connection instead of on every call. Turn off only behind a pooler that cannot keep prepared statements (PgBouncer before 1.21 in transaction mode).          |
| `wakeUp`             | `'notify'` | `'notify'`: publishers send `NOTIFY` (at most one per 10 ms per store) and dispatchers `LISTEN`, so other processes run a new event within milliseconds. `'poll'`: no `NOTIFY` and no listening connection; other processes find new events at their next poll. |
| `completionHistory`  | `false`    | `true` keeps a row per completed delivery for audits and `listDeliveries({ status: 'completed' })`, at one more heap and index write per delivery.                                                                                                              |
| `purgeBatchSize`     | `1000`     | Rows deleted per purge transaction; smaller batches hold row locks for less time.                                                                                                                                                                               |

**Dispatcher** (`new RelayBus({ dispatcher: { ... } })`, or the `dispatcher` option of `RelayBusModule.forRoot`)

| Option                  | Default     | Trade-off                                                                                                                                    |
| ----------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`               | `true`      | `false` on processes that only publish: they then claim nothing and open no listening connection.                                            |
| `concurrency`           | `10`        | Handlers running at once per process. Raise it for I/O-bound handlers; each running handler may need a connection for its completion.        |
| `batchSize`             | concurrency | Deliveries claimed per statement. Larger batches mean fewer claims per delivery.                                                             |
| `pollInterval`          | `'1s'`      | How often an idle dispatcher looks for work it was not woken for: retries that became due, and every new event when nothing wakes it.        |
| `leaseDuration`         | `'30s'`     | How long a crashed worker's deliveries wait before another worker takes them over. Must exceed your slowest handler's time between renewals. |
| `completionBatchWindow` | `'2ms'`     | Completions finishing within this window are written with one statement. `0` writes every completion on its own.                             |

Size the `pg` pool (`max`) for the publishing transactions you run at once plus the dispatcher's `concurrency`.
With `wakeUp: 'notify'` each dispatching process opens one more connection for `LISTEN`, outside the pool.

## Single-connection API

A service that publishes one event per request inside its own transaction and handles the events itself.
The defaults fit: one statement per publish, prepared once per connection, and the publishing process wakes
its own dispatcher right after the commit.

```ts
import { Pool } from 'pg';
import { RelayBus } from '@relaybus/core';
import { PostgresEventStore } from '@relaybus/postgres';

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 10 });
const bus = new RelayBus({
  store: new PostgresEventStore({ pool }),
  source: 'orders',
});

await bus.transaction(async (transaction) => {
  await transaction.query('INSERT INTO orders (id, total) VALUES ($1, $2)', [orderId, total]);
  await bus.publish(new OrderPlaced({ orderId, total }), { transaction });
});
```

Publish several events of one transaction with one `publishAll` call: it is still one statement.

## High-traffic API with many connections

Many API instances, each with a large pool and many requests in flight, publishing far more than they handle.
Let the API processes only publish and leave the handling to a worker fleet (next section). Keep `wakeUp:
'notify'`: each store sends at most one notification per 10 ms, so concurrent publishers do not queue behind
PostgreSQL's notification lock, and workers still start within milliseconds.

```ts
const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 40 });
const bus = new RelayBus({
  store: new PostgresEventStore({ pool }),
  source: 'checkout-api',
  dispatcher: { enabled: false },
});
```

Use one store per process: the notification interval is kept per store instance.

## Worker fleet

Processes that only run handlers, several of them against one database. Claim in larger batches and run more
handlers at once; workers share the backlog through lane leases, so adding a worker adds throughput until the
database is the limit.

```ts
const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 20 });
const bus = new RelayBus({
  store: new PostgresEventStore({ pool }),
  source: 'billing-worker',
  setupStore: false, // the API processes or your migrations create the tables
  dispatcher: {
    concurrency: 50,
    batchSize: 200,
    leaseDuration: '60s', // longer than the slowest handler between renewals
  },
});
```

Raise `concurrency` for handlers that mostly wait on I/O; keep it near the pool size for handlers that write
to the database in the delivery's transaction (`transactional: true`). Keep `pollInterval` at `'1s'`:
notifications start new work, the poll only picks up retries and anything a lost notification missed.

## The transaction budget and who owns the pool

`bus.transaction` holds every transaction to `transactionTimeout` (default `'5s'`). Where the database applies that budget depends on who created the pool:

| Pool                                                             | Where the budget lives                                                                                                                                  | Cost                                                                                             |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Created by the store (`connectionString` or `poolConfig`)        | Each connection keeps the budget as a session setting. The first `bus.transaction` on a connection sets it with `BEGIN`; later ones send nothing extra. | A publish costs what it did without a budget: `BEGIN`, the insert, `COMMIT`.                     |
| Passed in by you (`pool`, or an executor over Prisma, Knex, ...) | The budget is set for the transaction only and is gone when it ends. Your pool's session settings are never changed.                                    | Nothing outside the transaction is affected, and the budget is set again with every transaction. |

The trade-off of the owned pool: statements that run **outside** a transaction on that connection (claiming, completing and renewing deliveries) run under the budget the connection last received. With the default `'5s'` that is a `statement_timeout` of 5 seconds on those statements as well, and a raised budget (say `'30s'`) raises it for them. A transaction without a budget puts the session defaults back in its own `BEGIN`. If you need those statements to follow your own database settings, or want the budget to stay strictly per transaction, pass a pool you created. With `preparedStatements: false`, which you set behind a pooler in transaction mode, the budget stays per transaction on every pool, because session settings would reach other clients.

## PgBouncer in transaction mode

A pooler in transaction mode hands every transaction a different server session. Two things need a session:

- `LISTEN`: notifications are never delivered through it. Use `wakeUp: 'poll'` and shorten `pollInterval` to
  the latency you need, or give the dispatchers a direct connection (session mode) instead.
- Prepared statements: PgBouncer 1.21 and later keeps them per client when `max_prepared_statements` is above
  0, and then the default works. With an older PgBouncer, or with that setting at 0, use
  `preparedStatements: false`. If you forget, the first statement that lands on another session fails once,
  the store stops preparing and logs `[PgExecutor](query) The server does not keep prepared statements per
connection`.

```ts
const pool = new Pool({ connectionString: process.env.PGBOUNCER_URL, max: 20 });
const bus = new RelayBus({
  store: new PostgresEventStore({
    pool,
    wakeUp: 'poll',
    preparedStatements: false, // PgBouncer < 1.21, or max_prepared_statements = 0
  }),
  source: 'orders',
  dispatcher: { pollInterval: '200ms' },
});
```

A publish made in the same process still wakes that process's dispatcher at once; `pollInterval` only bounds
how late other processes see it.

## Through Prisma, TypeORM, Drizzle, Knex or Kysely

The adapters run relaybus' statements through your library, which sends every statement unprepared, so the
database parses and plans each publish and claim again. In our measurements that cost about 0.3 ms per publish and
1.3 ms per claim of 10 deliveries. Where you can, let the dispatcher use a store on a `pg` pool, which prepares, and
keep the adapter for the publishing transactions that must share your library's transaction:

```ts
import { createKyselyExecutor } from '@relaybus/postgres/kysely';

// API process: publishes in your Kysely transactions, dispatches nothing.
const api = new RelayBus({
  store: new PostgresEventStore({ executor: createKyselyExecutor(db) }),
  source: 'orders',
  dispatcher: { enabled: false },
});
// Worker process: dispatches through a pg pool on the same database, with prepared statements.
const worker = new RelayBus({
  store: new PostgresEventStore({ pool }),
  source: 'orders',
});
```

## MongoDB replica set

The MongoDB store needs a replica set (transactions) and has no notifications: a dispatcher is woken at once
by publishes of its own process and finds the rest at its next poll. Size the client pool for publishers plus
`concurrency`, and set `pollInterval` to the latency other processes may add.

```ts
import { MongoClient } from 'mongodb';
import { MongoEventStore } from '@relaybus/mongodb';

const client = await new MongoClient(process.env.MONGO_URL, { maxPoolSize: 50 }).connect();
const bus = new RelayBus({
  store: new MongoEventStore({ client, database: 'orders' }),
  source: 'orders',
  dispatcher: { concurrency: 20, pollInterval: '250ms' },
});
```

Keep the default `transactionOptions` (snapshot reads, majority writes): weaker write concerns can lose an
acknowledged event on failover.

## Related

- [Leases and fencing](/concepts/leases)
- [Guarantees](/guarantees)
- [@relaybus/postgres](/packages/postgres)
