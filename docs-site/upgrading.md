---
title: Upgrading relaybus from 0.1 to 0.2
description: Step-by-step upgrade of relaybus 0.1 to 0.2 on PostgreSQL and MongoDB, what changes for completed deliveries, how to keep history with completionHistory, running the schema upgrade yourself, PgBouncer settings and the new optional APIs.
---

# Upgrading from 0.1 to 0.2

```yaml
applies to: '@relaybus/* 0.1.0 -> 0.2.0'
downtime: 'a short stop of every 0.1.0 worker (the upgraded schema is not compatible with 0.1.0 code)'
rollback: 'restore from a backup; there is no down migration'
```

0.2.0 changes how deliveries are stored ([why](/performance/write-amplification)). Your code keeps working: `publish`, `@Subscribe`, handlers, retries and dead letters have the same API. What changes is the database layout, which `setup()` upgrades in place, and one visible behaviour: completed deliveries are deleted.

## What changes for you

| 0.1.0                                                                      | 0.2.0                                                                                                                      | Action                                                                     |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| A completed delivery stays as a `completed` row until retention purges it. | It is **deleted** when it completes. `listDeliveries({ status: 'completed' })` is empty and `statistics().completed` is 0. | If you read completed deliveries, set `completionHistory: true`.           |
| Each delivery has lease columns and a `processing` status.                 | Workers lease 256 lanes; the lease columns are dropped and `processing` deliveries become `pending`.                       | Stop 0.1.0 workers first; `setup()` converts the data.                     |
| Deduplication relies on completed rows.                                    | Every event remembers the handlers it ever had a delivery for, so a redelivered event runs no handler twice.               | None. Keep `retention.completed` longer than any broker redelivery window. |
| No notifications.                                                          | PostgreSQL `LISTEN`/`NOTIFY` wakes other processes.                                                                        | None, or `wakeUp: 'poll'` behind a pooler.                                 |
| Statements are parsed on every call.                                       | Repeated statements are prepared per connection.                                                                           | None, or `preparedStatements: false` behind PgBouncer before 1.21.         |

## Before you start

1. **Back up the database.** There is no down migration.
2. **Rehearse on a copy** of production data and time it. The PostgreSQL upgrade rewrites every `deliveries` row once.
3. **Purge old completed deliveries** to shorten that rewrite: set a short `retention.completed` and run `bus.purgeExpired()` while still on 0.1.0.
4. **Update every package to the same version**, for example `pnpm add @relaybus/core@0.2.0 @relaybus/nestjs@0.2.0 @relaybus/postgres@0.2.0` (and `mongodb`, `kafka`, `rabbitmq`, `testing` if you use them).

## Step 1: stop every 0.1.0 worker

Scale the processes that run a dispatcher (API processes with the dispatcher enabled, worker fleets) to zero. Running 0.1.0 and 0.2.0 together is not supported: 0.1.0 code fails on the dropped columns.

## Step 2: upgrade the schema

Choose one.

### Option A: let `setup()` do it (default)

Start one 0.2.0 process with `setupStore: true` (the default). `setup()` runs in one transaction under an advisory lock and is safe to run again. In order, on PostgreSQL it:

1. Detects a 0.1.0 table by its `deliveries.lease_token` column.
2. Adds `events.handlers`, filled from the existing deliveries (completed ones included, so nothing that completed under 0.1.0 runs again).
3. Adds `deliveries.lane` with the same hash new deliveries use, turns `processing` deliveries into `pending` (the attempt stays counted, as a lost lease would count it) and drops `lease_owner`, `lease_token` and `lease_expires_at`.
4. Drops the indexes that no longer exist, creates the new ones and seeds the 256 `lanes` rows.

Deliveries that were `completed` stay as `completed` rows and are purged by `retention.completed`, or kept forever with `retention: { completed: false }`. New completions are deleted.

### Option B: run the SQL with your own migration tool

When your application role has no DDL rights, or you apply migrations from CI, render the script and run it as **one transaction with the workers stopped**:

```ts
import { createPostgresSchemaSql } from '@relaybus/postgres';

// schema and tablePrefix must match your store options
console.log(createPostgresSchemaSql({ schema: 'app', tablePrefix: 'bus_' }));
```

Then start 0.2.0 with `setupStore: false`, so the processes only need DML rights.

### MongoDB

`setup()` upgrades the database in place and repeatably: `processing` deliveries become `pending` and due at once (the attempt stays counted), per-delivery lease fields are removed, every delivery gets its `laneId` and every event its `handlers` and `createdAt` (in batches of 1,000), the old indexes are dropped and the 256 lane documents are created. The lane documents mark the upgrade as done, so later runs skip it. `completed` documents left by 0.1.0 stay until `purge({ completedBefore })` removes them. There is no SQL to run: if your application role cannot create collections, call `setup()` once from a deployment script with a role that can.

## Step 3: keep completion history (optional)

If an audit, a dashboard or a test reads completed deliveries, opt in:

```ts
new PostgresEventStore({ pool, completionHistory: true });
new MongoEventStore({ client, database: 'orders', completionHistory: true });
new InMemoryEventStore({ retainCompleted: true }); // tests
```

With it, each completion also inserts one append-only row into `<prefix>completed_deliveries` (a collection on MongoDB) in the same transaction that deletes the delivery. `listDeliveries({ status: 'completed' })` and `statistics().completed` read it, and `retention.completed` purges it. The cost is one more heap and index write per delivery, so leave it off on hot paths. Use the same value in every process.

Completed rows that 0.1.0 left in `deliveries` keep being listed until retention purges them, whatever this option says.

## Step 4: start 0.2.0 and verify

Start the workers, one at a time if you want to watch, then check:

```ts
console.log(await bus.statistics()); // pending falls, dead is unchanged, nothing stays in processing
```

```sql
SELECT count(*) FROM relaybus_lanes;                          -- 256
SELECT count(*) FROM relaybus_deliveries WHERE lane IS NULL;  -- 0
```

Dead letters from 0.1.0 are still listed and can be requeued as before.

## PgBouncer and other poolers

A pooler in transaction mode gives each transaction a different server session, and 0.2.0 uses two things that need a session: `LISTEN` and prepared statements.

| Setup                                                                           | Settings                                                                               |
| ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Direct to PostgreSQL, or PgBouncer in session mode                              | Defaults.                                                                              |
| PgBouncer 1.21 or newer in transaction mode with `max_prepared_statements > 0`  | `wakeUp: 'poll'` (no `LISTEN` through a transaction pooler); prepared statements work. |
| PgBouncer **before 1.21** in transaction mode, or `max_prepared_statements = 0` | `preparedStatements: false` and `wakeUp: 'poll'`.                                      |

```ts
new PostgresEventStore({
  pool, // connects through PgBouncer
  wakeUp: 'poll',
  preparedStatements: false,
});
```

If you forget `preparedStatements: false`, the first statement that lands on another session fails once, the store stops preparing and logs `[PgExecutor](query) The server does not keep prepared statements per connection`. When you poll, shorten `dispatcher.pollInterval` to the latency you need. See [Tuning](/tuning#pgbouncer-in-transaction-mode).

## New optional APIs

None of these is required to upgrade.

- **Event catalogs:** `defineCatalog`, `HandlersOf`, `defineHandlers`, `bus.forCatalog`, `bus.subscribeCatalog`, `consume(...)` and `produce(...)` roles, and `@EventConsumer` in `@relaybus/nestjs`. [Guide](/concepts/event-catalogs).
- **Foreign messages:** `decode: plainJsonDecoder(...)` and `decode: decodeCloudEvent` on the Kafka and RabbitMQ transports. [Guide](/recipes/interoperating-with-other-services).
- **Store options:** `wakeUp`, `preparedStatements`, `completionHistory`. [Tuning](/tuning).

## Rollback

Going back to 0.1.0 needs a restore of the backup you took before step 2. Keep it until you are happy with 0.2.0 in production.

## Related

- [Releases: what is new and known issues](/releases)
- [Performance: 0.2.0 vs 0.1.0](/performance)
- [@relaybus/postgres: Upgrading from 0.1.0](/packages/postgres#upgrading-from-0-1-0)
- [@relaybus/mongodb: Upgrading from 0.1.0](/packages/mongodb#upgrading-from-0-1-0)
