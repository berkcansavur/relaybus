---
title: Upgrading relaybus
description: 'Step-by-step upgrades between relaybus releases: 0.4.0 to 0.4.1 (PostgreSQL payload columns become json), 0.3 to 0.4 (nothing required; optional pool, wake, Confluent client, record keys, injected producer), 0.2 to 0.3 (route ids, transaction budget, StopResult, decorator and codec renames, error codes) and 0.1 to 0.2 (completed deliveries deleted, completionHistory, schema upgrade, PgBouncer settings).'
---

# Upgrading relaybus

- [0.4.0 → 0.4.1](#upgrading-from-0-4-0-to-0-4-1)
- [0.3 → 0.4](#upgrading-from-0-3-to-0-4)
- [0.2 → 0.3](#upgrading-from-0-2-to-0-3)
- [0.1 → 0.2](#upgrading-from-0-1-to-0-2)

## Upgrading from 0.4.0 to 0.4.1

```yaml
applies to: '@relaybus/* 0.4.0 -> 0.4.1'
downtime: 'none; publishes and claims wait while setup() rewrites the events table once'
rollout: '0.4.0 instances that start after the upgrade need setupStore: false'
database: 'PostgreSQL: payload and headers of the events table become json (one table rewrite)'
rollback: 'safe with setupStore: false; 0.4.0 reads and writes the json columns'
```

**Why.** The PostgreSQL store kept `payload` and `headers` as `jsonb`, which re-orders object keys (shorter keys first, then alphabetically). An event read back from the outbox therefore serialized differently from what the producer published: JSON consumers did not notice, byte or string comparisons and consumers that depend on field order did. 0.4.1 stores both as `json`, which keeps the text the producer serialized, and hands the transport and the handlers exactly that. MongoDB and the in-memory store already kept the order.

**Rows already stored keep the order `jsonb` gave them.** Only events published after the upgrade keep their producer's key order. The values of old rows are unchanged.

### With `setup()` (the default, `setupStore: true`)

Nothing to do. The first 0.4.1 instance that starts converts the two columns in place. It is idempotent and guarded: it runs only while a column is still `jsonb`.

The conversion rewrites the events table once under an exclusive lock, so publishes and claims wait for it. It takes about as long as copying the table; on a large table run it at a quiet time, or run the DDL below yourself first. `setup()` runs it in a transaction of its own, so it cannot deadlock with running completions, and on a schema 0.4.0 created (recognized by the marker 0.4.0 left on the lane table) that is all it does: the other schema statements, whose table locks could queue behind running claims, do not run again.

### Without `setup()` (`setupStore: false`, your own migrations)

Run this once, with your schema and table prefix (the defaults are `public` and `relaybus_`):

```sql
ALTER TABLE "public"."relaybus_events"
  ALTER COLUMN headers DROP DEFAULT,
  ALTER COLUMN payload TYPE json USING payload::json,
  ALTER COLUMN headers TYPE json USING headers::json,
  ALTER COLUMN headers SET DEFAULT '{}'::json;
```

`createPostgresSchemaSql()` contains the same conversion guarded by a column type check, so re-applying its output is safe as well.

### During the rollout

0.4.0 and 0.4.1 instances can run side by side. Before the conversion, 0.4.1 publishes into the `jsonb` columns (PostgreSQL casts on assignment; key order is lost as before); after it, 0.4.0 publishes into the `json` columns the same way. Do not start or restart a 0.4.0 instance with `setupStore: true` once the conversion ran: 0.4.0 does not know the new schema marker, so its `setup()` re-applies all of its schema statements, whose table locks can deadlock with running claims (PostgreSQL then aborts one of them). Finish the rollout first, or give 0.4.0 instances that start during it `setupStore: false`. The same holds for a rollback: deploy 0.4.0 with `setupStore: false`. Rolling back needs no DDL.

### Also in 0.4.1

- **Prisma 5.** `@relaybus/postgres` declares its optional `@prisma/client` peer as `^5.0.0 || ^6.0.0 || ^7.0.0`. With 0.4.0 (`^6 || ^7`), npm refused to install it next to Prisma 5 (`ERESOLVE`) or installed a second Prisma Client. Nothing changes in your code; Prisma 5 needs `previewFeatures = ["driverAdapters"]` for `@prisma/adapter-pg`.
- **jsonEnvelope.** `bodyOrder` now also places the event headers spread by `headers: 'top-level-fields'`; before, they always came last, even when `bodyOrder` named them. Only a mapping whose `bodyOrder` names a spread header writes different bytes, and those are the bytes it asked for; headers it does not name still come last. New options: `otherFields` keeps unknown top-level fields of every JSON type, `wireTypes` names event types differently on the wire. See [Interoperating with other services](/recipes/interoperating-with-other-services).
- **Codec contract.** Compared byte for byte, the decoded payload must now also keep the expected key order, so a case whose expected payload lists keys in another order than the wire fails with a `decoded payload` line; write the expected payload in wire order or use `compare: 'json'`. Jest projects use `@relaybus/testing/jest`; `vitest` is now an optional peer.

## Upgrading from 0.3 to 0.4

```yaml
applies to: '@relaybus/* 0.3.x -> 0.4.0'
downtime: 'none; a rolling deploy is supported'
database: 'no migration; setup() has nothing new to apply'
rollback: 'safe; 0.4.0 writes the same rows, SQL and wire format as 0.3.x'
```

**There is nothing to do.** The public API only grew, defaults are unchanged and the stored and wire formats are the same. Update the packages (keep every `@relaybus/*` on one version) and deploy:

```sh
pnpm add @relaybus/core@0.4.0 @relaybus/postgres@0.4.0
```

Everything below is optional.

### Optional: give an ORM store the pool

With Prisma, Kysely or Drizzle, pass the `pg` pool your ORM's driver uses next to `executor`. Claims run as prepared statements, and an event published in your ORM transaction is dispatched right after the commit instead of at the next poll:

```ts
const pool = new Pool({ connectionString });
const prisma = new PrismaClient({ adapter: new PrismaPg(pool) });
const store = new PostgresEventStore({ executor: createPrismaExecutor(prisma), pool });
```

### Optional: wake the dispatcher after your own commit

For a store without commit notifications (MongoDB, `wakeUp: 'poll'`, an executor without `pool`):

```ts
await myTransaction.commit();
bus.wake();
```

Without it the dispatcher is woken 25, 100 and 250 ms after the latest publish and otherwise at the next poll.

### Optional: the Confluent Kafka client

```ts
import { KafkaJS } from '@confluentinc/kafka-javascript';

const kafka = new KafkaJS.Kafka({ kafkaJS: { clientId: 'orders', brokers: ['kafka:9092'] } });
new KafkaTransport({ kafka, client: 'confluent', groupId: 'billing' });
```

`kafkajs` stays the default and keeps working. The two clients differ in a few behaviours; read [Choosing a client](/packages/kafka#choosing-a-client) first and roll it out per service.

### Optional: control the Kafka record key

```ts
jsonEnvelope({ /* mapping */ recordKey: 'none' }); // or (event) => event.payload.accountId
```

The default `'event-key'` is what 0.3 wrote. A keyless record is placed by the producer, so per-key order then holds only up to the broker.

### Optional: publish through your own producer

```ts
const producer = kafka.producer(); // already connected, owned by your application
await producer.connect();
new KafkaTransport({ producer }); // publish-only; add kafka and groupId to consume as well
```

The transport only calls `send`; it never connects or disconnects the producer. Do not combine `producer` with `producerConfig` or `maxInFlightRequests` (a `ConfigurationError`): configure the producer itself.

### Optional: in-house formats and their tests

```ts
jsonEnvelope({
  type: { header: 'x-event-type' },
  payload: { body: 'payload' },
  occurredAt: { body: 'sentAt', format: 'epoch-millis' },
  extra: { body: { schema: 'orders/v1' }, headers: { 'x-schema-version': '1' } },
});
```

```ts
import { kafkaCodecContract } from '@relaybus/testing';

kafkaCodecContract('orders format', { encode: codec.encode, decode, cases });
```

See [Interoperating with non-relaybus services](/recipes/interoperating-with-other-services) for legacy flat envelopes and the contract helpers.

## Upgrading from 0.2 to 0.3

```yaml
applies to: '@relaybus/* 0.2.x -> 0.3.0'
downtime: 'none; a rolling deploy is supported'
database: 'no migration; setup() has nothing new to apply'
rollback: 'roll forward; 0.2.x does not know the new route ids of deliveries written by 0.3.0'
```

Your code keeps compiling and running: every renamed API still works under its old name. Five things behave differently and are worth checking, in this order. Coming from 0.1, upgrade to 0.2 first (it deletes completed deliveries and upgrades the schema in place), then follow this page.

1. [Update the packages](#step-1-update-the-packages)
2. [Route ids changed](#step-2-route-ids-changed)
3. [Transactions have a time budget](#step-3-transactions-have-a-time-budget)
4. [stop() returns a StopResult](#step-4-stop-returns-a-stopresult)
5. [Renames: decorators and codec](#step-5-renames-decorators-and-codec)
6. [Error codes and the Mongo error field](#step-6-error-codes)
7. [Optional new packages](#step-7-optional-new-packages)

### Step 1: update the packages

Keep every `@relaybus/*` package on the same version:

```sh
pnpm add @relaybus/core@0.3.0 @relaybus/nestjs@0.3.0 @relaybus/postgres@0.3.0
```

Do the same for `mongodb`, `kafka`, `rabbitmq` and `testing` if you use them. Node.js 22.12 or newer is still required.

### Step 2: route ids changed

A route sends deliveries of an event type to a transport destination. Routes to one destination used to be one handler per event type; they are now one handler per destination, so events of one key but different types reach the broker in commit order.

|                  | 0.2.0                                             | 0.3.0                             |
| ---------------- | ------------------------------------------------- | --------------------------------- |
| Route handler id | `relaybus.transport:kafka:orders:order.placed.v1` | `relaybus.transport:kafka:orders` |

**Nothing has to be migrated.** Deliveries stored under a 0.2.0 id, including those written by 0.2.0 processes during a rolling deploy, are still claimed and sent by the new route. The stored rows keep their old id.

#### Order during the upgrade

A delivery under a 0.2.0 id and one under the new id are two queues, so one key's backlog from 0.2.0 is not ordered against events published after the upgrade. Where cross-type order matters, drain the old backlog first:

1. Stop publishing with 0.2.0 (stop the 0.2.0 publishers before the 0.3.0 ones start).
2. Wait until no delivery is left under the old ids:

```ts
const old = await bus.listDeliveries({
  handler: 'relaybus.transport:kafka:orders:order.placed.v1', // one query per routed event type
  status: 'pending',
});
// continue when old is empty
```

3. Start the 0.3.0 publishers.

If order across types does not matter for your consumers, skip this and deploy normally.

#### Two routes that no longer construct

Routes to the same destination now have to resolve to the same `retry` and `timeout`, and an event type may be routed to a destination once; otherwise `new RelayBus` throws a `ConfigurationError`.

```ts
// Before (0.2.0): accepted
routes: [
  { event: OrderPlaced, transport: 'kafka', destination: 'orders', retry: { attempts: 5 } },
  { event: OrderCancelled, transport: 'kafka', destination: 'orders' },
],

// After (0.3.0): the same settings on both routes
routes: [
  { event: OrderPlaced, transport: 'kafka', destination: 'orders', retry: { attempts: 5 } },
  { event: OrderCancelled, transport: 'kafka', destination: 'orders', retry: { attempts: 5 } },
],
```

#### Dashboards and code that match ids

`handlers()`, `listDeliveries({ handler })`, `HandlerContext.handler` and observer events show the new id for new deliveries. Update alerts that filter on the old id; query both ids while the 0.2.0 backlog drains.

### Step 3: transactions have a time budget

`transactionTimeout` is new and defaults to `'5s'`. A `bus.transaction(...)` or `@RelayBusTransaction()` method that runs longer is rolled back before its commit, and the caller gets a `TransactionTimeoutError` (`RELAYBUS_TRANSACTION_TIMEOUT`). The database enforces the budget where it can: on PostgreSQL `statement_timeout` and, with the `pg` executor, `idle_in_transaction_session_timeout` (and `transaction_timeout` on PostgreSQL 17 or newer); on MongoDB `maxCommitTimeMS`.

First look for transactions that call another service. The usual fix is to move the call out:

```ts
// Before: an HTTP call holds the transaction (and its row locks) open
await bus.transaction(async (tx) => {
  const rate = await fetch(ratesUrl).then((r) => r.json());
  await orders.insert(tx, { ...order, rate });
  await bus.publish(new OrderPlaced({ orderId }), { transaction: tx });
});

// After: call first, then a short transaction
const rate = await fetch(ratesUrl).then((r) => r.json());
await bus.transaction(async (tx) => {
  await orders.insert(tx, { ...order, rate });
  await bus.publish(new OrderPlaced({ orderId }), { transaction: tx });
});
```

If a transaction legitimately needs longer (a large `publishAll`, a migration job), raise the budget for the bus:

```ts
new RelayBus({ store, source: 'orders', transactionTimeout: '30s' });
// NestJS
RelayBusModule.forRoot({ store, source: 'orders', transactionTimeout: '30s' });
```

`@relaybus/eslint-plugin` ([step 7](#step-7-optional-new-packages)) finds external calls inside transactions at lint time.

**Who owns the pool matters.** On a pool the store creates from `connectionString` or `poolConfig`, each connection keeps the budget as a session setting, so a publish costs no extra statement; the price is that statements outside a transaction on that connection (claims, completions, renewals) run under the budget the connection last received, `'5s'` by default. On a pool or executor you pass in, the budget lasts only for the transaction and your pool's settings are never changed. Details and when to choose which: [Tuning](/tuning#the-transaction-budget-and-who-owns-the-pool).

### Step 4: stop() returns a StopResult

`bus.stop()` is now bounded: the whole shutdown takes at most `timeout` (default `dispatcher.shutdownTimeout`, 30 s) and resolves with what happened.

```ts
// Before (0.2.0): Promise<void>, no deadline
await bus.stop();

// After (0.3.0): Promise<StopResult>
const { graceful, abandonedDeliveries, durationMs } = await bus.stop({ timeout: '25s' });
if (!graceful)
  logger.warn(`shutdown forced, ${abandonedDeliveries} deliveries left to be taken over`);
```

- Past the deadline, relaybus force-closes what it opened itself (a store's own pool, its `LISTEN` connection, a RabbitMQ connection) and never closes a pool or client you passed in. It never rejects because of the deadline.
- Abandoned deliveries keep their lease until it expires, then another process runs them again. This is the at-least-once guarantee; keep handlers idempotent.
- Align the timeout with Kubernetes: `terminationGracePeriodSeconds` should be larger than `shutdownTimeout`. With NestJS call `app.enableShutdownHooks()`; `RelayBusModule` stops the bus in `beforeApplicationShutdown`, now bounded.
- Existing `await bus.stop()` calls compile and behave as before; only code that needs to know whether the shutdown was graceful has to read the result.

### Step 5: renames: decorators and codec

Both old names keep working, are marked `@deprecated` and are removed in 1.0.0. Moving now means the editor shows no strikethrough and the upgrade to 1.0.0 is a no-op.

#### Decorators

```ts
// Before (0.2.0)
import { Subscribe, EventConsumer } from '@relaybus/nestjs';

@Subscribe(OrderPlaced)
async onPlaced(event: EventEnvelope<OrderPlacedPayload>) {}

@EventConsumer(OrderEvents)
export class BillingHandlers implements HandlersOf<typeof OrderEvents> {}

// After (0.3.0)
import { RelayBusConsumer, RelayBusCatalogConsumer } from '@relaybus/nestjs';

@RelayBusConsumer(OrderPlaced)
async onPlaced(event: EventEnvelope<OrderPlacedPayload>) {}

@RelayBusCatalogConsumer(OrderEvents)
export class BillingHandlers implements HandlersOf<typeof OrderEvents> {}
```

| Old                             | New                                                                                                                        |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `@Subscribe` (method)           | `@RelayBusConsumer` (method, the default style)                                                                            |
| `@EventConsumer` (class)        | `@RelayBusCatalogConsumer` (class, optional)                                                                               |
| Their option and metadata types | `RelayBusConsumerOptions`, `RelayBusConsumerMetadata`, `RelayBusCatalogConsumerOptions`, `RelayBusCatalogConsumerMetadata` |

`@RelayBusConsumer` only compiles on a method and takes an event; `@RelayBusCatalogConsumer` only on a class and takes a catalog. Handler ids are unchanged (`ClassName.methodName` by default), so no stored delivery is orphaned.

#### `decode` becomes `codec`

`KafkaTransport` and `RabbitMqTransport` take one symmetric `codec` for the wire format, both for publishing and for consuming. The `decode` option reads with your function and writes the default format; it is the same as `codec: { encode: encodeEnvelope, decode }`.

```ts
// Before (0.2.0): inbound only
new KafkaTransport({
  kafka,
  groupId: 'billing',
  decode: plainJsonDecoder({ type: fromHeader('event-type') }),
});

// After (0.3.0)
new KafkaTransport({
  kafka,
  groupId: 'billing',
  codec: { encode: encodeEnvelope, decode: plainJsonDecoder({ type: fromHeader('event-type') }) },
});
// or one of the built-in codecs: cloudEventsBinary (default), cloudEventsStructured, jsonEnvelope({ ... })
```

Giving both `decode` and `codec` is a configuration error. The default codec, `cloudEventsBinary`, writes exactly the bytes 0.2.0 wrote (pinned by golden tests), so services you did not upgrade keep reading your messages. See the [interoperability recipe](/recipes/interoperating-with-other-services).

### Step 6: error codes

- **Every relaybus error has a `code`.** `ErrorInfo` (the `lastError` of a delivery, the `error` of observer events) gains an optional `code` for relaybus errors; other errors are unchanged. Records written before 0.3.0 have no `code`, so read it as optional. The list is on [Error codes](/concepts/error-codes).
- **Five error classes report their own name.** `InvalidKafkaMessageError`, `RabbitMqUnavailableError`, `RabbitMqPublishRejectedError`, `UnroutableMessageError` and `MalformedMessageError` used to report `name: 'RelayBusError'`. Alerts that matched that name should match the class name or, better, the `code`.
- **MongoDB keeps the code.** The MongoDB store now stores `code` in the `lastError` of a delivery, as PostgreSQL always did (it stores `ErrorInfo` as JSON). Documents without `code` stay valid.
- **Broker dead letters carry more headers.** Kafka adds `relaybus-error-name` and `relaybus-error-code` to records copied to `<topic>.dead`; RabbitMQ adds `x-relaybus-dead-letter-error-name` and `x-relaybus-dead-letter-error-code`. The reason headers are unchanged.
- **`onFailure` has a new stage, `'backlog'`.** Add a case if you `switch` exhaustively over the stage.

### Step 7: optional new packages

Neither is required, and neither changes behaviour until you add it.

```sh
pnpm add -D @relaybus/eslint-plugin     # lint: no external call inside a transaction, no publish outside one
pnpm add @relaybus/otel @opentelemetry/api # metrics and traces
```

```js
// eslint.config.js
import tseslint from 'typescript-eslint';
import relaybus from '@relaybus/eslint-plugin';

export default tseslint.config(...tseslint.configs.recommended, relaybus.configs.recommended);
```

```ts
import { relayBusOpenTelemetry } from '@relaybus/otel';

const otel = relayBusOpenTelemetry(); // global meter and tracer providers

new RelayBus({ store, observers: [otel.observer], tracer: otel.tracer, backlogInterval: '30s' });
```

See [Transactions in NestJS](/recipes/transactions-in-nestjs) and [Observability](/concepts/observability).

### Checklist

- [ ] All `@relaybus/*` packages on 0.3.0
- [ ] Routes to one destination share `retry` and `timeout`
- [ ] Old route backlog drained, if cross-type order matters
- [ ] No transaction waits on a slow external call (or `transactionTimeout` raised on purpose)
- [ ] On a pool the store owns, you accept that statements outside transactions share the transaction budget (or pass your own pool)
- [ ] `stop()` callers handle `StopResult`; grace period larger than `shutdownTimeout`
- [ ] Dashboards read both route ids during the drain

## Upgrading from 0.1 to 0.2

```yaml
applies to: '@relaybus/* 0.1.0 -> 0.2.0'
downtime: 'a short stop of every 0.1.0 worker (the upgraded schema is not compatible with 0.1.0 code)'
rollback: 'restore from a backup; there is no down migration'
```

0.2.0 changes how deliveries are stored ([why](/performance/write-amplification)). Your code keeps working: `publish`, `@Subscribe`, handlers, retries and dead letters have the same API. What changes is the database layout, which `setup()` upgrades in place, and one visible behaviour: completed deliveries are deleted.

### What changes for you

| 0.1.0                                                                      | 0.2.0                                                                                                                      | Action                                                                     |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| A completed delivery stays as a `completed` row until retention purges it. | It is **deleted** when it completes. `listDeliveries({ status: 'completed' })` is empty and `statistics().completed` is 0. | If you read completed deliveries, set `completionHistory: true`.           |
| Each delivery has lease columns and a `processing` status.                 | Workers lease 256 lanes; the lease columns are dropped and `processing` deliveries become `pending`.                       | Stop 0.1.0 workers first; `setup()` converts the data.                     |
| Deduplication relies on completed rows.                                    | Every event remembers the handlers it ever had a delivery for, so a redelivered event runs no handler twice.               | None. Keep `retention.completed` longer than any broker redelivery window. |
| No notifications.                                                          | PostgreSQL `LISTEN`/`NOTIFY` wakes other processes.                                                                        | None, or `wakeUp: 'poll'` behind a pooler.                                 |
| Statements are parsed on every call.                                       | Repeated statements are prepared per connection.                                                                           | None, or `preparedStatements: false` behind PgBouncer before 1.21.         |

### Before you start

1. **Back up the database.** There is no down migration.
2. **Rehearse on a copy** of production data and time it. The PostgreSQL upgrade rewrites every `deliveries` row once.
3. **Purge old completed deliveries** to shorten that rewrite: set a short `retention.completed` and run `bus.purgeExpired()` while still on 0.1.0.
4. **Update every package to the same version**, for example `pnpm add @relaybus/core@0.2.0 @relaybus/nestjs@0.2.0 @relaybus/postgres@0.2.0` (and `mongodb`, `kafka`, `rabbitmq`, `testing` if you use them).

### Step 1: stop every 0.1.0 worker

Scale the processes that run a dispatcher (API processes with the dispatcher enabled, worker fleets) to zero. Running 0.1.0 and 0.2.0 together is not supported: 0.1.0 code fails on the dropped columns.

### Step 2: upgrade the schema

Choose one.

#### Option A: let `setup()` do it (default)

Start one 0.2.0 process with `setupStore: true` (the default). `setup()` runs in one transaction under an advisory lock and is safe to run again. In order, on PostgreSQL it:

1. Detects a 0.1.0 table by its `deliveries.lease_token` column.
2. Adds `events.handlers`, filled from the existing deliveries (completed ones included, so nothing that completed under 0.1.0 runs again).
3. Adds `deliveries.lane` with the same hash new deliveries use, turns `processing` deliveries into `pending` (the attempt stays counted, as a lost lease would count it) and drops `lease_owner`, `lease_token` and `lease_expires_at`.
4. Drops the indexes that no longer exist, creates the new ones and seeds the 256 `lanes` rows.

Deliveries that were `completed` stay as `completed` rows and are purged by `retention.completed`, or kept forever with `retention: { completed: false }`. New completions are deleted.

#### Option B: run the SQL with your own migration tool

When your application role has no DDL rights, or you apply migrations from CI, render the script and run it as **one transaction with the workers stopped**:

```ts
import { createPostgresSchemaSql } from '@relaybus/postgres';

// schema and tablePrefix must match your store options
console.log(createPostgresSchemaSql({ schema: 'app', tablePrefix: 'bus_' }));
```

Then start 0.2.0 with `setupStore: false`, so the processes only need DML rights.

#### MongoDB

`setup()` upgrades the database in place and repeatably: `processing` deliveries become `pending` and due at once (the attempt stays counted), per-delivery lease fields are removed, every delivery gets its `laneId` and every event its `handlers` and `createdAt` (in batches of 1,000), the old indexes are dropped and the 256 lane documents are created. The lane documents mark the upgrade as done, so later runs skip it. `completed` documents left by 0.1.0 stay until `purge({ completedBefore })` removes them. There is no SQL to run: if your application role cannot create collections, call `setup()` once from a deployment script with a role that can.

### Step 3: keep completion history (optional)

If an audit, a dashboard or a test reads completed deliveries, opt in:

```ts
new PostgresEventStore({ pool, completionHistory: true });
new MongoEventStore({ client, database: 'orders', completionHistory: true });
new InMemoryEventStore({ retainCompleted: true }); // tests
```

With it, each completion also inserts one append-only row into `<prefix>completed_deliveries` (a collection on MongoDB) in the same transaction that deletes the delivery. `listDeliveries({ status: 'completed' })` and `statistics().completed` read it, and `retention.completed` purges it. The cost is one more heap and index write per delivery, so leave it off on hot paths. Use the same value in every process.

Completed rows that 0.1.0 left in `deliveries` keep being listed until retention purges them, whatever this option says.

### Step 4: start 0.2.0 and verify

Start the workers, one at a time if you want to watch, then check:

```ts
console.log(await bus.statistics()); // pending falls, dead is unchanged, nothing stays in processing
```

```sql
SELECT count(*) FROM relaybus_lanes;                          -- 256
SELECT count(*) FROM relaybus_deliveries WHERE lane IS NULL;  -- 0
```

Dead letters from 0.1.0 are still listed and can be requeued as before.

### PgBouncer and other poolers

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

### New optional APIs

None of these is required to upgrade.

- **Event catalogs:** `defineCatalog`, `HandlersOf`, `defineHandlers`, `bus.forCatalog`, `bus.subscribeCatalog`, `consume(...)` and `produce(...)` roles, and `@EventConsumer` in `@relaybus/nestjs`. [Guide](/concepts/event-catalogs).
- **Foreign messages:** `decode: plainJsonDecoder(...)` and `decode: decodeCloudEvent` on the Kafka and RabbitMQ transports. [Guide](/recipes/interoperating-with-other-services).
- **Store options:** `wakeUp`, `preparedStatements`, `completionHistory`. [Tuning](/tuning).

### Rollback

Going back to 0.1.0 needs a restore of the backup you took before step 2. Keep it until you are happy with 0.2.0 in production.

### Related

- [Releases: what is new and known issues](/releases)
- [Performance: 0.2.0 vs 0.1.0](/performance)
- [@relaybus/postgres: Upgrading from 0.1.0](/packages/postgres#upgrading-from-0-1-0)
- [@relaybus/mongodb: Upgrading from 0.1.0](/packages/mongodb#upgrading-from-0-1-0)
