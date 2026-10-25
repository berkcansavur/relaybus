---
title: Upgrading relaybus from 0.2 to 0.3
description: Step-by-step upgrade of relaybus 0.2 to 0.3 with before and after code, the route id change and draining the 0.2 route backlog, the 5 second transaction budget, the StopResult of stop(), decorator and codec renames, new error codes and the optional new packages.
---

# Upgrading from 0.2 to 0.3

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

## Step 1: update the packages

Keep every `@relaybus/*` package on the same version:

```sh
pnpm add @relaybus/core@0.3.0 @relaybus/nestjs@0.3.0 @relaybus/postgres@0.3.0
```

Do the same for `mongodb`, `kafka`, `rabbitmq` and `testing` if you use them. Node.js 22.12 or newer is still required.

## Step 2: route ids changed

A route sends deliveries of an event type to a transport destination. Routes to one destination used to be one handler per event type; they are now one handler per destination, so events of one key but different types reach the broker in commit order.

|                  | 0.2.0                                             | 0.3.0                             |
| ---------------- | ------------------------------------------------- | --------------------------------- |
| Route handler id | `relaybus.transport:kafka:orders:order.placed.v1` | `relaybus.transport:kafka:orders` |

**Nothing has to be migrated.** Deliveries stored under a 0.2.0 id, including those written by 0.2.0 processes during a rolling deploy, are still claimed and sent by the new route. The stored rows keep their old id.

### Order during the upgrade

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

### Two routes that no longer construct

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

### Dashboards and code that match ids

`handlers()`, `listDeliveries({ handler })`, `HandlerContext.handler` and observer events show the new id for new deliveries. Update alerts that filter on the old id; query both ids while the 0.2.0 backlog drains.

## Step 3: transactions have a time budget

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

## Step 4: stop() returns a StopResult

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

## Step 5: renames: decorators and codec

Both old names keep working, are marked `@deprecated` and are removed in 1.0.0. Moving now means the editor shows no strikethrough and the upgrade to 1.0.0 is a no-op.

### Decorators

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

### `decode` becomes `codec`

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

## Step 6: error codes

- **Every relaybus error has a `code`.** `ErrorInfo` (the `lastError` of a delivery, the `error` of observer events) gains an optional `code` for relaybus errors; other errors are unchanged. Records written before 0.3.0 have no `code`, so read it as optional. The list is on [Error codes](/concepts/error-codes).
- **Five error classes report their own name.** `InvalidKafkaMessageError`, `RabbitMqUnavailableError`, `RabbitMqPublishRejectedError`, `UnroutableMessageError` and `MalformedMessageError` used to report `name: 'RelayBusError'`. Alerts that matched that name should match the class name or, better, the `code`.
- **MongoDB keeps the code.** The MongoDB store now stores `code` in the `lastError` of a delivery, as PostgreSQL always did (it stores `ErrorInfo` as JSON). Documents without `code` stay valid.
- **Broker dead letters carry more headers.** Kafka adds `relaybus-error-name` and `relaybus-error-code` to records copied to `<topic>.dead`; RabbitMQ adds `x-relaybus-dead-letter-error-name` and `x-relaybus-dead-letter-error-code`. The reason headers are unchanged.
- **`onFailure` has a new stage, `'backlog'`.** Add a case if you `switch` exhaustively over the stage.

## Step 7: optional new packages

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

## Checklist

- [ ] All `@relaybus/*` packages on 0.3.0
- [ ] Routes to one destination share `retry` and `timeout`
- [ ] Old route backlog drained, if cross-type order matters
- [ ] No transaction waits on a slow external call (or `transactionTimeout` raised on purpose)
- [ ] `stop()` callers handle `StopResult`; grace period larger than `shutdownTimeout`
- [ ] Dashboards read both route ids during the drain
