# @relaybus/core

@relaybus/core is the engine of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it writes events in the same database transaction as your data (the transactional outbox pattern) and delivers them to every handler with per-handler retries, leases, dead letters and ordering keys.

The relaybus engine: transactional publishing, per-handler deliveries, leased
dispatching, retries with jitter, dead letters, ordering keys and transport
routing. Most applications install it together with a store and, for NestJS,
the module:

```sh
pnpm add @relaybus/core @relaybus/nestjs @relaybus/postgres pg
```

| Package                                                                  | Use it for                                                    |
| ------------------------------------------------------------------------ | ------------------------------------------------------------- |
| [`@relaybus/nestjs`](https://www.npmjs.com/package/@relaybus/nestjs)     | `RelayBusModule` and the `@RelayBusConsumer` decorator        |
| [`@relaybus/postgres`](https://www.npmjs.com/package/@relaybus/postgres) | PostgreSQL store (pg, Prisma, TypeORM, Knex, Kysely, Drizzle) |
| [`@relaybus/mongodb`](https://www.npmjs.com/package/@relaybus/mongodb)   | MongoDB store (driver or Mongoose sessions)                   |
| [`@relaybus/kafka`](https://www.npmjs.com/package/@relaybus/kafka)       | Kafka transport between services                              |
| [`@relaybus/rabbitmq`](https://www.npmjs.com/package/@relaybus/rabbitmq) | RabbitMQ transport between services                           |
| [`@relaybus/testing`](https://www.npmjs.com/package/@relaybus/testing)   | Contract suite, fake clock, in-memory transport               |

## With NestJS

```ts
// order.events.ts
export class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

// app.module.ts
@Module({ imports: [RelayBusModule.forRoot({ store: new PostgresEventStore({ pool }) })] })
export class AppModule {}

// shipping.handlers.ts: consume with a decorator
@Injectable()
export class ShippingHandlers {
  @RelayBusConsumer(OrderPlaced, { retry: { attempts: 5 } })
  async ship(event: EventEnvelope<{ orderId: string }>) {
    /* runs after the order transaction commits */
  }
}

// order.service.ts: produce inside your transaction
await this.bus.transaction(async () => {
  await this.orders.insert(order);
  await this.bus.publish(new OrderPlaced({ orderId: order.id }), { key: order.id });
});
```

See the [project README](https://github.com/berkcansavur/relaybus#readme) for
guarantees and architecture.

## Without a framework

```ts
import { InMemoryEventStore, RelayBus, defineEvent } from '@relaybus/core';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const bus = new RelayBus({ store: new InMemoryEventStore(), source: 'orders' });

bus.subscribe(
  OrderPlaced,
  async (event, context) => {
    console.log(event.payload.orderId, context.attempt);
  },
  { id: 'orders.log' },
);

await bus.start();
await bus.transaction(async () => {
  await bus.publish(new OrderPlaced({ orderId: 'o-1' }), { key: 'o-1' });
});
```

Use a database store in production: [`@relaybus/postgres`](../postgres) or
[`@relaybus/mongodb`](../mongodb). `InMemoryEventStore` refuses to start when
`NODE_ENV=production` unless `allowInProduction: true`.

## `new RelayBus(options)`

| Option                                    | Default                                       | Description                                                                                                                                   |
| ----------------------------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `store`                                   | required                                      | `EventStore` implementation.                                                                                                                  |
| `source`                                  | `'relaybus'`                                  | Service name recorded on every event.                                                                                                         |
| `transactionMode`                         | `'required'`                                  | `'optional'` lets `publish` open its own transaction.                                                                                         |
| `transactionResolver`                     | —                                             | Returns the caller's active transaction, e.g. from `@nestjs-cls/transactional`.                                                               |
| `transactionTimeout`                      | `'5s'`                                        | Budget of every `bus.transaction`; the database enforces it where it can, an overrun rolls back with `TransactionTimeoutError`.               |
| `unroutedEvents`                          | `'warn'`                                      | `'throw'` rejects events without any handler or route.                                                                                        |
| `retry`                                   | 10 attempts, `1s`, ×2, max `10m`, full jitter | Default handler retry policy. `attempts: Infinity` retries forever.                                                                           |
| `handlerTimeout`                          | `false`                                       | Default handler timeout.                                                                                                                      |
| `dispatcher.enabled`                      | `true`                                        | Run handlers in this process.                                                                                                                 |
| `dispatcher.concurrency`                  | `10`                                          | Handlers running at once.                                                                                                                     |
| `dispatcher.batchSize`                    | concurrency                                   | Deliveries claimed per query.                                                                                                                 |
| `dispatcher.pollInterval`                 | `'1s'`                                        | Idle wait between claims; retries are picked up on this cadence.                                                                              |
| `dispatcher.leaseDuration`                | `'30s'`                                       | Lease length, renewed every third while a handler runs.                                                                                       |
| `dispatcher.shutdownTimeout`              | `'30s'`                                       | Longest `stop` takes in all (default of `stop({ timeout })`); handlers are aborted and released at four fifths of it.                         |
| `dispatcher.completionBatchWindow`        | `'2ms'`                                       | Deliveries that finish within this window are completed with one store call; `0` completes one by one.                                        |
| `dispatcher.completionBatchSize`          | `200`                                         | Most deliveries completed in one store call.                                                                                                  |
| `retention.completed`                     | `'7d'`                                        | Delete events whose deliveries all completed after this; also the deduplication horizon. A completed delivery is deleted at once (see below). |
| `retention.dead`                          | `false`                                       | Delete dead letters after this.                                                                                                               |
| `retention.interval`                      | `'1h'`                                        | Cleanup cadence.                                                                                                                              |
| `transports` / `routes` / `subscriptions` | —                                             | Broker integration, see below.                                                                                                                |
| `setupStore`                              | `true`                                        | Create tables and indexes on `start`.                                                                                                         |
| `observers`                               | `[]`                                          | Lifecycle hooks for logs, metrics and tracing (see Observability below).                                                                      |
| `tracer`                                  | none                                          | Spans around publish, send and handler runs; writes `traceparent`/`tracestate` into published events. `@relaybus/otel` provides one.          |
| `backlogInterval`                         | off                                           | Reads `statistics()` this often while running and reports it to `onBacklog`.                                                                  |
| `logger`                                  | `console`                                     | `{ log, warn, error }`.                                                                                                                       |
| `clock`, `generateIdentifier`, `random`   | system                                        | Replace in tests. A clock with `schedule` also runs the bus's timers.                                                                         |

Durations are milliseconds or strings such as `'250ms'`, `'30s'`, `'5m'`, `'1h'`, `'7d'`.

**How the dispatcher schedules claims.** A claim that fills its batch is followed by the next one at once. Otherwise the
dispatcher sleeps until it is woken: by a publish, by the store reporting new deliveries from any process (the PostgreSQL
store through `LISTEN/NOTIFY`, the MongoDB store through a change stream), or by settled deliveries once 70% of a batch
(of `min(concurrency, batchSize)`) is free again, so slots are refilled in groups rather than one by one. A smaller number
of free slots is refilled after 10 ms without a claim, and the poll interval stays the safety net. Wake-ups within 5 ms
of a claim are merged into one claim. Settled unkeyed deliveries do not trigger a claim when their handler's last claim
got less than it asked for (nothing more was due); a settled keyed delivery always may, since it unblocks the next of
its key.

## Publishing

`publish(event, options)` and `publishAll(events, options)` write into, in
order: `options.transaction`, the transaction of `bus.transaction()`, then
`transactionResolver`. Options: `key`, `delay`, `headers`, `correlationId`,
and `id` on `publish` for idempotent publishing. Register every handler in each
process that publishes the event: deliveries are created at publish time.

Declare the ordering key on the event to apply it to every publish: `defineEvent('order.placed.v1',
{ schema, key: (payload) => payload.orderId })` (or `defineEvent<Payload>(type, { key })`). The
function is typed from the payload, so a wrong field fails to compile. An explicit `key` option
overrides it; in `publishAll` an explicit key applies to every event of the call.

## Handlers

`subscribe(event, handler, { id, retry, timeout, concurrency, transactional })`.
The handler receives the envelope and a context with `attempt`, `maxAttempts`,
`deliveryId`, `signal` and, for transactional handlers, `transaction`.

- Throw `NonRetryableError` to dead-letter at once. Schema failures are never retried.
- `transactional: true` runs the handler in a store transaction that also completes
  the delivery. It must finish within the lease duration or its `timeout`.
- After a timeout or lease loss the handler's `signal` is aborted; the retry waits
  for the abandoned run to settle so runs never overlap.

## Event catalogs

A catalog groups the events of one domain so the compiler checks that every event
has a handler and that only those events are published through it.

```ts
class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string; carrier: string }>('order.shipped.v1') {}

export const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });

// A class that misses a handler does not compile:
class ShippingHandlers implements HandlersOf<typeof OrderEvents> {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>, context: HandlerContext) {}
  onOrderShipped(event: EventOf<typeof OrderShipped>) {}
}
bus.subscribeCatalog(OrderEvents, new ShippingHandlers()); // ids: OrderEvents.onOrderPlaced, ...

// Or declare the handlers as an object; missing or extra keys do not compile:
bus.subscribeCatalog(
  OrderEvents,
  defineHandlers(OrderEvents, {
    onOrderPlaced: (event) => reserveStock(event.payload.orderId),
    onOrderShipped: (event) => notify(event.payload.carrier),
  }),
  { idPrefix: 'fulfilment' },
);

// Publish through a view that only accepts the catalog's events:
const orders = bus.forCatalog(OrderEvents);
await orders.publish(new OrderPlaced({ orderId }), { transaction });
await orders.publish(new UserRegistered({ userId })); // compile error
```

- `defineCatalog(events, { name? })` returns a frozen catalog with frozen entries. It rejects
  empty catalogs, entries that are not event classes, names that are not identifiers of letters,
  digits and underscores (a name becomes part of a method name and a handler id) and two entries
  with one event type.
- `HandlersOf<C>` requires an `on${Key}` method per event; `HandlersOf<C, 'A' | 'B'>` requires
  only those (`PartialHandlersOf<C, 'A' | 'B'>` is the same type), and `HandlersOf<C, K, Tx>` types
  `context.transaction`. `EventOf<typeof OrderPlaced>` is the envelope type of one event;
  `CatalogEvents<C, K?>` is the union of the catalog's (selected) events.
- `bus.subscribeCatalog(catalog, handlers, { events?, idPrefix?, ...subscribeOptions })` registers
  the `on${Key}` method of every event (or of the `events` listed) with the stable id
  `${idPrefix ?? catalog name ?? class name}.on${Key}` and `this` bound to `handlers`. Methods of
  events that are not listed are ignored. A missing method does not compile and throws a
  `ConfigurationError` naming it. Handlers are narrowed only by an `events` list the compiler can see;
  options whose `events` may be absent require every handler.
- `defineHandlers(catalog, handlers, { events? })` checks completeness, extra keys (including
  handlers of events that are not listed) and payload types at compile time and at runtime. A
  handler may type its context, for example `HandlerContext<PoolClient>`.
- **Boot check:** `new RelayBus({ catalogs: [OrderEvents] })` (or `bus.registerCatalog`) makes
  `start()` throw a `ConfigurationError` listing every catalog event that has no handler or route
  and the method it expects. A failed check leaves the bus idle, before any I/O, so it can be fixed
  and started again. For services that share a catalog, declare the role instead; see below.
- `forCatalog(...).publish` also checks the event type at runtime. TypeScript compares event
  classes by shape, so two events with identical payload types are interchangeable to the compiler;
  the runtime check still rejects the one that is not in the catalog, before anything is stored.

### Microservices: shared catalogs

In a system of services the catalog usually lives in a shared contracts package, and each service
produces some of its events and consumes others. Declare what this service does with each part:

```ts
import { consume, produce } from '@relaybus/core';
import { OrderEvents } from '@acme/contracts';

// orders service: publishes OrderPlaced to the broker, handles OrderShipped
class DeliveryStatusHandlers implements HandlersOf<typeof OrderEvents, 'OrderShipped'> {
  onOrderShipped(event: EventOf<typeof OrderShipped>) {}
}

const bus = new RelayBus({
  store,
  transports: [kafka],
  routes: [{ event: OrderPlaced, transport: 'kafka', destination: 'orders' }],
  subscriptions: [{ transport: 'kafka', destinations: ['shipments'] }],
  catalogs: [
    produce(OrderEvents, { events: ['OrderPlaced'] }),
    consume(OrderEvents, { events: ['OrderShipped'] }),
  ],
});
bus.subscribeCatalog(OrderEvents, new DeliveryStatusHandlers(), { events: ['OrderShipped'] });
const orders = bus.forCatalog(OrderEvents, { events: ['OrderPlaced'] });
await orders.publish(new OrderPlaced({ orderId }), { transaction, key: orderId });
await orders.publish(new OrderShipped({ orderId, carrier })); // compile error: not produced here

// shipping service: handles OrderPlaced only
class ShipmentHandlers implements HandlersOf<typeof OrderEvents, 'OrderPlaced'> {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
}
```

- `consume(catalog, { events? })`: each listed event (default: all) needs a **local handler**;
  a route does not count. `produce(catalog, { events? })`: each listed event needs a **route to a
  transport**, so nothing is published without a destination; a local handler does not count.
  Both can be declared for one catalog, and for one event. Unknown names do not compile and are
  rejected at runtime, as are empty and duplicate lists. A bare catalog keeps its meaning: every
  event needs a handler or a route.
- `start()` lists every gap, per event:

  ```text
  ConfigurationError: Catalog events this service does not cover:
    - OrderEvents.OrderShipped (order.shipped.v1): consumed here, needs handler method onOrderShipped
    - OrderEvents.OrderPlaced (order.placed.v1): produced here, needs a route to a transport
  ```

- `bus.forCatalog(catalog, { events })` publishes only the listed events: others do not compile
  and are rejected at runtime before anything is stored.
- Received events (`bus.receive`, called by Kafka and RabbitMQ consumers) are matched to the
  catalog handlers by event type, idempotently on the event id, and the payload is validated
  against the event's schema before the handler runs; a payload that fails it is dead-lettered.
- Per-key order on the wire holds **per destination**: every route to one destination of one
  transport shares one queue per key, so `OrderPlaced` and `OrderCancelled` of one key reach the
  broker in publish order even when a send of the first is retried (Kafka: same partition;
  RabbitMQ: `ordered: true`). On the consuming side order holds **per handler**: two handlers of
  two types run independently and can interleave.

## Transports

```ts
new RelayBus({
  store,
  transports: [kafka],
  routes: [{ event: OrderPlaced, transport: 'kafka', destination: 'orders.events' }],
  subscriptions: [{ transport: 'kafka', destinations: ['payments.events'] }],
});
```

Routes are deliveries like any handler: retried until the broker accepts them
(unlimited attempts by default) with a 30s publish timeout. Subscriptions call
`bus.receive(envelope)`, which stores the event once by id for local handlers;
transports acknowledge only after it resolves and dead-letter `InvalidEnvelopeError`.

Every route to one destination of one transport is **one handler**, with the id
`relaybus.transport:<transport>:<destination>` (`handlers()` lists it once per routed
event type). Its deliveries of one key are sent one at a time in commit order, whatever
their event types: while the oldest is retried, the later ones of that key wait until it
is sent or dead-lettered (a dead-lettered head releases them, as for any handler). Other
keys and unkeyed events are not held back. Because the routes share one queue, they must
resolve to the same `retry` and `timeout`; `new RelayBus` throws a `ConfigurationError`
when they differ, and when one event type is routed to the same destination twice. Each
delivery is one transport call: a hot key is sent one event per round trip (sending a run
of one key's deliveries in one ordered call is a possible follow-up, not implemented).

### Upgrading from 0.2.0

0.2.0 registered one route per event type, with the id
`relaybus.transport:<transport>:<destination>:<eventType>`, so events of one key but two
types could reach the broker out of order. 0.3.0 groups them under
`relaybus.transport:<transport>:<destination>`. Nothing has to be migrated: deliveries
stored under a 0.2.0 id, including those written by 0.2.0 processes during a rolling
deploy, are still claimed and sent by the new route; the store rows keep their old id.

- **Order during the upgrade.** A delivery under a 0.2.0 id and one under the new id are
  two queues, so one key's backlog from 0.2.0 is not ordered against events published
  after the upgrade. Where that matters, let the route backlog drain first
  (`listDeliveries({ handler: '<0.2.0 id>', status: 'pending' })` returns nothing) and
  stop 0.2.0 publishers before new ones start.
- **Routes to one destination with different `retry` or `timeout`** now fail at
  construction; give them the same settings.
- Code or dashboards that match route ids (`handlers()`, `listDeliveries({ handler })`,
  `HandlerContext.handler`, observer events) see the new id for new deliveries.

## Operations

`listDeliveries(query)`, `requeue(ids)`, `statistics()`, `purgeExpired()`,
`handlers()`, `runOnce()` and `drain()` (tests), `start()`, `stop()` and `wake()`.

**`wake()` after your own commit.** A publish in relaybus' own transaction wakes the dispatcher right after its
commit. A publish in your transaction commits at a time the bus cannot see: unless the store reports the commit
(PostgreSQL with `LISTEN/NOTIFY`), the dispatcher is woken 25, 100 and 250 ms after the latest publish and otherwise
finds the delivery at its next poll. Call `bus.wake()` right after such a commit to claim at once.

**`stop({ timeout })` is bounded.** The whole shutdown (stop claiming, wait for running
handlers, hand back leases, close transports and the store) takes at most `timeout`, default
`dispatcher.shutdownTimeout` (30 s). Running handlers get four fifths of it; then their
`context.signal` aborts and their deliveries are released for another process. If the deadline
itself passes (a hung query, a broker that does not answer), `stop` gives up waiting, force-closes
what relaybus opened itself (the pool a store created from `connectionString`, its listening
connection, a RabbitMQ transport's connection) and resolves; it never closes a pool or client you
passed in, and never rejects because of the deadline. It resolves with a `StopResult`:

```ts
const { graceful, abandonedDeliveries, durationMs } = await bus.stop({ timeout: '25s' });
```

`graceful` is false when the deadline passed, and a warning is logged
(`[RelayBus](stop) Shutdown deadline passed, forced close | abandoned=1 | timeoutMs=25000`).
Abandoned deliveries keep their lease until it expires (`dispatcher.leaseDuration`), then another
process runs them again: at-least-once still holds, so keep handlers idempotent. Kafka's
`disconnect` has no forced variant; past the deadline the bus simply stops waiting for it.

**Observability.** Besides the lifecycle hooks, observers can implement timed hooks:
`onPublishFinished` (event types, duration, outcome), `onClaimFinished` (requested, claimed,
lanes, duration), `onHandlerStarted` (end-to-end latency from `occurredAt`),
`onHandlerFinished` (duration, outcome, error with its code), `onTransportSent` (transport,
system, destination, duration, outcome), `onReceiveFinished` (transport, destination, source,
trace context; `accepted`, `duplicate`, `ignored`, `invalid` or `error`), `onInvalidMessage`
(a transport could not decode an inbound message, so it never reached the bus: transport,
destination, reason) and `onBacklog`
(pending deliveries and age of the oldest due one, every `backlogInterval`). A hook no observer
implements costs nothing, not even a clock read. Hooks run synchronously and must not throw; a
throwing hook is reported to `onFailure` with stage `observer`. `traceContextOf(event.headers)`
returns the W3C trace context an event carries. `@relaybus/otel` maps all of it to OpenTelemetry.

**Completed deliveries are deleted.** Completing a delivery removes its row instead
of updating it, which is what keeps the write cost of a delivery constant (see
`docs/performance/write-amplification.md`). `listDeliveries({ status: 'completed' })`
and `statistics().completed` therefore report nothing unless the store keeps a
completion history (`completionHistory: true` on the SQL stores,
`retainCompleted: true` on `InMemoryEventStore`). Deduplication does not depend
on it: an event remembers every handler it ever had a delivery for, so a
redelivered or republished event never runs a completed handler again, for as
long as the event is kept (`retention.completed`).

## Error codes

Every relaybus error has a stable, machine-readable `code` next to its `name`. Codes are
never renamed or reused once released, so branch on them in alerts and retry rules. The
`lastError` of a dead or retried delivery and the `error` of observer events carry the
`code` of relaybus errors; other errors keep `name`, `message` and `stack` only.

| Code                                       | Error class                     | Package              |
| ------------------------------------------ | ------------------------------- | -------------------- |
| `RELAYBUS_ERROR`                           | `RelayBusError`                 | `@relaybus/core`     |
| `RELAYBUS_CONFIGURATION`                   | `ConfigurationError`            | `@relaybus/core`     |
| `RELAYBUS_TRANSACTION_REQUIRED`            | `TransactionRequiredError`      | `@relaybus/core`     |
| `RELAYBUS_TRANSACTION_TIMEOUT`             | `TransactionTimeoutError`       | `@relaybus/core`     |
| `RELAYBUS_NON_RETRYABLE`                   | `NonRetryableError`             | `@relaybus/core`     |
| `RELAYBUS_EVENT_VALIDATION`                | `EventValidationError`          | `@relaybus/core`     |
| `RELAYBUS_INVALID_ENVELOPE`                | `InvalidEnvelopeError`          | `@relaybus/core`     |
| `RELAYBUS_LEASE_LOST`                      | `LeaseLostError`                | `@relaybus/core`     |
| `RELAYBUS_HANDLER_TIMEOUT`                 | `HandlerTimeoutError`           | `@relaybus/core`     |
| `RELAYBUS_DISPATCHER_STOPPED`              | `DispatcherStoppedError`        | `@relaybus/core`     |
| `RELAYBUS_ATTEMPTS_EXHAUSTED`              | `AttemptsExhaustedError`        | `@relaybus/core`     |
| `RELAYBUS_LIFECYCLE`                       | `LifecycleError`                | `@relaybus/core`     |
| `RELAYBUS_DRAIN_INCOMPLETE`                | `DrainIncompleteError`          | `@relaybus/core`     |
| `RELAYBUS_KAFKA_INVALID_MESSAGE`           | `InvalidKafkaMessageError`      | `@relaybus/kafka`    |
| `RELAYBUS_KAFKA_UNENCODABLE_EVENT`         | `UnencodableEventError`         | `@relaybus/kafka`    |
| `RELAYBUS_KAFKA_PUBLISH_TIMEOUT`           | `PublishTimeoutError`           | `@relaybus/kafka`    |
| `RELAYBUS_KAFKA_HANDLER_FAILED`            | `HandlerFailedError`            | `@relaybus/kafka`    |
| `RELAYBUS_KAFKA_TRANSPORT_CLOSED`          | `TransportClosedError`          | `@relaybus/kafka`    |
| `RELAYBUS_KAFKA_GROUP_JOIN_TIMEOUT`        | `GroupJoinTimeoutError`         | `@relaybus/kafka`    |
| `RELAYBUS_POSTGRES_RECONCILE_FAILED`       | `ReconcileFailedError`          | `@relaybus/postgres` |
| `RELAYBUS_RABBITMQ_UNAVAILABLE`            | `RabbitMqUnavailableError`      | `@relaybus/rabbitmq` |
| `RELAYBUS_RABBITMQ_PUBLISH_REJECTED`       | `RabbitMqPublishRejectedError`  | `@relaybus/rabbitmq` |
| `RELAYBUS_RABBITMQ_UNROUTABLE_MESSAGE`     | `UnroutableMessageError`        | `@relaybus/rabbitmq` |
| `RELAYBUS_RABBITMQ_MALFORMED_MESSAGE`      | `MalformedMessageError`         | `@relaybus/rabbitmq` |
| `RELAYBUS_RABBITMQ_UNSERIALIZABLE_PAYLOAD` | `UnserializablePayloadError`    | `@relaybus/rabbitmq` |
| `RELAYBUS_TESTING_CODEC_CONTRACT`          | `CodecContractError`            | `@relaybus/testing`  |
| `RELAYBUS_RABBITMQ_UNENCODABLE_EVENT`      | `RabbitMqUnencodableEventError` | `@relaybus/rabbitmq` |
| `RELAYBUS_RABBITMQ_AMQP_LIMIT_EXCEEDED`    | `AmqpLimitExceededError`        | `@relaybus/rabbitmq` |

An error class you derive from `RelayBusError` inherits `RELAYBUS_ERROR` unless it sets
its own `code`. See [Error codes](https://berkcansavur.github.io/relaybus/concepts/error-codes) for what each one means.

## Writing a store

Implement `EventStore` and run `eventStoreContract` from
[`@relaybus/testing`](../testing) against a real database. The contract defines
fencing, ordering, commit-order sequencing, visibility and purge semantics.

## When to use @relaybus/core, and when not to

Use it directly when you are not on NestJS, or when you write a custom `EventStore` or transport. Do not run it alone in production: add a store (`@relaybus/postgres` or `@relaybus/mongodb`). `InMemoryEventStore` refuses to start when `NODE_ENV=production`.

Documentation: <https://berkcansavur.github.io/relaybus/packages/core>

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

## License

MIT
