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
| [`@relaybus/nestjs`](https://www.npmjs.com/package/@relaybus/nestjs)     | `RelayBusModule` and the `@Subscribe` decorator               |
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
  @Subscribe(OrderPlaced, { retry: { attempts: 5 } })
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

| Option                                    | Default                                       | Description                                                                     |
| ----------------------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------- |
| `store`                                   | required                                      | `EventStore` implementation.                                                    |
| `source`                                  | `'relaybus'`                                  | Service name recorded on every event.                                           |
| `transactionMode`                         | `'required'`                                  | `'optional'` lets `publish` open its own transaction.                           |
| `transactionResolver`                     | —                                             | Returns the caller's active transaction, e.g. from `@nestjs-cls/transactional`. |
| `unroutedEvents`                          | `'warn'`                                      | `'throw'` rejects events without any handler or route.                          |
| `retry`                                   | 10 attempts, `1s`, ×2, max `10m`, full jitter | Default handler retry policy. `attempts: Infinity` retries forever.             |
| `handlerTimeout`                          | `false`                                       | Default handler timeout.                                                        |
| `dispatcher.enabled`                      | `true`                                        | Run handlers in this process.                                                   |
| `dispatcher.concurrency`                  | `10`                                          | Handlers running at once.                                                       |
| `dispatcher.batchSize`                    | concurrency                                   | Deliveries claimed per query.                                                   |
| `dispatcher.pollInterval`                 | `'1s'`                                        | Idle wait between claims; retries are picked up on this cadence.                |
| `dispatcher.leaseDuration`                | `'30s'`                                       | Lease length, renewed every third while a handler runs.                         |
| `dispatcher.shutdownTimeout`              | `'30s'`                                       | Wait for running handlers on `stop`, then release them.                         |
| `retention.completed`                     | `'7d'`                                        | Delete completed deliveries after this; also the deduplication horizon.         |
| `retention.dead`                          | `false`                                       | Delete dead letters after this.                                                 |
| `retention.interval`                      | `'1h'`                                        | Cleanup cadence.                                                                |
| `transports` / `routes` / `subscriptions` | —                                             | Broker integration, see below.                                                  |
| `setupStore`                              | `true`                                        | Create tables and indexes on `start`.                                           |
| `observers`                               | `[]`                                          | Lifecycle hooks for logs, metrics and tracing.                                  |
| `logger`                                  | `console`                                     | `{ log, warn, error }`.                                                         |
| `clock`, `generateIdentifier`, `random`   | system                                        | Replace in tests.                                                               |

Durations are milliseconds or strings such as `'250ms'`, `'30s'`, `'5m'`, `'1h'`, `'7d'`.

## Publishing

`publish(event, options)` and `publishAll(events, options)` write into, in
order: `options.transaction`, the transaction of `bus.transaction()`, then
`transactionResolver`. Options: `key`, `delay`, `headers`, `correlationId`,
and `id` on `publish` for idempotent publishing. Register every handler in each
process that publishes the event: deliveries are created at publish time.

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

- `defineCatalog(events, { name? })` returns a frozen catalog. It rejects empty
  catalogs, entries that are not event classes, names that are not identifiers and
  two entries with one event type.
- `HandlersOf<C>` requires an `on${Key}` method per event; `PartialHandlersOf<C, 'A' | 'B'>`
  names a subset for consumers that handle only some events. `EventOf<typeof OrderPlaced>`
  is the envelope type of one event; `CatalogEvents<C>` is the union of the catalog's events.
- `bus.subscribeCatalog(catalog, handlers, { idPrefix?, ...subscribeOptions })` registers every
  `on${Key}` method with the stable id `${idPrefix ?? catalog name ?? class name}.on${Key}` and
  `this` bound to `handlers`. A missing method throws a `ConfigurationError` naming it.
- **Boot check:** `new RelayBus({ catalogs: [OrderEvents] })` (or `bus.registerCatalog`) makes
  `start()` throw a `ConfigurationError` listing every catalog event that has no handler or route
  and the method it expects. A failed check leaves the bus idle, before any I/O, so it can be fixed
  and started again.
- `forCatalog(...).publish` also checks the event type at runtime. TypeScript compares event
  classes by shape, so two events with identical payload types are interchangeable to the compiler;
  the runtime check still rejects the one that is not in the catalog, before anything is stored.

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

## Operations

`listDeliveries(query)`, `requeue(ids)`, `statistics()`, `purgeExpired()`,
`handlers()`, `runOnce()` and `drain()` (tests), `start()` and `stop()`.

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
