# @relaybus/nestjs

@relaybus/nestjs is the NestJS module of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: declare handlers with `@RelayBusConsumer` and publish inside your database transaction, and every committed event reaches every handler at least once.

NestJS integration for [relaybus](../core): one module, one decorator, correct lifecycle.

## Install

```bash
npm install @relaybus/core @relaybus/nestjs
```

Peer dependencies: `@nestjs/common` and `@nestjs/core` (`^11 || ^12`), `reflect-metadata`, `rxjs`.

## Quick start

```ts
// app.module.ts
@Module({
  imports: [RelayBusModule.forRoot({ store: new PostgresEventStore({ pool }), source: 'orders' })],
})
export class AppModule {}

// shipping.handlers.ts - any provider of any module
@Injectable()
export class ShippingHandlers {
  @RelayBusConsumer(OrderPlaced)
  async onOrderPlaced(event: EventEnvelope<{ orderId: string }>) {
    /* ... */
  }
}

// order.service.ts
@Injectable()
export class OrderService {
  constructor(
    private readonly bus: RelayBus,
    private readonly transactions: RelayBusTransactionHost<PoolClient>,
  ) {}

  @RelayBusTransaction()
  async place(orderId: string): Promise<void> {
    await this.transactions.current().query('INSERT INTO orders (id) VALUES ($1)', [orderId]);
    await this.bus.publish(new OrderPlaced({ orderId })); // joins the transaction
  }
}
```

That is all: the module is global, the handler is discovered, the bus starts after bootstrap and
stops on shutdown. Inject the bus anywhere with `constructor(private readonly bus: RelayBus) {}`;
add the transaction type when you want it typed: `RelayBus<PoolClient>` (the generic is erased at
runtime, so DI is unaffected).

Call `app.enableShutdownHooks()` in `main.ts` so SIGTERM/SIGINT close the app and let the bus
drain running handlers (`dispatcher.shutdownTimeout`, default 30s).

## `RelayBusModule`

- `forRoot(options)`
- `forRootAsync({ imports?, inject?, useFactory })`, or `useClass` / `useExisting` (a class with
  `create(): RelayBusModuleOptions | Promise<RelayBusModuleOptions>`, see `RelayBusModuleOptionsFactory`)

Both accept `isGlobal` next to the options. It defaults to `true`; with `false`, import the module
wherever `RelayBus` is injected. `RelayBus` and `RelayBusHealth` are exported.

### Options

Everything of core's `RelayBusOptions` passes through unchanged, except `logger`: the bus always
logs through a Nest `Logger` with context `RelayBus`, so `app.useLogger(...)` controls it.

| Option                                        | Default                   | Notes                                                                            |
| --------------------------------------------- | ------------------------- | -------------------------------------------------------------------------------- |
| `store`                                       | required                  | An `EventStore`: `@relaybus/postgres`, `@relaybus/mongodb`, `InMemoryEventStore` |
| `source`                                      | `'relaybus'`              | Service name recorded on every event; set it to your app name                    |
| `transactionMode`                             | `'required'`              | `'optional'` lets `publish` open its own transaction                             |
| `transactionResolver`                         | none                      | Finds the caller's active transaction (see below)                                |
| `transactionTimeout`                          | `'5s'`                    | Budget of every `bus.transaction` / `@RelayBusTransaction()` (see below)         |
| `retry`                                       | core default              | Default retry policy of every handler and route                                  |
| `handlerTimeout`                              | `false`                   | Default handler timeout                                                          |
| `dispatcher.enabled`                          | `true`                    | `false` on publish-only processes (API pods, scripts)                            |
| `dispatcher.concurrency`                      | `10`                      | Handlers running at once                                                         |
| `dispatcher.batchSize`                        | concurrency               | Deliveries claimed per query                                                     |
| `dispatcher.pollInterval`                     | `'1s'`                    | Wait between claims when idle                                                    |
| `dispatcher.leaseDuration`                    | `'30s'`                   | Renewed every third of it                                                        |
| `dispatcher.shutdownTimeout`                  | `'30s'`                   | How long shutdown waits for running handlers                                     |
| `retention.completed` / `.dead` / `.interval` | `'7d'` / `false` / `'1h'` | Cleanup of finished deliveries                                                   |
| `transports`, `routes`, `subscriptions`       | none                      | Broker fan-out and fan-in (`@relaybus/kafka`, `@relaybus/rabbitmq`)              |
| `setupStore`                                  | `true`                    | Create tables/indexes on start                                                   |
| `observers`                                   | none                      | Metrics and tracing hooks                                                        |
| `clock`, `generateIdentifier`, `random`       | system                    | Mainly for tests                                                                 |
| `isGlobal`                                    | `true`                    | Module-level, see above                                                          |

## `@RelayBusConsumer(event, options?)`

The default way to handle events: put `@RelayBusConsumer` on any method of any provider.

```ts
@Injectable()
export class ShippingHandlers {
  @RelayBusConsumer(OrderPlaced)
  async shipOrder(event: EventEnvelope<{ orderId: string }>) {}

  @RelayBusConsumer(UserRegistered, { id: 'shipping.welcome-pack', retry: { attempts: 5 } })
  async sendWelcomePack(event: EventOf<typeof UserRegistered>, context: HandlerContext) {}
}
```

```ts
@RelayBusConsumer(OrderPlaced)                                   // class
@RelayBusConsumer('order.placed.v1', { concurrency: 2 })         // type string
@RelayBusConsumer(OrderPlaced, { id: 'billing.invoice', transactional: true, timeout: '10s' })
```

- **No class-level setup.** No catalog, interface or base class: method names are free, and one
  class may handle events of different catalogs (or of none).
- **Typed at compile time.** With a class argument the first parameter must be the envelope of
  that event: `@RelayBusConsumer(OrderPlaced) ship(event: EventEnvelope<{ userId: number }>)` does
  not compile.
- Options are core's `SubscribeOptions` with `id` optional. Methods receive `(envelope, context)`.

**Names.** The relaybus decorators share the `RelayBus` prefix (`@RelayBusConsumer`,
`@RelayBusCatalogConsumer`, `@RelayBusTransaction`), so they never collide with `@nestjs/event-emitter`'s
`@OnEvent` or `@nestjs/cqrs`. `@Subscribe` and `@EventConsumer` are the earlier names of
`@RelayBusConsumer` and `@RelayBusCatalogConsumer`: they still work, are the same functions, are marked
`@deprecated` (editors strike them through) and are removed in 1.0.0. `@RelayBusConsumer` only
compiles on a method and takes an event; `@RelayBusCatalogConsumer` only compiles on a class and
takes a catalog.

Rules, all enforced at boot (the application fails to start, no silent skips):

- **Stable ids.** The default id is `ClassName.methodName`. Deliveries are stored under the id, so
  renaming the class or method orphans pending deliveries; pin `{ id }` on handlers that matter.
- **Duplicate ids** fail init with both locations named. A method with several `@RelayBusConsumer`
  decorators must give each an explicit `id`.
- **Singletons only.** Handlers on request-scoped or transient providers (or default-scoped
  providers that depend on one) are rejected: deliveries run outside any request, so there is no
  request instance to call. This covers class, `useClass` and `useFactory` providers (a factory is
  checked through its class token; a request-scoped factory under a string token cannot be
  inspected). Providers without `@RelayBusConsumer` are never inspected for scope.
- `this` is bound to the provider instance. Only providers are scanned, in every module. A handler
  reachable through several tokens (`useExisting`) is registered once. Declarations are stored per
  class and method, so decorators that wrap the method later do not drop them, and subclasses
  inherit the handlers of their base class.

## Module contracts: `RelayBusModule.forFeature(...)`

The recommended way to use catalogs in NestJS. A feature module states which events it consumes
and produces; the method-level `@RelayBusConsumer` handlers of its providers fulfil the contract,
spread over any classes and method names.

```ts
@Module({
  imports: [
    RelayBusModule.forFeature({
      consumes: [OrderEvents], // or consume(OrderEvents, { events: ['OrderPlaced'] })
      produces: [produce(BillingEvents, { events: ['InvoiceIssued'] })],
    }),
  ],
  providers: [ChargeHandlers, RefundHandlers],
})
export class BillingModule {}
```

When the application initialises:

- every consumed event needs a `@RelayBusConsumer` method (or a `@RelayBusCatalogConsumer` class)
  among the providers **of that module**; consumers in other modules do not count;
- every produced event needs a route to a transport (`routes` of `forRoot`).

Otherwise the application fails to start with one line per gap, for example
`[RelayBusHandlerExplorer](assertModuleContracts) BillingModule: OrderEvents.OrderCancelled (order.cancelled.v1) has no consumer`.
`forRoot({ catalogs })` keeps working as the application-wide check (handlers anywhere).

## Stricter checking (optional): catalogs and `@RelayBusCatalogConsumer(catalog, options?)`

Method-level handlers are enough for most services. When a team wants the compiler and the boot
to prove that _every_ event of a shared contract is handled, group the events in a catalog and
let a class consume it. Everything here is optional and can be added later, next to
`@RelayBusConsumer` methods.

```ts
export const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });

@Injectable()
@RelayBusCatalogConsumer(OrderEvents, { transactional: true })
export class ShippingHandlers implements HandlersOf<typeof OrderEvents> {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>, context: HandlerContext) {}
  onOrderShipped(event: EventOf<typeof OrderShipped>) {}
}

RelayBusModule.forRoot({ store, catalogs: [OrderEvents] });
```

Registers every `on${EventName}` method the class defines, under the stable id
`ClassName.on${EventName}`, with `this` bound to the provider. `implements HandlersOf<...>` makes
the compiler reject a class that misses a handler; `PartialHandlersOf<C, 'OrderShipped'>`
documents a deliberate subset. Options are core's `SubscribeOptions` without `id`, applied to every
handler of the class.

- `catalogs: [OrderEvents]` in the module options is the boot check: if no provider (via
  `@RelayBusCatalogConsumer`, `@RelayBusConsumer` or a configured route) handles an event, the application fails to
  start with the list of missing events and the methods they expect.
- It works next to `@RelayBusConsumer` in the same class. A `@RelayBusConsumer` that would take the same default
  id (`@RelayBusConsumer(OrderPlaced) onOrderPlaced()`) fails init naming both; give it an explicit `id`.
- Same rules as `@RelayBusConsumer`: singleton providers only, `useExisting` aliases register once,
  subclasses inherit the decoration and use their own class name in ids.
- A class decorated with `@RelayBusCatalogConsumer` that defines none of the catalog's methods fails init,
  which catches misspelled method names.
- Declarations are inherited from base classes; a subclass that declares the same catalog again
  replaces the inherited declaration, and one catalog declared twice on one class throws.

### Microservices: consuming part of a shared catalog

```ts
// shipping service
@Injectable()
@RelayBusCatalogConsumer(OrderEvents, { events: ['OrderPlaced'] })
export class ShippingHandlers {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
}

RelayBusModule.forRoot({
  store,
  transports: [rabbit],
  subscriptions: [{ transport: 'rabbit', destinations: ['orders'] }],
  routes: [{ event: OrderShipped, transport: 'rabbit', destination: 'shipments' }],
  catalogs: [
    consume(OrderEvents, { events: ['OrderPlaced'] }),
    produce(OrderEvents, { events: ['OrderShipped'] }),
  ],
});
```

- With `events`, exactly those methods are registered (other `on${EventName}` methods are
  ignored), and a class that misses one of them does not compile and fails init.
- `consume(...)` fails the boot when a consumed event has no handler in the application;
  `produce(...)` fails it when a produced event has no route. See `@relaybus/core`, "Microservices:
  shared catalogs".

## Lifecycle

1. `onModuleInit`: handlers are discovered and registered on the bus.
2. `onApplicationBootstrap`: `bus.start()` creates the schema (unless `setupStore: false`),
   connects transports and starts the dispatcher. If it rejects (database down, broker
   unreachable) the application does not boot.
3. `beforeApplicationShutdown`: `bus.stop()` stops claiming, waits for running handlers up to
   `shutdownTimeout`, then closes transports and the store. Needs `app.enableShutdownHooks()` for
   signals; `app.close()` runs it regardless. Nest runs `onApplicationShutdown` hooks (where
   TypeORM, Mongoose and most pools disconnect) after this, so handlers still have their
   connections while draining. If your own provider disconnects in `onModuleDestroy` (common with
   Prisma), move it to `onApplicationShutdown`.

## Transactions

### `@RelayBusTransaction()`

Runs an async method in a relaybus transaction. Every `bus.publish(...)` inside it, and inside
everything it awaits, joins the transaction without `{ transaction }`; the transaction commits when
the method resolves and rolls back when it throws.

```ts
// Before: the transaction is threaded through by hand
async place(orderId: string): Promise<void> {
  await this.bus.transaction(async (tx) => {
    await tx.query('INSERT INTO orders (id) VALUES ($1)', [orderId]);
    await this.bus.publish(new OrderPlaced({ orderId }), { transaction: tx });
  });
}

// After
@RelayBusTransaction()
async place(orderId: string): Promise<void> {
  await this.orders.insert(orderId); // the repository reads RelayBusTransactionHost.current()
  await this.bus.publish(new OrderPlaced({ orderId }));
}
```

- **Self-calls are transactional.** The decorator replaces the method itself (not an interceptor
  or proxy), so `this.place()` from another method of the class opens the transaction too.
- **Nested calls share one transaction.** A decorated method called inside another one, inside
  `bus.transaction`, inside a transactional handler, or while `transactionResolver` finds a
  transaction (for example `@nestjs-cls/transactional`) joins it instead of opening a second one.
- **No injection needed.** The bus is the one of the application's `RelayBusModule`. Calling the
  method before the application initialised, after it shut down, or with no `RelayBusModule`
  throws a `ConfigurationError` that says so.
- **Async methods only.** On a method that does not return a Promise it fails to compile:
  `'@RelayBusTransaction() needs a method that returns a Promise: make the method async'`.

### Time budget: `transactionTimeout`

Every relaybus transaction (`bus.transaction` and `@RelayBusTransaction()`) is held to
`transactionTimeout` (default `'5s'`). The database enforces it where it can, so a transaction
waiting on something slow is ended by the server and its row locks are released:

- **PostgreSQL:** `SET LOCAL statement_timeout`, and with the `pg` executor also
  `idle_in_transaction_session_timeout` and, on PostgreSQL 17+, `transaction_timeout` (detected
  per transaction). Adapters other than `pg` get `statement_timeout` only, because a server-ended
  session would leave their checked-out connection without an `error` listener.
- **MongoDB:** the driver's operation timeout (`timeoutMS`) over the whole `withTransaction` and
  `maxCommitTimeMS` on the commit.

A transaction that still runs past the budget is rolled back before it commits. Either way the
caller gets a `TransactionTimeoutError` (`RELAYBUS_TRANSACTION_TIMEOUT`, with the database error as
`cause`) whose message points at the usual cause: a slow call to another service inside the
transaction. Move HTTP calls before the transaction or after it commits (the
`@relaybus/eslint-plugin` rule `no-external-call-in-transaction` catches them) instead of raising
the budget.

### Repositories: `RelayBusTransactionHost<T>`

Inject `RelayBusTransactionHost` typed with your store's transaction handle. `current()` returns
the active transaction and throws `TransactionRequiredError` outside one; `find()` returns
`undefined` instead. Outside Nest, `bus.currentTransaction()` does the same as `find()`.

| Store / executor                    | `T` of `RelayBusTransactionHost<T>` | Write with                                   |
| ----------------------------------- | ----------------------------------- | -------------------------------------------- |
| `PostgresEventStore` with `pg`      | `PoolClient`                        | `tx.query(sql, values)`                      |
| `createKyselyExecutor(db)`          | `Transaction<DB>`                   | `tx.insertInto('orders')...execute()`        |
| `createTypeOrmExecutor(dataSource)` | `EntityManager`                     | `tx.getRepository(Order).save(order)`        |
| `createPrismaExecutor(prisma)`      | `Prisma.TransactionClient`          | `tx.order.create({ data })`                  |
| `MongoEventStore`                   | `ClientSession`                     | `collection.insertOne(doc, { session: tx })` |

Without a decorator or `bus.transaction`, `publish` needs a transaction: pass `{ transaction }`,
configure `transactionResolver`, or set `transactionMode: 'optional'` (event stays durable but is
no longer atomic with your writes).

### Transactional handlers

```ts
@RelayBusConsumer(PaymentCaptured, { id: 'ledger.record-capture', transactional: true })
async record(event: EventEnvelope<{ paymentId: string }>, context: HandlerContext<PoolClient>) {
  // Writes through context.transaction commit atomically with the delivery completion:
  // effectively-once processing. Keep these handlers short.
  await context.transaction!.query('INSERT INTO ledger ...', [event.payload.paymentId]);
  await this.bus.publish(new LedgerUpdated({ ... }), { transaction: context.transaction! });
}
```

Non-transactional handlers are at-least-once: make them idempotent (deduplicate on `event.id`).

### ORM integration

Prefer `transactionResolver`, so services call `bus.publish(event)` inside whatever transaction
the ORM already opened. The resolver is called synchronously and must return `undefined` when
there is no active transaction.

```ts
// @nestjs-cls/transactional (not a dependency of this package)
RelayBusModule.forRootAsync({
  inject: [TransactionHost],
  useFactory: (txHost: TransactionHost) => ({
    store,
    transactionResolver: () => (txHost.isTransactionActive() ? txHost.tx : undefined),
  }),
});
```

- **Prisma / raw `pg`:** `@relaybus/postgres` store; Prisma's interactive transaction client is
  adapted by the store package, see its README.
- **TypeORM:** `@relaybus/postgres` over the same `DataSource` connection; resolve the active
  `EntityManager`'s query runner in `transactionResolver`.
- **MongoDB:** `@relaybus/mongodb` store; the transaction is the `ClientSession` (needs a replica set).

## Health

`RelayBusHealth` (exported by the module) has no `@nestjs/terminus` dependency.

```ts
const report = await health.check();
// { status: 'up' | 'down', running, ready, handlers, statistics?, lagMilliseconds?, error? }
```

`down` when the bus is not running, a transport is not ready (`ready` is `bus.isReady()`: for
example a dead Kafka consumer or a disconnected RabbitMQ channel) or the store cannot be queried.
`lagMilliseconds` is how long the oldest due delivery has waited (measured with the `clock` from
the module options, the system clock by default), a good alert signal. With terminus 11:

```ts
@Injectable()
export class RelayBusIndicator {
  constructor(
    private readonly indicators: HealthIndicatorService,
    private readonly relayBus: RelayBusHealth,
  ) {}

  async isHealthy(key = 'relaybus') {
    const indicator = this.indicators.check(key);
    const { status, ...details } = await this.relayBus.check();
    return status === 'up' ? indicator.up(details) : indicator.down(details);
  }
}
```

## Startup failure and restarts

The bus starts during application bootstrap. If the start fails (store setup, transport connect),
application boot fails with that error. A bus whose start failed is stopped for good and cannot be
started again: restart the process rather than retrying in place.

## Testing

Use `@nestjs/testing` with `InMemoryEventStore` and turn the dispatcher off so tests control time:

```ts
const moduleRef = await Test.createTestingModule({
  imports: [
    RelayBusModule.forRoot({ store: new InMemoryEventStore(), dispatcher: { enabled: false } }),
    AppFeatureModule,
  ],
}).compile();
await moduleRef.init();

await moduleRef.get(OrderService).place('o-1');
await moduleRef.get(RelayBus).drain(); // runs every due delivery, including handlers' follow-up events
// assert on side effects
await moduleRef.close();
```

- `bus.runOnce()` runs a single batch; `bus.drain()` loops until nothing is due.
- Retries and delays are scheduled in the future: pass `clock: new FakeClock()` from
  `@relaybus/testing`, `clock.advance(5_000)`, then `drain()` again.
- `bus.listDeliveries({ status: 'dead' })` shows handlers that exhausted their retries.

## Troubleshooting

- **`Nest can't resolve dependencies of ... (?)`** when injecting `RelayBus`: the module is not
  global (`isGlobal: false`) and not imported by the consuming module.
- **Handler never runs:** the provider is not listed in any module `providers`, the process runs
  with `dispatcher.enabled: false`, or the event type differs (check `bus.handlers()`).
- **`uses @RelayBusConsumer but its provider is request-scoped or transient`:** make the provider a
  singleton; inject request data through the event payload instead.
- **Vitest/esbuild:** esbuild does not emit decorator metadata; use explicit `@Inject(Token)` in
  test classes.
- **App hangs on shutdown:** a handler ignores `context.signal`; it is waited for until
  `dispatcher.shutdownTimeout`.

## When to use @relaybus/nestjs, and when not to

Use it when your service is a NestJS 11 or 12 application and you want decorator-based handlers, lifecycle integration and a health check. It needs `@relaybus/core` and a store; for other frameworks use `@relaybus/core` directly.

Documentation: <https://berkcansavur.github.io/relaybus/packages/nestjs>

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
