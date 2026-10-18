# @relaybus/nestjs

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
  @Subscribe(OrderPlaced)
  async onOrderPlaced(event: EventEnvelope<{ orderId: string }>) {
    /* ... */
  }
}

// order.service.ts
await this.bus.transaction(async (tx) => {
  await tx.query('INSERT INTO orders ...');
  await this.bus.publish(new OrderPlaced({ orderId }), { transaction: tx });
});
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

## `@Subscribe(event, options?)`

```ts
@Subscribe(OrderPlaced)                                   // class
@Subscribe('order.placed.v1', { concurrency: 2 })         // type string
@Subscribe(OrderPlaced, { id: 'billing.invoice', transactional: true, timeout: '10s' })
```

Options are core's `SubscribeOptions` with `id` optional. Methods receive `(envelope, context)`;
with a class argument the envelope payload is typed from the event.

**Why `@Subscribe`:** `@nestjs/event-emitter` already owns `@OnEvent`, and the two are commonly
installed together. `@Subscribe` mirrors core's `bus.subscribe` and cannot collide.

Rules, all enforced at boot (the application fails to start, no silent skips):

- **Stable ids.** The default id is `ClassName.methodName`. Deliveries are stored under the id, so
  renaming the class or method orphans pending deliveries; pin `{ id }` on handlers that matter.
- **Duplicate ids** fail init with both locations named. A method with several `@Subscribe`
  decorators must give each an explicit `id`.
- **Singletons only.** Handlers on request-scoped or transient providers (or default-scoped
  providers that depend on one) are rejected: deliveries run outside any request, so there is no
  request instance to call. Providers without `@Subscribe` are never inspected for scope.
- `this` is bound to the provider instance. Only providers are scanned, in every module.

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

Inside `bus.transaction(fn)` every `publish` joins the transaction. Without it, publish needs a
transaction: pass `{ transaction }`, configure `transactionResolver`, or set
`transactionMode: 'optional'` (event stays durable but is no longer atomic with your writes).

### Transactional handlers

```ts
@Subscribe(PaymentCaptured, { id: 'ledger.record-capture', transactional: true })
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
// { status: 'up' | 'down', running, handlers, statistics?, lagMilliseconds?, error? }
```

`down` when the bus is not running or the store cannot be queried. `lagMilliseconds` is how long
the oldest due delivery has waited, a good alert signal. With terminus 11:

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
- **`uses @Subscribe but its provider is request-scoped or transient`:** make the provider a
  singleton; inject request data through the event payload instead.
- **Vitest/esbuild:** esbuild does not emit decorator metadata; use explicit `@Inject(Token)` in
  test classes.
- **App hangs on shutdown:** a handler ignores `context.signal`; it is waited for until
  `dispatcher.shutdownTimeout`.
