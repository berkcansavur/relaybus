# @relaybus/core

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

| Option                                    | Default                                       | Description                                                                                                                                   |
| ----------------------------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `store`                                   | required                                      | `EventStore` implementation.                                                                                                                  |
| `source`                                  | `'relaybus'`                                  | Service name recorded on every event.                                                                                                         |
| `transactionMode`                         | `'required'`                                  | `'optional'` lets `publish` open its own transaction.                                                                                         |
| `transactionResolver`                     | —                                             | Returns the caller's active transaction, e.g. from `@nestjs-cls/transactional`.                                                               |
| `unroutedEvents`                          | `'warn'`                                      | `'throw'` rejects events without any handler or route.                                                                                        |
| `retry`                                   | 10 attempts, `1s`, ×2, max `10m`, full jitter | Default handler retry policy. `attempts: Infinity` retries forever.                                                                           |
| `handlerTimeout`                          | `false`                                       | Default handler timeout.                                                                                                                      |
| `dispatcher.enabled`                      | `true`                                        | Run handlers in this process.                                                                                                                 |
| `dispatcher.concurrency`                  | `10`                                          | Handlers running at once.                                                                                                                     |
| `dispatcher.batchSize`                    | concurrency                                   | Deliveries claimed per query.                                                                                                                 |
| `dispatcher.pollInterval`                 | `'1s'`                                        | Idle wait between claims; retries are picked up on this cadence.                                                                              |
| `dispatcher.leaseDuration`                | `'30s'`                                       | Lease length, renewed every third while a handler runs.                                                                                       |
| `dispatcher.shutdownTimeout`              | `'30s'`                                       | Wait for running handlers on `stop`, then release them.                                                                                       |
| `dispatcher.completionBatchWindow`        | `'2ms'`                                       | Deliveries that finish within this window are completed with one store call; `0` completes one by one.                                        |
| `dispatcher.completionBatchSize`          | `200`                                         | Most deliveries completed in one store call.                                                                                                  |
| `retention.completed`                     | `'7d'`                                        | Delete events whose deliveries all completed after this; also the deduplication horizon. A completed delivery is deleted at once (see below). |
| `retention.dead`                          | `false`                                       | Delete dead letters after this.                                                                                                               |
| `retention.interval`                      | `'1h'`                                        | Cleanup cadence.                                                                                                                              |
| `transports` / `routes` / `subscriptions` | —                                             | Broker integration, see below.                                                                                                                |
| `setupStore`                              | `true`                                        | Create tables and indexes on `start`.                                                                                                         |
| `observers`                               | `[]`                                          | Lifecycle hooks for logs, metrics and tracing.                                                                                                |
| `logger`                                  | `console`                                     | `{ log, warn, error }`.                                                                                                                       |
| `clock`, `generateIdentifier`, `random`   | system                                        | Replace in tests.                                                                                                                             |

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

**Completed deliveries are deleted.** Completing a delivery removes its row instead
of updating it, which is what keeps the write cost of a delivery constant (see
`docs/performance/write-amplification.md`). `listDeliveries({ status: 'completed' })`
and `statistics().completed` therefore report nothing unless the store keeps a
completion history (`completionHistory: true` on the SQL stores,
`retainCompleted: true` on `InMemoryEventStore`). Deduplication does not depend
on it: an event remembers every handler it ever had a delivery for, so a
redelivered or republished event never runs a completed handler again, for as
long as the event is kept (`retention.completed`).

## Writing a store

Implement `EventStore` and run `eventStoreContract` from
[`@relaybus/testing`](../testing) against a real database. The contract defines
fencing, ordering, commit-order sequencing, visibility and purge semantics.

## License

MIT
