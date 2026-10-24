---
title: Transactions in NestJS - @RelayBusTransaction, repositories, time budget and lint rules
description: Run NestJS service methods in an outbox transaction with @RelayBusTransaction(), share it with repositories, hold it to a time budget and lint external calls out of it.
---

# Transactions in NestJS

The outbox only works when the event is written in the same transaction as your business data. This page shows the NestJS way to get that without threading a transaction handle through every call, how repositories join the same transaction, how relaybus keeps transactions short, and the lint rules that catch mistakes before review does.

## Before: the transaction is threaded by hand

```ts
@Injectable()
export class OrderService {
  constructor(
    private readonly bus: RelayBus<PoolClient>,
    private readonly orders: OrderRepository,
  ) {}

  async place(orderId: string): Promise<void> {
    await this.bus.transaction(async (tx) => {
      await this.orders.insert(tx, orderId); // every repository method takes tx
      await this.bus.publish(new OrderPlaced({ orderId }), { transaction: tx });
    });
  }
}
```

## After: `@RelayBusTransaction()`

```ts
@Injectable()
export class OrderService {
  constructor(
    private readonly bus: RelayBus,
    private readonly orders: OrderRepository,
  ) {}

  @RelayBusTransaction()
  async place(orderId: string): Promise<void> {
    await this.orders.insert(orderId);
    await this.bus.publish(new OrderPlaced({ orderId })); // joins the transaction
  }
}

@Injectable()
export class OrderRepository {
  constructor(private readonly transactions: RelayBusTransactionHost<PoolClient>) {}

  async insert(orderId: string): Promise<void> {
    await this.transactions.current().query('INSERT INTO orders (id) VALUES ($1)', [orderId]);
  }
}
```

- The method commits when it resolves and rolls back when it throws.
- The decorator replaces the method itself, so `this.place()` called from another method of the class is transactional too; interceptor-based decorators miss such self-calls.
- A decorated method called inside another one (or inside `bus.transaction`, a transactional handler, or a transaction `transactionResolver` finds) joins that transaction.
- The class does not inject anything for the decorator: the bus is the one of the application's `RelayBusModule`. Without a running module the call throws a `ConfigurationError` saying so.
- Only async methods compile: on a method that does not return a Promise, TypeScript reports `'@RelayBusTransaction() needs a method that returns a Promise: make the method async'`.

## Repositories with your data library

`RelayBusTransactionHost<T>` is typed with the store's transaction handle. `current()` throws `TransactionRequiredError` outside a transaction; `find()` returns `undefined`. Outside NestJS, `bus.currentTransaction()` does the same as `find()`.

| Library            | Store / executor                    | `T`                        | A repository writes with                      |
| ------------------ | ----------------------------------- | -------------------------- | --------------------------------------------- |
| `pg`               | `new PostgresEventStore({ pool })`  | `PoolClient`               | `tx.query(sql, values)`                       |
| Kysely             | `createKyselyExecutor(db)`          | `Transaction<DB>`          | `tx.insertInto('orders').values(o).execute()` |
| TypeORM            | `createTypeOrmExecutor(dataSource)` | `EntityManager`            | `tx.getRepository(Order).save(order)`         |
| Prisma             | `createPrismaExecutor(prisma)`      | `Prisma.TransactionClient` | `tx.order.create({ data })`                   |
| MongoDB / Mongoose | `new MongoEventStore({ client })`   | `ClientSession`            | `Order.create([doc], { session: tx })`        |

### Next to `@nestjs-cls/transactional`

If your services already use `@Transactional()` from `@nestjs-cls/transactional`, keep it and let relaybus find its transaction through `transactionResolver`. A `@RelayBusTransaction()` method called inside a `@Transactional()` one then joins that transaction instead of opening a second one, and `publish` writes into it:

```ts
RelayBusModule.forRootAsync({
  inject: [TransactionHost],
  useFactory: (txHost: TransactionHost<TransactionalAdapterPgPromise>) => ({
    store,
    transactionResolver: () => (txHost.isTransactionActive() ? txHost.tx : undefined),
  }),
});
```

## Time budget

Every relaybus transaction is held to `transactionTimeout` (default `'5s'`), enforced by the database where it can be:

- **PostgreSQL:** `statement_timeout`, and with the `pg` executor also `idle_in_transaction_session_timeout` and, on PostgreSQL 17+, `transaction_timeout`, all set for the transaction only. A transaction left idle by a slow HTTP call is ended by the server and its locks are released.
- **MongoDB:** the driver's `timeoutMS` over the whole `withTransaction` and `maxCommitTimeMS` on the commit.

A transaction that still runs past the budget is rolled back before its commit. The caller gets a `TransactionTimeoutError` (`RELAYBUS_TRANSACTION_TIMEOUT`), with the database's error as `cause`, whose message names the usual reason: a call to another service inside the transaction. Move the call out instead of raising the budget.

## Lint rules

`@relaybus/eslint-plugin` checks both halves at `error` level:

```sh
pnpm add -D @relaybus/eslint-plugin
```

```js
// eslint.config.js
import tseslint from 'typescript-eslint';
import relaybus from '@relaybus/eslint-plugin';

export default tseslint.config(...tseslint.configs.recommended, relaybus.configs.recommended);
```

- `@relaybus/no-external-call-in-transaction` reports `fetch`, `axios`, `got`, Node's `http` / `https`, `undici` and `@nestjs/axios`'s `HttpService` inside `@RelayBusTransaction()` methods, transactional handlers, `.transaction(...)` callbacks, and the methods they call through `this`.
- `@relaybus/publish-requires-transaction` reports `bus.publish` / `publishAll` outside all of those without `{ transaction }`.

```ts
@RelayBusTransaction()
async place(order: Order): Promise<void> {
  await this.httpService.axiosRef.post('/fraud-check', order); // no-external-call-in-transaction
  await this.orders.insert(order);
  await this.bus.publish(new OrderPlaced({ orderId: order.id }));
}

async cancel(orderId: string): Promise<void> {
  await this.bus.publish(new OrderCancelled({ orderId })); // publish-requires-transaction
}
```

See the [package page](/packages/eslint-plugin) for what each rule recognises and its options.
