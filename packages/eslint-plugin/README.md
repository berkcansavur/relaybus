# @relaybus/eslint-plugin

@relaybus/eslint-plugin is the ESLint plugin of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it keeps outbox transactions short and events atomic with your writes by flagging calls to other services inside a transaction and `publish` calls outside one.

```sh
pnpm add -D @relaybus/eslint-plugin
```

ESLint 9 or newer, flat config. For TypeScript files with decorators use the `typescript-eslint` parser (type information is not needed).

```js
// eslint.config.js
import tseslint from 'typescript-eslint';
import relaybus from '@relaybus/eslint-plugin';

export default tseslint.config(...tseslint.configs.recommended, relaybus.configs.recommended);
```

`recommended` turns both rules on at `error` under the plugin name `@relaybus`.

## Where a transaction is

Both rules agree on what runs inside a transaction:

- a method decorated with `@RelayBusTransaction()`;
- a handler that runs in a transaction: `@RelayBusConsumer(Event, { transactional: true })`, an
  `on<Event>` method of a `@RelayBusCatalogConsumer(Catalog, { transactional: true })` class (also
  under the deprecated names `@Subscribe` and `@EventConsumer`), and
  `bus.subscribe(Event, handler, { transactional: true })`;
- the callback of `x.transaction(...)`, `x.$transaction(...)` or `x.withTransaction(...)`
  (`bus.transaction`, TypeORM, Knex, Sequelize, Prisma, MongoDB sessions);
- any method of the same class called through `this.method()` from one of the above, transitively.

## `@relaybus/no-external-call-in-transaction`

Reports calls to other services inside a transaction: `fetch`, `axios` (`axios(...)`,
`axios.get(...)`, any method), `got`, `http.request` / `http.get` / `https.*` of Node.js, `undici`
(`request`, `fetch`, `stream`, `pipeline`), and `HttpService` of `@nestjs/axios`
(`this.httpService.get(...)`, `this.http.post(...)`, `httpService.axiosRef.get(...)`). Imports,
renamed imports and `require` are followed.

A transaction holds its row locks and a database connection until it ends. An HTTP call inside it
makes every other writer of those rows wait for the remote service, and relaybus rolls the
transaction back with `TransactionTimeoutError` once it outlives `transactionTimeout`.

```ts
// Reported
@RelayBusTransaction()
async place(order: Order): Promise<void> {
  const price = await this.httpService.axiosRef.get(`/prices/${order.sku}`);
  await this.orders.insert({ ...order, price: price.data });
  await this.bus.publish(new OrderPlaced({ orderId: order.id }));
}

// Fixed: call before the transaction, and react to the event after the commit
async place(order: Order): Promise<void> {
  const price = await this.httpService.axiosRef.get(`/prices/${order.sku}`);
  await this.save({ ...order, price: price.data });
}

@RelayBusTransaction()
private async save(order: PricedOrder): Promise<void> {
  await this.orders.insert(order);
  await this.bus.publish(new OrderPlaced({ orderId: order.id }));
}
```

## `@relaybus/publish-requires-transaction`

Reports `publish` / `publishAll` on a relaybus bus or catalog publisher outside a transaction
without `{ transaction }`. Such a publish is not atomic with your writes and throws
`TransactionRequiredError` at runtime (`transactionMode: 'required'`, the default). Options that
the rule cannot see into (a variable, a spread) are trusted.

The publishers are recognised by name: the object's last name ends in `bus`, `Bus`, `publisher` or
`Publisher` (`this.bus`, `eventBus`, `orderPublisher`). Set your own pattern when you name them
differently:

```js
{ rules: { '@relaybus/publish-requires-transaction': ['error', { publishers: '^(bus|orders)$' }] } }
```

## Limits

The rules read one file and know names, not types: a call to a service hidden behind your own
wrapper function, or a transaction opened in another file, is not seen. The runtime checks stay in
place behind them (`TransactionRequiredError`, `transactionTimeout`).

Documentation: https://berkcansavur.github.io/relaybus/recipes/transactions-in-nestjs

## License

MIT
