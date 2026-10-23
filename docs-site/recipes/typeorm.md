---
title: Transactional outbox with TypeORM and NestJS - relaybus recipe
description: Publish reliable events in the same TypeORM transaction as your entities using @relaybus/postgres and createTypeOrmExecutor with a NestJS DataSource.
---

# TypeORM

Use `@relaybus/postgres` with the TypeORM executor to write events in the same transaction as your entities.

```sh
pnpm add @relaybus/core @relaybus/nestjs @relaybus/postgres
```

## Wire the store

```ts
import { DataSource } from 'typeorm';
import { RelayBusModule } from '@relaybus/nestjs';
import { PostgresEventStore } from '@relaybus/postgres';
import { createTypeOrmExecutor } from '@relaybus/postgres/typeorm';

RelayBusModule.forRootAsync({
  inject: [DataSource],
  useFactory: (dataSource: DataSource) => ({
    store: new PostgresEventStore({ executor: createTypeOrmExecutor(dataSource) }),
    source: 'orders',
  }),
});
```

## Publish in a TypeORM transaction

The transaction handle is the `EntityManager` of `dataSource.transaction`:

```ts
await dataSource.transaction(async (manager) => {
  const order = await manager.save(Order, { total: 10 });
  await bus.publish(new OrderPlaced({ orderId: order.id }), {
    transaction: manager,
    key: order.id,
  });
});
```

## Avoid passing the manager around

If your services already run inside a transaction opened by a library such as `@nestjs-cls/transactional`, configure a `transactionResolver` so `bus.publish(event)` joins the active transaction automatically. The resolver is called synchronously and must return `undefined` when no transaction is active.

See [@relaybus/postgres](/packages/postgres) and [@relaybus/nestjs](/packages/nestjs).
