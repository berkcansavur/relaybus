---
title: Transactional outbox with Prisma and NestJS - relaybus recipe
description: Publish reliable events in the same Prisma interactive transaction as your data using @relaybus/postgres and createPrismaExecutor, with NestJS wiring and timeout notes.
---

# Prisma

Use `@relaybus/postgres` with the Prisma executor so events are written in the same interactive transaction as your Prisma writes.

```sh
pnpm add @relaybus/core @relaybus/nestjs @relaybus/postgres
```

## Wire the store

```ts
import { RelayBusModule } from '@relaybus/nestjs';
import { PostgresEventStore } from '@relaybus/postgres';
import { createPrismaExecutor } from '@relaybus/postgres/prisma';

RelayBusModule.forRootAsync({
  inject: [PrismaService],
  useFactory: (prisma: PrismaService) => ({
    store: new PostgresEventStore({ executor: createPrismaExecutor(prisma) }),
    source: 'orders',
  }),
});
```

## Publish in a Prisma transaction

```ts
await prisma.$transaction(async (tx) => {
  const order = await tx.order.create({ data: { total: 10 } });
  await bus.publish(new OrderPlaced({ orderId: order.id }), { transaction: tx, key: order.id });
});
```

If the transaction rolls back, the event is rolled back with it.

## Notes

- Prisma interactive transactions default to a 5 second timeout. If handlers run longer, pass `createPrismaExecutor(prisma, { timeout, maxWait })`.
- Move your Prisma disconnect from `onModuleDestroy` to `onApplicationShutdown`, so handlers still have their connection while the bus drains.
- The Prisma adapter is typed structurally (relaybus does not depend on Prisma) and is unit-tested against a fake client; it is not yet integration-tested against a real Prisma client. The other adapters run the full contract suite against PostgreSQL.

See [@relaybus/postgres](/packages/postgres) for all adapters.
