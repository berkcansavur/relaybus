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
- The Prisma adapter is typed structurally (relaybus does not depend on Prisma). It runs the full contract suite against PostgreSQL with a generated Prisma Client (7.10, and 6.19 and 5.22 in CI) on `@prisma/adapter-pg`. Prisma 5 needs `previewFeatures = ["driverAdapters"]` in its generator for the adapter.
- Give the store the `pg` pool of your driver adapter too: `new PostgresEventStore({ executor: createPrismaExecutor(prisma), pool })` with `new PrismaClient({ adapter: new PrismaPg(pool) })`. The dispatcher's statements then run as prepared statements, and the store listens for the notification PostgreSQL sends when your `$transaction` commits, so the handler starts within milliseconds of the commit instead of after a fixed delay or the next poll.
- Behind PgBouncer in transaction mode, `?pgbouncer=true` on the connection string is supported. At an isolation level (`bus.transaction(work, { isolationLevel })`), the executor passes the level to Prisma's own `$transaction(work, { isolationLevel })`. Before 1.1.0 it sent `SET TRANSACTION ISOLATION LEVEL`, which failed with SQLSTATE `25001` (Prisma `P2010`) on Prisma 5 and 6 with `?pgbouncer=true`, because their query engine sends `DEALLOCATE ALL` right after `BEGIN`. See [PgBouncer in transaction mode](/tuning#pgbouncer-in-transaction-mode).
- If the work of a `bus.transaction` catches a statement error and carries on, PostgreSQL has aborted the transaction and answers `COMMIT` with `ROLLBACK`. relaybus throws a `TransactionRolledBackError` (`RELAYBUS_TRANSACTION_ROLLED_BACK`) instead of reporting success. The executor runs one `SELECT 1` at the end of each transaction relaybus opens to find this. A transaction you open yourself with `prisma.$transaction` is not checked.
- Prisma 7 raw queries cannot read a `void` column; relaybus' statements never return one.

See [@relaybus/postgres](/packages/postgres) for all adapters.
