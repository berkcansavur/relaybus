---
title: Getting started with relaybus in NestJS and PostgreSQL
description: Add a reliable transactional outbox event bus to a NestJS service backed by PostgreSQL in five minutes, with a subscribed handler, a transactional publish and retries.
---

# Getting started: NestJS and PostgreSQL in 5 minutes

This guide adds relaybus to a NestJS service that already uses PostgreSQL. You need Node.js 22.12 or newer, NestJS 11 or 12 and PostgreSQL 13 or newer.

## 1. Install

```sh
pnpm add @relaybus/core @relaybus/nestjs @relaybus/postgres pg
pnpm add -D @types/pg
```

## 2. Register the module

```ts
// app.module.ts
import { Module } from '@nestjs/common';
import { Pool } from 'pg';
import { RelayBusModule } from '@relaybus/nestjs';
import { PostgresEventStore } from '@relaybus/postgres';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

@Module({
  imports: [
    RelayBusModule.forRoot({
      store: new PostgresEventStore({ pool }),
      source: 'orders',
    }),
  ],
})
export class AppModule {}
```

The module is global. On application bootstrap it creates the tables (unless you set `setupStore: false`) and starts the dispatcher. Call `app.enableShutdownHooks()` in `main.ts` so running handlers finish on SIGTERM.

## 3. Define an event and a handler

```ts
// order.events.ts
import { defineEvent } from '@relaybus/core';

export class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
```

```ts
// shipping.handlers.ts
import { Injectable } from '@nestjs/common';
import { Subscribe } from '@relaybus/nestjs';
import type { EventEnvelope } from '@relaybus/core';
import { OrderPlaced } from './order.events';

@Injectable()
export class ShippingHandlers {
  @Subscribe(OrderPlaced, { retry: { attempts: 5 } })
  async ship(event: EventEnvelope<{ orderId: string }>) {
    // runs after the order transaction commits, retried until it succeeds
  }
}
```

List `ShippingHandlers` in the `providers` of one of your modules. Handlers are discovered automatically.

## 4. Publish inside your transaction

```ts
// order.service.ts
import { Injectable } from '@nestjs/common';
import { RelayBus } from '@relaybus/core';
import type { PoolClient } from 'pg';
import { OrderPlaced } from './order.events';

@Injectable()
export class OrderService {
  constructor(private readonly bus: RelayBus<PoolClient>) {}

  async place(orderId: string) {
    await this.bus.transaction(async (tx) => {
      await tx.query('INSERT INTO orders (id) VALUES ($1)', [orderId]);
      await this.bus.publish(new OrderPlaced({ orderId }), { key: orderId });
    });
  }
}
```

If the `INSERT` rolls back, the event never existed. If the transaction commits, `ShippingHandlers.ship` runs at least once, retried with exponential backoff and jitter until it succeeds or the event is dead-lettered.

## 5. See it work

Call `place('o-1')`, then look at the tables relaybus created: `relaybus_events` holds the event and `relaybus_deliveries` holds one delivery per handler with its status. Make the handler throw to watch the retries.

## Next steps

- [Guarantees](/guarantees) states exactly what is promised.
- [Concepts](/concepts/outbox) explains outbox, deliveries, leases, retries, ordering keys and dead letters.
- Already using an ORM? See the [Prisma](/recipes/prisma), [TypeORM](/recipes/typeorm) and [Mongoose](/recipes/mongoose) recipes.
- Need to reach other services? See [Kafka between services](/recipes/kafka-between-services) and [RabbitMQ](/recipes/rabbitmq).
