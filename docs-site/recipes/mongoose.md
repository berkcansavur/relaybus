---
title: MongoDB transactional outbox with Mongoose - relaybus recipe
description: Publish reliable events in the same MongoDB multi-document transaction as your Mongoose writes using @relaybus/mongodb and a ClientSession, on a replica set.
---

# Mongoose

`@relaybus/mongodb` uses MongoDB multi-document transactions, which need a **replica set or a sharded cluster** (a single-node replica set is enough for development). Mongoose exposes the same driver session.

```sh
pnpm add @relaybus/core @relaybus/mongodb mongodb
```

## Wire the store

```ts
import mongoose from 'mongoose';
import { RelayBus } from '@relaybus/core';
import { MongoEventStore } from '@relaybus/mongodb';
import type { ClientSession } from 'mongodb';

const bus = new RelayBus<ClientSession>({
  store: new MongoEventStore({ client: mongoose.connection.getClient(), database: 'shop' }),
});
```

## Publish in a Mongoose transaction

```ts
const session = await mongoose.startSession();
await session.withTransaction(async () => {
  await Order.create([{ total: 10 }], { session });
  await bus.publish(new OrderPlaced({ orderId: '1' }), { transaction: session });
});
```

## Things to know

- A session without a started transaction is rejected with a `ConfigurationError`: MongoDB would commit each write immediately and silently break atomicity.
- `session.withTransaction` may run your callback again after a transient error, so keep the callback free of side effects other than database writes.
- A standalone `mongod` is rejected with a clear error.

See [@relaybus/mongodb](/packages/mongodb) for options, indexes and retention.
