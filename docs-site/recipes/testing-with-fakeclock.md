---
title: Test retries and dead letters with FakeClock - relaybus recipe
description: Test NestJS event handlers deterministically using @relaybus/testing FakeClock, the in-memory store and bus.drain() to run retries and delays without waiting.
---

# Testing with FakeClock

Retries and delays are scheduled in the future. `FakeClock` from `@relaybus/testing` lets a test advance time instead of sleeping.

```sh
pnpm add -D @relaybus/testing vitest
```

## NestJS

Use the in-memory store and turn the dispatcher off so the test controls when deliveries run:

```ts
import { Test } from '@nestjs/testing';
import { InMemoryEventStore, RelayBus } from '@relaybus/core';
import { RelayBusModule } from '@relaybus/nestjs';
import { FakeClock } from '@relaybus/testing';

const clock = new FakeClock('2026-01-01T00:00:00Z');
const moduleRef = await Test.createTestingModule({
  imports: [
    RelayBusModule.forRoot({
      store: new InMemoryEventStore(),
      dispatcher: { enabled: false },
      clock,
    }),
    AppFeatureModule,
  ],
}).compile();
await moduleRef.init();

await moduleRef.get(OrderService).place('o-1');
await moduleRef.get(RelayBus).drain(); // runs every due delivery

clock.advance(5_000); // make retries due
await moduleRef.get(RelayBus).drain();
```

- `bus.runOnce()` runs a single batch; `bus.drain()` loops until nothing is due.
- `bus.listDeliveries({ status: 'dead' })` shows handlers that exhausted their retries.
- esbuild does not emit decorator metadata, so use explicit `@Inject(Token)` in test classes under Vitest.

## Testing your own store

Run `eventStoreContract` from `@relaybus/testing` against a real database to prove a custom `EventStore` behaves like the built-in ones. See [@relaybus/testing](/packages/testing).
