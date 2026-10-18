# @relaybus/testing

Test utilities for relaybus.

```sh
pnpm add -D @relaybus/testing vitest
```

## Store contract suite

Every `EventStore` must pass it unchanged against a real database:

```ts
import { eventStoreContract } from '@relaybus/testing';

eventStoreContract('MyEventStore', {
  createStore: async () => {
    const store = new MyEventStore(await connect());
    await store.setup();
    return store;
  },
  destroyStore: async (store) => store.close(),
  // a second instance on the same database with its own connections
  createPeerStore: async () => new MyEventStore(await connect()),
});
```

It covers transactional visibility, idempotent appends, leasing and fencing,
retries and dead letters, release, per-key ordering in commit order, starvation,
purge, statistics, data fidelity and cross-instance claim races.

## FakeClock

```ts
const clock = new FakeClock('2026-01-01T00:00:00Z');
const bus = new RelayBus({ store, clock });
clock.advance(60_000); // make retries due
await bus.drain();
```

## InMemoryTransport

Records publishes, simulates outages with `failNextPublishes(n)` and delivers
JSON copies to subscribers with `deliver(destination, envelope, maxRedeliveries)`.

## License

MIT
