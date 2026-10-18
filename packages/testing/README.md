# @relaybus/testing

@relaybus/testing is the test toolkit of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it provides the EventStore contract suite, a `FakeClock` and an in-memory transport for deterministic tests of retries and delivery.

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

## When to use @relaybus/testing, and when not to

Use it in your test suite for fast, deterministic tests, or to prove that a custom `EventStore` satisfies the contract. It is a development dependency and requires `vitest` 3 or newer.

Documentation: <https://berkcansavur.github.io/relaybus/packages/testing>

### relaybus vs alternatives

<!-- relaybus:snippet:comparison -->

|                                                                      | What it is                                                                                                                                                | Survives a restart                | Written in your database transaction                                      | Choose it when                                                                                                       |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **relaybus**                                                         | Event bus built on the transactional outbox: PostgreSQL and MongoDB stores, Kafka and RabbitMQ transports, per-handler deliveries                         | Yes, events live in your database | Yes                                                                       | Events must never be lost or phantom, and handlers need retries, dead letters and ordering keys                      |
| [`@nestjs/event-emitter`](https://docs.nestjs.com/techniques/events) | In-process event emitter built on `eventemitter2`                                                                                                         | No persistence is documented      | No                                                                        | Simple in-process notifications where losing an event on restart is acceptable                                       |
| [`@nestjs/cqrs`](https://docs.nestjs.com/recipes/cqrs)               | CommandBus, QueryBus, EventBus and sagas; the docs say sagas and event handlers run in memory                                                             | No built-in persistence           | No                                                                        | You want CQRS structure; it can sit on top of an outbox                                                              |
| [`@nestjs/outbox`](https://github.com/nestjs/outbox)                 | Transactional outbox module from the NestJS organisation: PostgreSQL, MySQL and in-memory stores, relay with leases and retries, dead-letter table, inbox | Yes with the database stores      | Yes                                                                       | You need MySQL, or a NestJS-organisation module; compare features with relaybus before choosing                      |
| [BullMQ](https://docs.bullmq.io/)                                    | Job queue on Redis with retries, delayed jobs and flows; at-least-once in the worst case                                                                  | Yes, in Redis                     | No, jobs are stored in Redis and cannot join a SQL or MongoDB transaction | Your stack is Redis-based and background jobs matter more than event atomicity                                       |
| [pg-boss](https://github.com/timgit/pg-boss)                         | Job queue on PostgreSQL using `SKIP LOCKED`, with retries, dead letter queues, scheduling and pub/sub; jobs can be created in your own transaction        | Yes, in PostgreSQL                | Yes, through its database adapters                                        | You want a PostgreSQL job queue with scheduling; relaybus is an event bus with per-handler deliveries and transports |
| Kafka client directly                                                | A distributed log; the idempotent producer and transactions give exactly-once within Kafka                                                                | Yes, in Kafka                     | No, producing to Kafka is a second write                                  | You consume and produce only inside Kafka; add an outbox when a database write must be announced                     |

This table summarises the vendors' own documentation as of October 2026; check their docs for current details.
Full comparison and sources: <https://berkcansavur.github.io/relaybus/comparison>.

<!-- relaybus:end -->

## License

MIT
