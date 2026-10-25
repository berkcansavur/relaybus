---
title: relaybus FAQ - reliable events, transactional outbox and Kafka in NestJS
description: Answers to common questions about publishing events reliably in NestJS, implementing the transactional outbox pattern, avoiding lost events between a database and Kafka, retries, ordering and idempotency.
---

# FAQ

## How do I publish events reliably in NestJS?

Write the event to your database in the same transaction as your data, then deliver it after commit. With relaybus, register `RelayBusModule.forRoot({ store: new PostgresEventStore({ pool }) })`, call `bus.publish(event)` inside `bus.transaction(...)`, and declare handlers with `@RelayBusConsumer(EventClass)`. If the transaction commits, every handler is called at least once, retried with backoff until it succeeds or is dead-lettered. If it rolls back, the event never existed. See [Getting started](/getting-started).

## How to implement the transactional outbox pattern in NestJS?

Store each event in an outbox table inside the business transaction, and run a relay that claims due rows, delivers them and marks them done. relaybus implements this for you: `publish` inserts the event and one delivery per handler into your transaction, a dispatcher claims deliveries with leases, and failures are rescheduled with exponential backoff and jitter. You do not write the table, the polling loop or the retry logic. See [Transactional outbox](/concepts/outbox).

## How to avoid losing events between the database and Kafka?

Do not write to the database and Kafka separately. Write the event to the outbox in your database transaction and let a Kafka route forward it after commit. With `@relaybus/kafka`, the route is retried until the broker acknowledges (idempotent producer, `acks=-1`), and the consuming service commits the Kafka offset only after storing the event in its inbox, so duplicates are stored once. See [Kafka between services](/recipes/kafka-between-services).

## What is the dual-write problem?

A dual write updates two systems (for example a database and a message broker) without a shared transaction. If one write fails or the process crashes between them, the systems disagree: an event is lost, or an event announces a change that rolled back. The transactional outbox removes the second write from the request path.

## Does relaybus provide exactly-once delivery?

No. Delivery to each handler is at-least-once. A handler subscribed with `transactional: true` that writes through its transaction takes effect exactly once for its database writes, because the writes and the delivery completion commit together. Side effects outside your database, such as HTTP calls, can repeat; send an idempotency key. See [Guarantees](/guarantees).

## How do I make event handlers idempotent?

Deduplicate on `event.id`, or use `transactional: true` and write through `context.transaction` so the effect and the completion commit together. Publishing with a deterministic `id` makes publishing itself idempotent: `bus.publish(event, { id: 'order-123' })`.

## How do I retry failed event handlers in NestJS?

Set `retry` on `@RelayBusConsumer`: `@RelayBusConsumer(OrderPlaced, { retry: { attempts: 5, initialDelay: '2s', maxDelay: '5m' } })`. The defaults are 10 attempts with capped exponential backoff and full jitter. Throw `NonRetryableError` to skip the remaining attempts. See [Retries and backoff](/concepts/retries).

## How do I inspect and requeue failed events?

Use `bus.listDeliveries({ status: 'dead' })` to list dead letters with their last error, and `bus.requeue(ids)` to run them again once the cause is fixed. See [Dead letters](/concepts/dead-letters).

## How do I keep events in order?

Pass a `key` when publishing. Events with the same key are handled one at a time per handler, in commit order. There is no global ordering. See [Ordering keys](/concepts/ordering-keys).

## Which databases and brokers does relaybus support?

PostgreSQL 13 or newer through `@relaybus/postgres` (pg, Prisma, TypeORM, Drizzle, Knex and Kysely), MongoDB 6 or newer on a replica set through `@relaybus/mongodb` (driver or Mongoose sessions), Kafka through `@relaybus/kafka` and RabbitMQ through `@relaybus/rabbitmq`. You can bring your own store by implementing `EventStore` and running the contract suite from `@relaybus/testing`.

## Can I use relaybus without NestJS?

Yes. `@relaybus/core` works with any Node.js application: create `new RelayBus({ store })`, call `bus.subscribe(...)`, `await bus.start()` and publish inside `bus.transaction(...)`. See [@relaybus/core](/packages/core).

## How is relaybus different from @nestjs/event-emitter, BullMQ or pg-boss?

`@nestjs/event-emitter` is in-process and does not persist events. BullMQ stores jobs in Redis, which cannot join your SQL or MongoDB transaction. pg-boss is a PostgreSQL job queue that can enqueue in your transaction; relaybus is an event bus with one delivery per handler, ordering keys and Kafka and RabbitMQ transports. See [the comparison](/comparison).

## How do I test code that publishes events?

Use `InMemoryEventStore`, disable the dispatcher, call `bus.drain()` to run due deliveries and `FakeClock` to advance time past retry delays. See [Testing with FakeClock](/recipes/testing-with-fakeclock).

## Which Node.js and NestJS versions are supported?

Node.js 22.12 or newer and NestJS 11 or 12.
