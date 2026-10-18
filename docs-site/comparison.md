---
title: relaybus vs @nestjs/event-emitter, @nestjs/cqrs, @nestjs/outbox, BullMQ and pg-boss
description: A factual comparison of relaybus with @nestjs/event-emitter, @nestjs/cqrs, @nestjs/outbox, BullMQ, pg-boss and using Kafka directly, covering durability, database transactions and when to choose each.
---

# relaybus vs alternatives

relaybus is an event bus built on the transactional outbox. The tools below solve neighbouring problems and are often the right choice. This page states what each one documents; verify details in their documentation before deciding.

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

## Notes on each alternative

**`@nestjs/event-emitter`.** Built on `eventemitter2`. Its documentation describes an observer implementation and mentions no persistence or delivery guarantee. Errors thrown by a listener are logged by default (`suppressErrors`). It is the simplest option when events are in-process notifications.

**`@nestjs/cqrs`.** Provides CommandBus, QueryBus, EventBus and sagas. Its documentation states that sagas and event handlers run in memory. It does not describe an outbox. Nothing prevents you from using it with relaybus as the durable transport underneath.

**`@nestjs/outbox`.** A transactional outbox module published under the NestJS organisation. Its repository describes PostgreSQL and MySQL stores, a relay with leases, retries and a dead-letter table, and an inbox that deduplicates redeliveries. If you need MySQL, evaluate it. relaybus additionally provides a MongoDB store, Kafka and RabbitMQ transports with publisher confirms and offset-after-inbox semantics, ordering keys and a contract-tested store interface.

**BullMQ.** A queue library on Redis with retries, delayed jobs and flows. Its documentation says it aims for exactly-once delivery and delivers at least once in the worst case. Because jobs live in Redis, adding a job cannot be part of your SQL or MongoDB transaction. It is a good fit when you already run Redis and need background job features.

**pg-boss.** A PostgreSQL job queue using `SKIP LOCKED`, with retries, dead letter queues, scheduling and pub/sub. Its documentation says jobs can be created inside an existing transaction through adapters for Drizzle, Knex, Kysely and Prisma. It models jobs for queues; relaybus models events with one delivery per subscribed handler, per-key ordering and broker routes. For cron-style schedules, prefer a job queue.

**Kafka directly.** Kafka's idempotent producer and transactions give exactly-once semantics within Kafka. Its own design documentation notes that exactly-once with an external system needs cooperation from that system. Producing to Kafka and writing your database are two writes; an outbox makes them one.

## Choosing quickly

| If you need                                                                                      | Consider                                                    |
| ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------- |
| In-process notifications, loss on restart acceptable                                             | `@nestjs/event-emitter`                                     |
| CQRS structure (commands, queries, sagas)                                                        | `@nestjs/cqrs`, optionally with relaybus for durable events |
| Atomic publish with PostgreSQL or MongoDB, per-handler retries, ordering keys, Kafka or RabbitMQ | relaybus                                                    |
| Transactional outbox on MySQL                                                                    | `@nestjs/outbox`                                            |
| Background jobs and rate limiting on Redis                                                       | BullMQ                                                      |
| A PostgreSQL job queue with scheduling                                                           | pg-boss                                                     |
| Streaming processing entirely inside Kafka                                                       | Kafka clients and Kafka transactions                        |

## Sources

- <https://docs.nestjs.com/techniques/events>
- <https://docs.nestjs.com/recipes/cqrs>
- <https://github.com/nestjs/outbox>
- <https://docs.bullmq.io/>
- <https://github.com/timgit/pg-boss>
- <https://kafka.apache.org/41/design/design/>

Comparison checked on 18 October 2026.
