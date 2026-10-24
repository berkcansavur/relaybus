---
title: relaybus packages - core, NestJS, PostgreSQL, MongoDB, Kafka, RabbitMQ, testing
description: The seven @relaybus npm packages - core, nestjs, postgres, mongodb, kafka, rabbitmq and testing - what each does and which ones to install together.
---

# Packages

relaybus is published as seven npm packages under the `@relaybus` scope, released together with one shared version.

| Package                                    | Purpose                                                                                    |
| ------------------------------------------ | ------------------------------------------------------------------------------------------ |
| [`@relaybus/core`](/packages/core)         | The bus, dispatcher, retry policies and the in-memory store.                               |
| [`@relaybus/nestjs`](/packages/nestjs)     | `RelayBusModule`, the `@RelayBusConsumer` decorator, lifecycle and health.                 |
| [`@relaybus/postgres`](/packages/postgres) | PostgreSQL 13+ store for `pg`, Kysely, Knex, TypeORM, Drizzle and Prisma transactions.     |
| [`@relaybus/mongodb`](/packages/mongodb)   | MongoDB 6+ store on multi-document transactions; works with Mongoose sessions.             |
| [`@relaybus/kafka`](/packages/kafka)       | Kafka transport for kafkajs and `@confluentinc/kafka-javascript`, CloudEvents binary mode. |
| [`@relaybus/rabbitmq`](/packages/rabbitmq) | RabbitMQ transport with publisher confirms, reconnects, quorum queues and dead letters.    |
| [`@relaybus/testing`](/packages/testing)   | The store contract suite, a fake clock and an in-memory transport.                         |

## Typical installs

```sh
# NestJS + PostgreSQL
pnpm add @relaybus/core @relaybus/nestjs @relaybus/postgres pg

# NestJS + MongoDB
pnpm add @relaybus/core @relaybus/nestjs @relaybus/mongodb mongodb

# add a broker
pnpm add @relaybus/kafka kafkajs
pnpm add @relaybus/rabbitmq amqplib

# tests
pnpm add -D @relaybus/testing vitest
```

Bring your own store: implement the `EventStore` interface and run the contract suite from `@relaybus/testing` against it.
