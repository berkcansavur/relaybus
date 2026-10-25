---
title: relaybus compatibility and tested versions
description: 'The tested matrix of relaybus: Node.js, CommonJS and ESM, NestJS, PostgreSQL clients and ORMs, MongoDB, Kafka and RabbitMQ, codecs, what is explicitly unsupported and notes for serverless.'
---

# Compatibility

What is tested, what is supported but not tested, and what is not supported. "Tested" means a CI job or an integration test in the repository runs against it; the [Stability](/stability) policy says how this list changes.

## Runtimes

|                              | Status                          | Evidence                                                                                                             |
| ---------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Node.js 22 (22.12 and newer) | tested                          | CI test matrix `22.x` and `24.x`                                                                                     |
| Node.js 24                   | tested                          | CI test matrix                                                                                                       |
| CommonJS and ESM             | built and type-checked for both | every package ships `import` and `require` entry points with their own types; `publint` and `attw` check them in CI  |
| NestJS 11 and 12             | tested                          | CI job `nestjs compat` re-resolves `@nestjs/*` to each major and runs the typecheck and the `@relaybus/nestjs` tests |
| Bun, Deno                    | untested, not supported         |                                                                                                                      |

## Stores

| Store               | Versions                                                        | Notes                                                                                                                                                                                     |
| ------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL          | 13 and newer; integration tests run on 13 and 17                | `pg` is the primary client and the only one with prepared statements.                                                                                                                     |
| `pg` pool or client | tested                                                          | `PostgresEventStore({ pool })`                                                                                                                                                            |
| Kysely              | integration tested                                              | `createKyselyExecutor`                                                                                                                                                                    |
| Knex                | integration tested                                              |                                                                                                                                                                                           |
| TypeORM             | integration tested                                              |                                                                                                                                                                                           |
| Drizzle             | integration tested                                              |                                                                                                                                                                                           |
| Prisma              | integration tested (Client 6.19 and 7.10, `@prisma/adapter-pg`) | Full contract suite on a generated client. Give the store your `pg` pool as well for prepared statements and wake-ups at commit.                                                          |
| MongoDB             | 6.0 and newer, integration tests on 7                           | **Replica set or sharded cluster only**: relaybus uses multi-document transactions. A single-node replica set is enough for development. Driver `mongodb` 6 or 7; Mongoose sessions work. |

## Transports

| Transport                                   | Tested                                                                            | Notes                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kafka with `kafkajs` 2.2.x                  | integration tested against a real broker (`confluentinc/cp-kafka:7.5.0`)          | `kafkajs` is **unmaintained**. It is the client the transport is verified with, so it is the supported one today.                                                                                                                                                                                                                                                                                                               |
| Kafka with `@confluentinc/kafka-javascript` | **not tested here**                                                               | The maintained client, and the one to move to. Its KafkaJS-compatible API satisfies the transport's types, but its producer and consumer settings live in a nested `kafkaJS` block, `fromBeginning` and some consumer events differ, so the transport's options are not guaranteed to apply. Read the [Kafka package page](/packages/kafka#confluent-experimental-untested) and test against your version before relying on it. |
| RabbitMQ                                    | integration tested on RabbitMQ 4 (`rabbitmq:4-alpine`); 3.13 or newer is required | **Quorum queues** are used for the queue and the dead-letter queue. `amqplib` 0.10 or newer.                                                                                                                                                                                                                                                                                                                                    |

### Codecs

| Codec                         | Kafka                | RabbitMQ             |
| ----------------------------- | -------------------- | -------------------- |
| `cloudEventsBinary` (default) | tested, golden bytes | tested, golden bytes |
| `cloudEventsStructured`       | tested               | tested               |
| `jsonEnvelope({ ... })`       | tested               | tested               |
| `{ encode, decode }` (custom) | tested               | tested               |

## Explicitly unsupported

These are not planned for the current release line; no store or transport exists for them.

|                                                         | Why / what to use                                                                                                                                                             |
| ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MySQL, SQL Server, SQLite                               | No store. relaybus needs `SELECT ... FOR UPDATE SKIP LOCKED`-style claims, advisory locks and a transactional outbox in your database; PostgreSQL and MongoDB are the stores. |
| MongoDB standalone                                      | No multi-document transactions. Use a replica set.                                                                                                                            |
| Amazon SQS and SNS, NATS, Google Pub/Sub, Redis Streams | No transport. Implement the small `Transport` interface (`publish`, `subscribe`, `close`) yourself if you need one; it is not tested by relaybus.                             |
| Bun, Deno                                               | Untested.                                                                                                                                                                     |

## Serverless and short-lived processes

relaybus delivers from a long-running dispatcher: it claims deliveries on a poll and, on PostgreSQL, wakes on `LISTEN`/`NOTIFY`, and it holds leases while a handler runs. A process that is frozen or killed between requests cannot do that.

- **Publishing from a function works**: publish inside your database transaction and let the commit make the event durable. Set `dispatcher: { enabled: false }` in the function and run the dispatcher in a long-running worker ([Tuning](/tuning)).
- **Dispatching from a function** is possible only by calling `bus.runOnce()` per invocation (one batch) and finishing before the platform freezes the process. Expect higher latency, no `NOTIFY` wake-ups, and leases that expire when an invocation is cut short, which makes the delivery run again (at-least-once holds).
- Behind a connection pooler in transaction mode, use the settings in [Tuning](/tuning) (`wakeUp: 'poll'`, `preparedStatements: false` where needed).

## Where this is checked

CI (`.github/workflows/ci.yml`): `quality` (Prettier, ESLint, typecheck, performance budgets), `test` on Node 22 and 24 with 100 % coverage thresholds, `build` (all packages, `publint`, `attw`), `nestjs-compat` on NestJS 11 and 12. Integration tests start their databases and brokers with Testcontainers.
