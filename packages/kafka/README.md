# @relaybus/kafka

@relaybus/kafka is the Apache Kafka transport of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it forwards committed outbox events to Kafka topics and stores consumed events once in an idempotent inbox, so events are not lost between your database and Kafka.

Apache Kafka transport for [relaybus](../../README.md). It forwards committed
outbox events to Kafka topics and feeds events consumed from Kafka into the
bus inbox, with at-least-once delivery and idempotent storage.

- Wire format: [CloudEvents Kafka protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md), binary content mode, so any CloudEvents-aware consumer or tool can read the topic.
- Built for and tested against [`kafkajs`](https://kafka.js.org) 2.2.x on a real broker. The KafkaJS-compatible API of `@confluentinc/kafka-javascript` is **experimental and untested**, see [Confluent](#confluent-experimental-untested). Neither client is a dependency; install the one you use.
- Never loses an event: the offset is committed only after the bus stored the event; a failing store blocks the partition instead of skipping.

## Install

```sh
pnpm add @relaybus/core @relaybus/kafka kafkajs
```

## Usage

```ts
import { Kafka, Partitioners } from 'kafkajs';
import { RelayBus } from '@relaybus/core';
import { KafkaTransport } from '@relaybus/kafka';

const kafka = new Kafka({ clientId: 'orders', brokers: ['kafka:9092'] });

const transport = new KafkaTransport({
  kafka,
  groupId: 'billing', // required to subscribe
  producerConfig: { createPartitioner: Partitioners.DefaultPartitioner },
});

const bus = new RelayBus({
  store,
  source: 'orders',
  transports: [transport],
  routes: [{ event: OrderPlaced, transport: 'kafka', destination: 'orders.events' }],
  subscriptions: [{ transport: 'kafka', destinations: ['payments.events'] }],
});
await bus.start(); // throws if the consumer cannot join its group
```

Readiness for Nest or Kubernetes:

```ts
// true only when the producer is connected and, if subscribed, the consumer is consuming
const ready = transport.isReady(); // used by the bus health check

// false while rebalancing, after a crash, after close, or while any partition is stuck failing
const consuming = transport.isConsuming();
```

## Options

| Option                         | Default         | Meaning                                                                                                                                                                                          |
| ------------------------------ | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kafka`                        | required        | A `kafkajs` `Kafka` or Confluent `KafkaJS.Kafka` instance. It is the producer/consumer factory.                                                                                                  |
| `name`                         | `'kafka'`       | Transport name used by routes and subscriptions.                                                                                                                                                 |
| `groupId`                      | none            | Consumer group. Required as soon as `subscribe` is used; publish-only services can omit it.                                                                                                      |
| `fromBeginning`                | `true`          | Where a group without committed offsets starts. `true` so events published before the first start are not skipped.                                                                               |
| `maxInFlightRequests`          | `1`             | Producer in-flight requests, 1 to 5. `1` keeps retries from reordering events of one key.                                                                                                        |
| `producerConfig`               | `{}`            | Extra producer settings (partitioner, compression, ...). `idempotent` and `maxInFlightRequests` always win.                                                                                      |
| `consumerConfig`               | `{}`            | Extra consumer settings (`sessionTimeout`, `retry`, `heartbeatInterval`, ...). `groupId` always wins.                                                                                            |
| `processing.maxAttempts`       | `5`             | In-process attempts per delivery of a record.                                                                                                                                                    |
| `processing.initialBackoff`    | `'200ms'`       | Delay before attempt 2, doubling per attempt.                                                                                                                                                    |
| `processing.maxBackoff`        | `'5s'`          | Upper bound of the delay between attempts.                                                                                                                                                       |
| `processing.heartbeatInterval` | `'1s'`          | Heartbeat interval while sleeping between attempts, so a long backoff never exceeds `sessionTimeout`.                                                                                            |
| `startupTimeout`               | `'30s'`         | How long `subscribe` waits for the group join. A failed start-up disconnects the consumer it started.                                                                                            |
| `publishTimeout`               | `'30s'`         | How long `publish` waits for the broker ack before rejecting with `PublishTimeoutError`.                                                                                                         |
| `restart.initialBackoff`       | `'1s'`          | First delay of the consumer restart loop after an unrecoverable crash; doubles per failure.                                                                                                      |
| `restart.maxBackoff`           | `'30s'`         | Upper bound of the restart delay.                                                                                                                                                                |
| `onInvalidMessage`             | `'dead-letter'` | `'dead-letter'` (to `<topic>.dead`), `{ deadLetterTopic: string \| (sourceTopic) => string }`, a callback `(message) => void \| Promise<void>`, or `'skip'` (drops the record; explicit opt-in). |
| `logger`                       | `console`       | `log`, `warn`, `error`. Messages start with `[KafkaTransport](method)`.                                                                                                                          |

## Wire format

Value: the JSON payload. Key: the envelope key (`null` means no key).

| Header                               | Content                                                   |
| ------------------------------------ | --------------------------------------------------------- |
| `ce_specversion`                     | `1.0`                                                     |
| `ce_id`, `ce_type`, `ce_source`      | envelope `id`, `type`, `source`                           |
| `ce_time`                            | `occurredAt` as RFC 3339 UTC (`2026-01-01T00:00:00.000Z`) |
| `ce_subject`                         | envelope key, only when present                           |
| `content-type`                       | `application/json`                                        |
| `ce_correlationid`, `ce_causationid` | relaybus extensions, only when present                    |
| anything else                        | envelope headers, passed through                          |

An envelope header whose name collides with the protocol (`ce_*`, `content-type`,
`x-relaybus-*`, any case) is stored as `x-relaybus-h-<name>` and restored on read,
so user headers can never overwrite event attributes and round-trip unchanged.
Decoding is strict: wrong spec version, missing or empty `ce_id`/`ce_type`/`ce_source`,
an invalid event type, a non-RFC 3339 `ce_time`, a non-JSON content type, a missing or non-JSON/UTF-8
value, repeated protocol headers, or a `ce_subject` that disagrees with the record key all make
the record malformed.

`ce_time` is optional in CloudEvents: when a producer omits it, the Kafka record timestamp becomes
`occurredAt`. relaybus itself always writes it.

Payloads are JSON. An `undefined`, function, symbol, `BigInt` or circular payload, and an
`occurredAt` that is an invalid date or lies outside the years 0 to 9999 (not representable as
RFC 3339), are rejected on publish with `UnencodableEventError`, a `NonRetryableError`, because
retrying cannot help. `Date` payload values become ISO strings. A header named `__proto__` is kept
as ordinary data by the codec (kafkajs itself drops it when it builds the consumer's header object).

## Partitioning and ordering

The key decides the partition. Events with the same key are consumed in publish order; events
without a key (`key: null`) are spread over partitions and have no ordering. Use the aggregate id
as the key where order matters. Use the same partitioner in every producer of a topic, otherwise
one key can map to different partitions depending on who wrote it.

## Guarantees

- **Publish**: idempotent producer, `acks=-1`; `publish` resolves after the broker ack and rejects
  with `PublishTimeoutError` after `publishTimeout` (kafkajs would otherwise retry an idempotent
  send for as long as the broker is unreachable). A rejected publish makes the outbox retry; a
  timed-out send may still land, so a message can be duplicated, and the idempotent producer plus
  the consumer's dedup on the event id absorb it. Pass `producerConfig: { retry: { retries } }` to
  bound client-side retries further.
- **Consume**: `eachMessage` awaits the bus (`RelayBus.receive`, one idempotent insert) and the
  client commits the offset only afterwards. A duplicate message is stored once.
- **Handler failure**: retried in-process `processing.maxAttempts` times with backoff and
  heartbeats (also during the backoff sleeps). If it still fails, the error is rethrown to the client
  wrapped in a `HandlerFailedError` that keeps the original name and message but has no `cause`;
  kafkajs classifies a raw `TypeError`, `RangeError`, `ReferenceError` or `SyntaxError` as
  unrecoverable and would stop the consumer for good. The client does not commit and re-fetches the
  same record. The record is never skipped, so a persistent outage blocks the partition until the
  store recovers. That is deliberate: `receive` is a cheap insert, and skipping would lose the
  event. `isConsuming()` is `false` while any partition is stuck (tracked per partition).
- **Dead consumer**: the transport does not rely on the client to restart itself. When the client
  reports a crash with `restart=false`, the transport logs it and reconnects, re-subscribes and
  re-runs the consumer with bounded backoff (`restart.*`) until it joined again or the transport
  closed. `isReady()` is `false` meanwhile.
  We deliberately do not use `pause()` plus `seek()` plus return: the client resolves the offset of
  a returned `eachMessage` before applying the seek, which can commit past the record if the
  process dies in that window.
- **Malformed or rejected record**: a record that cannot be decoded, or that the bus rejects with
  any `NonRetryableError` (such as `InvalidEnvelopeError` or `EventValidationError`), is handled by
  `onInvalidMessage`. The default copies the untouched record to `<topic>.dead` with its original
  headers plus `relaybus-invalid-reason`, `relaybus-source-topic`, `relaybus-source-partition` and
  `relaybus-source-offset`, using the same idempotent producer, and commits only after the ack.
  A failing callback or dead-letter send is retried like a handler failure, so a record is dropped
  only when you chose `'skip'` or your callback returned. A subscription that consumes its own
  dead-letter topic is rejected as a loop.
- **Start-up**: `subscribe` resolves only after the consumer joined its group. A connect, subscribe
  or run error, a crash before the first join, or `startupTimeout` rejects it, so `bus.start()`
  fails instead of reporting a healthy service that consumes nothing.

## Client API used

Only these members are called (`KafkaClientLike` and friends in `src/kafka-client.ts`):

| Client                     | Members                                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| `kafka`                    | `producer(config)`, `consumer(config)`                                                                   |
| producer                   | `connect()`, `send({ topic, acks, messages: [{ key, value, headers }] })`, `disconnect()`                |
| consumer                   | `connect()`, `subscribe({ topics, fromBeginning })`, `run({ eachMessage })`, `disconnect()`              |
| `eachMessage` payload      | `topic`, `partition`, `message.{ key, value, headers, offset }`, `heartbeat()`                           |
| consumer events (optional) | `consumer.on(name, listener)` and `consumer.events.{ GROUP_JOIN, CRASH, REBALANCING, STOP, DISCONNECT }` |

`on`/`events` are optional: without `GROUP_JOIN` the transport cannot wait for the join or observe
crashes, so `subscribe` resolves when `run` returns and `isConsuming()` only reflects processing
stalls and `close()`. kafkajs 2.2.4 is verified by the integration tests.

## Confluent (experimental, untested)

`@confluentinc/kafka-javascript` exposes a KafkaJS-compatible API, and the structural client types
here are satisfied by it, but nothing in this repository runs against it. Its documented
differences from kafkajs mean the options below do **not** work as written for Confluent:

- `idempotent` and `maxInFlightRequests` (producer) and `groupId` (consumer) must be nested in the
  client's `kafkaJS: { ... }` config block. The transport passes them at the top level, so with
  Confluent the producer is not guaranteed to be idempotent and the group id may be ignored.
- `fromBeginning` is not supported per `subscribe()`; it is a consumer-level `kafkaJS` setting.
  The transport's `fromBeginning` option is therefore not applied.
- `heartbeat()` is a no-op there and the consumer `events` (`GROUP_JOIN`, `CRASH`, ...) are not
  documented, so start-up falls back to "resolved when `run` returns" and crash recovery cannot
  trigger.
- Failed `eachMessage` handling (re-seek) and the error classification of a rethrown error differ.

Use it only after verifying these against your version; until a Confluent path is tested, treat
kafkajs as the supported client.

## Operations

- **Topics**: create them up front with the partitions and retention you want. Relying on
  auto-creation gives broker defaults (and kafkajs subscribes to a missing topic only after
  auto-creating it). Create the dead letter topics (`<topic>.dead` by default) too, or the broker
  must auto-create them; otherwise the dead-letter send fails and the record keeps blocking its
  partition (nothing is lost). Monitor the dead-letter topics.
- **One subscription per transport**: `subscribe` can be called once; list every topic in that
  subscription. Create a second `KafkaTransport` (with its own `name` and `groupId`) for a second
  consumer group.
- **Session timeout**: the retry loop heartbeats between attempts. Keep
  `maxAttempts * maxBackoff` well below the consumer `sessionTimeout` (30 s default).
- **Concurrency**: one record at a time per partition (kafkajs default); scale with partitions and
  service replicas in the same group. With `partitionsConsumedConcurrently > 1` in `consumerConfig`
  the stall state is tracked per partition.
- **Retention vs. inbox**: Kafka may redeliver old records after rebalances or offset resets. The
  bus deduplicates on the event id for as long as it keeps completed deliveries
  (`retention.completed`, default 7 days), so keep that longer than your redelivery window.
- **Partitioner warning** (kafkajs 2.x): pass `createPartitioner: Partitioners.DefaultPartitioner`
  in `producerConfig` to silence the legacy partitioner warning.

## Troubleshooting

- `did not join group ... within 30000ms` at start: broker unreachable, wrong credentials, or the
  group is rebalancing for longer than `startupTimeout`. Check the kafkajs connection settings.
- `isConsuming()` stays `false` with `Giving up for now` logs: `RelayBus.receive` keeps failing
  (database down?). Fix the store; consumption resumes by itself.
- `Dead-lettered malformed message` logs: a producer on that topic is not writing CloudEvents binary
  mode with `content-type: application/json`. Inspect `<topic>.dead` and its `relaybus-invalid-reason`.
- `Consumer crashed ... restart=false` followed by `Consumer restarted`: the client gave up and the
  transport recovered it. Repeated `Restart failed` logs mean the broker or credentials are the problem.
- `PublishTimeoutError`: no broker ack within `publishTimeout`; the outbox retries.
- Events appear twice in logs but handlers run once: expected, the inbox deduplicates on the id.

## When to use @relaybus/kafka, and when not to

Use it to send events between services through Kafka without a dual write between your database and the broker. It is a transport, not a store: it needs `@relaybus/core` and a store, and `@confluentinc/kafka-javascript` support is experimental and untested.

Documentation: <https://berkcansavur.github.io/relaybus/packages/kafka>

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
