# @relaybus/kafka

Apache Kafka transport for [relaybus](../../README.md). It forwards committed
outbox events to Kafka topics and feeds events consumed from Kafka into the
bus inbox, with at-least-once delivery and idempotent storage.

- Wire format: [CloudEvents Kafka protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md), binary content mode, so any CloudEvents-aware consumer or tool can read the topic.
- Client agnostic: works with [`kafkajs`](https://kafka.js.org) and with the KafkaJS-compatible API of [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript). Neither is a dependency; install the one you use.
- Never loses an event: the offset is committed only after the bus stored the event; a failing store blocks the partition instead of skipping.

## Install

```sh
pnpm add @relaybus/core @relaybus/kafka kafkajs
# or: pnpm add @relaybus/core @relaybus/kafka @confluentinc/kafka-javascript
```

## Usage

```ts
import { Kafka, Partitioners } from 'kafkajs';
import { RelayBus } from '@relaybus/core';
import { KafkaTransport } from '@relaybus/kafka';

const kafka = new Kafka({ clientId: 'orders', brokers: ['kafka:9092'] });
// With Confluent: new KafkaJS.Kafka({ kafkaJS: { brokers: ['kafka:9092'] } })

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
// false while rebalancing, after a crash, after close, or while a record is stuck failing
const ready = transport.isConsuming();
```

## Options

| Option                      | Default   | Meaning                                                                                                            |
| --------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------ |
| `kafka`                     | required  | A `kafkajs` `Kafka` or Confluent `KafkaJS.Kafka` instance. It is the producer/consumer factory.                    |
| `name`                      | `'kafka'` | Transport name used by routes and subscriptions.                                                                   |
| `groupId`                   | none      | Consumer group. Required as soon as `subscribe` is used; publish-only services can omit it.                        |
| `fromBeginning`             | `true`    | Where a group without committed offsets starts. `true` so events published before the first start are not skipped. |
| `maxInFlightRequests`       | `1`       | Producer in-flight requests, 1 to 5. `1` keeps retries from reordering events of one key.                          |
| `producerConfig`            | `{}`      | Extra producer settings (partitioner, compression, ...). `idempotent` and `maxInFlightRequests` always win.        |
| `consumerConfig`            | `{}`      | Extra consumer settings (`sessionTimeout`, `retry`, `heartbeatInterval`, ...). `groupId` always wins.              |
| `processing.maxAttempts`    | `5`       | In-process attempts per delivery of a record.                                                                      |
| `processing.initialBackoff` | `'200ms'` | Delay before attempt 2, doubling per attempt.                                                                      |
| `processing.maxBackoff`     | `'5s'`    | Upper bound of the delay between attempts.                                                                         |
| `startupTimeout`            | `'30s'`   | How long `subscribe` waits for the group join.                                                                     |
| `onInvalidMessage`          | `'skip'`  | `'skip'`, a callback `(message) => void \| Promise<void>`, or `{ deadLetterTopic }`.                               |
| `logger`                    | `console` | `log`, `warn`, `error`. Messages start with `[KafkaTransport](method)`.                                            |

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
Decoding is strict: wrong spec version, missing or empty `ce_id`/`ce_type`/`ce_source`/`ce_time`,
an invalid event type, a non-RFC 3339 time, a non-JSON content type, a missing or non-JSON/UTF-8
value, repeated protocol headers, or a `ce_subject` that disagrees with the record key all make
the record malformed.

Payloads are JSON: `Date`, `bigint`, `undefined` and the like do not survive. Publishing an
`undefined` payload fails (and is retried or dead-lettered by the outbox).

## Partitioning and ordering

The key decides the partition. Events with the same key are consumed in publish order; events
without a key (`key: null`) are spread over partitions and have no ordering. Use the aggregate id
as the key where order matters. Use the same partitioner in every producer of a topic, otherwise
one key can map to different partitions depending on who wrote it.

## Guarantees

- **Publish**: idempotent producer, `acks=-1`; `publish` resolves after the broker ack. A rejected
  publish makes the outbox retry, which can duplicate a message after a lost ack, and consumers
  deduplicate on the event id.
- **Consume**: `eachMessage` awaits the bus (`RelayBus.receive`, one idempotent insert) and the
  client commits the offset only afterwards. A duplicate message is stored once.
- **Handler failure**: retried in-process `processing.maxAttempts` times with backoff and
  heartbeats. If it still fails the error is rethrown to the client. kafkajs then does not commit,
  re-fetches the same record and eventually restarts the consumer (`retry` and `restartOnFailure`
  consumer settings control the pace). The record is never skipped, so a persistent outage blocks
  the partition until the store recovers. That is deliberate: `receive` is a cheap insert, and
  skipping would lose the event. `isConsuming()` is `false` while this is happening.
  We deliberately do not use `pause()` plus `seek()` plus return: the client resolves the offset of
  a returned `eachMessage` before applying the seek, which can commit past the record if the
  process dies in that window.
- **Malformed record**: handled by `onInvalidMessage` and never crashes the loop. A failing
  callback or dead-letter send is retried like a handler failure, so a record is dropped only when
  you chose `'skip'` or your callback returned.
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
stalls and `close()`. kafkajs 2.2.4 is verified by the integration tests. The Confluent
compatibility layer is typed from its documented KafkaJS-compatible API and is not exercised in this
repository; its handling of a rejected `eachMessage` (it re-seeks to the failed record) and of
consumer events should be checked against your version before relying on `isConsuming()`.

## Operations

- **Topics**: create them up front with the partitions and retention you want. Relying on
  auto-creation gives broker defaults (and kafkajs subscribes to a missing topic only after
  auto-creating it). Create the dead letter topic too.
- **One subscription per transport**: `subscribe` can be called once; list every topic in that
  subscription. Create a second `KafkaTransport` (with its own `name` and `groupId`) for a second
  consumer group.
- **Session timeout**: the retry loop heartbeats between attempts. Keep
  `maxAttempts * maxBackoff` well below the consumer `sessionTimeout` (30 s default).
- **Concurrency**: one record at a time per partition (kafkajs default); scale with partitions and
  service replicas in the same group.
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
- `Skipping malformed message` logs: a producer on that topic is not writing CloudEvents binary
  mode with `content-type: application/json`. Use `{ deadLetterTopic }` to inspect the records.
- Events appear twice in logs but handlers run once: expected, the inbox deduplicates on the id.
