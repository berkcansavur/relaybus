# @relaybus/kafka

@relaybus/kafka is the Apache Kafka transport of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it forwards committed outbox events to Kafka topics and stores consumed events once in an idempotent inbox, so events are not lost between your database and Kafka.

Apache Kafka transport for [relaybus](../../README.md). It forwards committed
outbox events to Kafka topics and feeds events consumed from Kafka into the
bus inbox, with at-least-once delivery and idempotent storage.

- Wire format: [CloudEvents Kafka protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md), binary content mode, so any CloudEvents-aware consumer or tool can read the topic.
- Tested on a real broker with both clients: [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript) 1.11.0 (its KafkaJS-compatible API, **recommended**: maintained, librdkafka based) and [`kafkajs`](https://kafka.js.org) 2.2.4 (unmaintained). See [Choosing a client](#choosing-a-client). Neither client is a dependency; install the one you use.
- Never loses an event: the offset is committed only after the bus stored the event; a failing store blocks the partition instead of skipping.

## Install

```sh
pnpm add @relaybus/core @relaybus/kafka @confluentinc/kafka-javascript
```

## Usage

```ts
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { RelayBus } from '@relaybus/core';
import { KafkaTransport } from '@relaybus/kafka';

const kafka = new KafkaJS.Kafka({ kafkaJS: { clientId: 'orders', brokers: ['kafka:9092'] } });

const transport = new KafkaTransport({
  kafka,
  client: 'confluent', // the default is 'kafkajs'
  groupId: 'billing', // required to subscribe
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

| Option                         | Default             | Meaning                                                                                                                                                                                                                                                                                                                           |
| ------------------------------ | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kafka`                        | required            | A `kafkajs` `Kafka` or Confluent `KafkaJS.Kafka` instance: the factory of the transport's consumer and producer. Optional with `producer`, but then the transport cannot subscribe.                                                                                                                                               |
| `producer`                     | none                | An already-connected producer the application owns (kafkajs `Producer`, Confluent `KafkaJS.Producer`), used to publish and dead-letter instead of one of `kafka`. Never connected or disconnected by the transport. See [Reusing the application's producer](#reusing-the-applications-producer).                                 |
| `client`                       | `'kafkajs'`         | Which client `kafka` is: `'kafkajs'` or `'confluent'`. Set `'confluent'` for a Confluent `KafkaJS.Kafka`; see [Choosing a client](#choosing-a-client).                                                                                                                                                                            |
| `name`                         | `'kafka'`           | Transport name used by routes and subscriptions.                                                                                                                                                                                                                                                                                  |
| `groupId`                      | none                | Consumer group. Required as soon as `subscribe` is used; publish-only services can omit it.                                                                                                                                                                                                                                       |
| `fromBeginning`                | `true`              | Where a group without committed offsets starts. `true` so events published before the first start are not skipped.                                                                                                                                                                                                                |
| `maxInFlightRequests`          | `1`                 | Producer in-flight requests, 1 to 5. `1` keeps retries from reordering events of one key.                                                                                                                                                                                                                                         |
| `producerConfig`               | `{}`                | Extra producer settings (partitioner, compression, ...). `idempotent` and `maxInFlightRequests` always win. Confluent: KafkaJS-style settings in a `kafkaJS` block, librdkafka properties at the top level; `acks: -1` also wins, as do `enable.idempotence`, `max.in.flight` and `acks` there.                                   |
| `consumerConfig`               | `{}`                | Extra consumer settings (`sessionTimeout`, `retry`, `heartbeatInterval`, ...). `groupId` always wins. Confluent: KafkaJS-style settings in a `kafkaJS` block, librdkafka properties at the top level; `fromBeginning` also wins, as do `group.id` and `auto.offset.reset` there. Your `rebalance_cb` is still called.             |
| `processing.maxAttempts`       | `5`                 | In-process attempts per delivery of a record.                                                                                                                                                                                                                                                                                     |
| `processing.initialBackoff`    | `'200ms'`           | Delay before attempt 2, doubling per attempt.                                                                                                                                                                                                                                                                                     |
| `processing.maxBackoff`        | `'5s'`              | Upper bound of the delay between attempts.                                                                                                                                                                                                                                                                                        |
| `processing.heartbeatInterval` | `'1s'`              | Heartbeat interval while sleeping between attempts, so a long backoff never exceeds `sessionTimeout`.                                                                                                                                                                                                                             |
| `startupTimeout`               | `'30s'`             | How long `subscribe` waits for the group join. A failed start-up disconnects the consumer it started.                                                                                                                                                                                                                             |
| `publishTimeout`               | `'30s'`             | How long `publish` waits for the broker ack before rejecting with `PublishTimeoutError`.                                                                                                                                                                                                                                          |
| `restart.initialBackoff`       | `'1s'`              | First delay of the consumer restart loop after an unrecoverable crash; doubles per failure.                                                                                                                                                                                                                                       |
| `restart.maxBackoff`           | `'30s'`             | Upper bound of the restart delay.                                                                                                                                                                                                                                                                                                 |
| `onInvalidMessage`             | `'dead-letter'`     | `'dead-letter'` (to `<topic>.dead`), `{ deadLetterTopic: string \| (sourceTopic) => string }`, a callback `(message) => void \| Promise<void>`, or `'skip'` (drops the record; explicit opt-in).                                                                                                                                  |
| `codec`                        | `cloudEventsBinary` | The wire format, both ways: `cloudEventsBinary` (CloudEvents binary mode, strict on read), `cloudEventsStructured`, `jsonEnvelope({...})` (your JSON envelope, or the bare payload) or `{ encode, decode }`. See [Interoperating with services that do not use relaybus](#interoperating-with-services-that-do-not-use-relaybus). |
| `decode`                       | none                | Deprecated: reads with your function, writes binary mode. Same as `codec: { encode: encodeEnvelope, decode }`; not together with `codec`.                                                                                                                                                                                         |
| `logger`                       | `console`           | `log`, `warn`, `error`. Messages start with `[KafkaTransport](method)`.                                                                                                                                                                                                                                                           |

## Wire format

Value: the JSON payload. Key: the event key (`null` means no key); `jsonEnvelope` can write
another key or none (`recordKey`).

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

## Interoperating with services that do not use relaybus

**relaybus producer, other consumer.** Any Kafka client reads the record as is: the value is the
JSON payload (UTF-8), the key is the event key, and the `ce_*` headers above carry the attributes.
A plain consumer only needs `JSON.parse(value)` and the `ce_type` / `ce_id` headers. CloudEvents
SDKs decode the record natively, because it is CloudEvents binary mode of the Kafka protocol
binding: Java `io.cloudevents:cloudevents-kafka` (`CloudEventDeserializer`), Go `sdk-go`
(`protocol/kafka_sarama`), and Spring Cloud Function / Spring Cloud Stream, which read the
`ce_`-prefixed headers. Use `ce_id` to deduplicate: an outbox delivers at least once.

**Other formats, both ways.** By default the transport reads only binary-mode CloudEvents;
anything else is malformed and dead-lettered, so a misconfigured producer cannot slip events in.
The `codec` option sets another wire format for publishing and consuming (one transport is one
subscription):

```ts
import {
  KafkaTransport,
  cloudEventsStructured,
  encodeEnvelope,
  fromHeader,
  jsonEnvelope,
  plainJsonDecoder,
  type KafkaCodec,
} from '@relaybus/kafka';

// CloudEvents structured mode (`content-type: application/cloudevents+json`); reads binary too.
new KafkaTransport({ kafka, groupId: 'billing', codec: cloudEventsStructured });

// A service envelope: `{ "eventName": ..., "messageId": ..., "data": {...}, "meta": {...} }`.
const envelope = jsonEnvelope({
  type: { body: 'eventName' },
  id: { body: 'messageId' },
  payload: { body: 'data' },
  correlationId: { body: 'meta.correlationId' },
  occurredAt: { body: 'meta.occurredAt' },
  headers: { body: 'meta.headers' },
});

// The bare JSON payload of a Spring / Go / plain kafkajs producer, the type in a header.
const plain = jsonEnvelope({
  type: { header: 'event-type' },
  id: 'record-position', // kafka:<topic>:<partition>:<offset>, or { header: 'message-id' }
  payload: 'whole-body',
});

// Your own: binary mode out, plain JSON and CloudEvents in.
const custom: KafkaCodec = {
  encode: encodeEnvelope,
  decode: plainJsonDecoder({ type: fromHeader('event-type') }),
};
```

| Codec                   | Writes                                                   | Reads                                                                                |
| ----------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `cloudEventsBinary`     | Binary mode, as above. Default, byte for byte unchanged. | Binary mode only, strictly.                                                          |
| `cloudEventsStructured` | One `application/cloudevents+json` document.             | Structured and binary mode (`decodeCloudEvent`).                                     |
| `jsonEnvelope(mapping)` | The mapped JSON body and headers.                        | The same mapping. A missing type, mapped id or payload is malformed.                 |
| `{ encode, decode }`    | Whatever `encode` returns.                               | Whatever `decode` returns; missing fields are filled as for a decoder (table below). |

`jsonEnvelope` mappings name a body path (`{ body: 'meta.correlationId' }`, `'messageType.0'`
indexes an array) or a header (`{ header: 'event-type' }`) for `type`, `id`, `payload` (or
`'whole-body'`), `key`, `source`, `occurredAt`, `correlationId`, `causationId` and `headers`, plus a
`contentType`. `type`, `id` and `payload` are required by the types; `id: 'record-position'` uses the
record position (records relaybus writes carry the event id in a `relaybus-event-id` header, which
wins on read, so outbox retries stay deduplicated); `{ body: 'id', whenMissing: 'record-position' }`
uses the body id when a record has one. The record key is the event key unless `recordKey` says
otherwise (`'none'` or `(event) => string | null`), `contentTypeHeader: false` writes no
`content-type` header, and when `occurredAt` is not mapped the record timestamp carries it. Event headers that are record headers keep their names, so a name the
mapping uses, `content-type` or `relaybus-event-id`, is refused with `UnencodableEventError`. The
[interoperability recipe](https://berkcansavur.github.io/relaybus/recipes/interoperating-with-other-services)
has a full table and a MassTransit example.

Decoded payloads go through the event's schema (`defineEvent(type, { schema })`) as relaybus's own
do; a payload that fails it makes the delivery dead with `EventValidationError`.

The decoders below remain available for a custom codec's `decode`, and for the deprecated `decode`
option:

| Decoder                    | Accepts                                                                                                                                                                                                                                        |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| default (`decodeEnvelope`) | Binary-mode CloudEvents only, strictly as described above. Unchanged.                                                                                                                                                                          |
| `decodeCloudEvent`         | Binary mode as the default, plus structured mode: a JSON value with `specversion` `1.0`, `id`, `type`, `source`, optional `time` (else the record timestamp), `subject` (else the record key), `data` (the payload; `data_base64` is refused). |
| `plainJsonDecoder(opts)`   | Any JSON value. A record that is a CloudEvent anyway (a `ce_specversion` header or a structured content type) goes through `decodeCloudEvent`, so relaybus and foreign producers can share a topic.                                            |
| your function              | `(record) => ({ id, type, payload, ...optional })`. Missing fields are filled: `source` = topic, `key` = record key, `occurredAt` = record timestamp, `headers` = `{}`.                                                                        |

`plainJsonDecoder` options (each extractor gets `{ topic, partition, offset, key, headers, payload, timestamp }`;
an extractor returning `undefined` or `''` selects the default):

| Option       | Default                              | Meaning                                                                                                                        |
| ------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `type`       | required                             | A fixed event type, or an extractor such as `fromHeader('event-type')` or `fromPayloadField('type')`.                          |
| `id`         | `kafka:<topic>:<partition>:<offset>` | Inbox dedup id. The default is stable across redeliveries of the record. Prefer a producer id when it may send an event twice. |
| `key`        | the record key                       | Ordering key. A key that is not UTF-8 (Java `LongSerializer`) is base64-encoded.                                               |
| `source`     | the topic                            | Fixed text or an extractor.                                                                                                    |
| `occurredAt` | the record timestamp                 | Extractor returning a `Date`.                                                                                                  |

Every record header becomes an event header (the last value of a repeated header; a value that is
not valid UTF-8 is left out, so binary ids can never collide).

**Guarantees stay the same.** Decoding runs again on every redelivery and must be deterministic;
the inbox deduplicates on the id, so a redelivered record is stored and handled once. Under every
codec, a record that cannot be decoded (not JSON, a tombstone, no type, an invalid type, a missing
mapped id, an extractor returning a non-string) and anything a custom `decode` throws go to
`onInvalidMessage` (dead-letter by default)
with the reason in `relaybus-invalid-reason`; the consumer keeps running. Per-key order is the
Kafka partition order, as for relaybus producers.

**Limits.** The derived id assumes offsets are never reused: deleting and recreating a topic
restarts its offsets, so records after a recreation could collide with ids already in the inbox
(within its retention). Two sends of one event by a producer are two offsets; only an id set by the
producer (`id: fromHeader(...)` or `{ header }` / `{ body }` in a mapping) dedupes them. Avro or
Protobuf need your own codec, whose functions must stay synchronous.

## Partitioning and ordering

The key decides the partition. Events with the same key are consumed in publish order; events
without a key (`key: null`) are spread over partitions and have no ordering. Use the aggregate id
as the key where order matters. Use the same partitioner in every producer of a topic, otherwise
one key can map to different partitions depending on who wrote it.

The record key is the event key by default. A `jsonEnvelope` codec with `recordKey: 'none'`
writes records without a key, as many existing producers do, and `recordKey: (event) => ...`
derives another one. The event key, and the outbox's per-key order, stay as they are: relaybus
still publishes events of one key one after the other. But a record without a key is placed on a
partition by the producer (round-robin or sticky), so events of one key can land on different
partitions and consumers may see them out of order. Keep the event key on the wire wherever
consumers rely on per-key order.

## Guarantees

- **Publish**: idempotent producer, `acks=-1`; `publish` resolves after the broker ack and rejects
  with `PublishTimeoutError` after `publishTimeout` (kafkajs would otherwise retry an idempotent
  send for as long as the broker is unreachable). A rejected publish makes the outbox retry; a
  timed-out send may still land, so a message can be duplicated, and the idempotent producer plus
  the consumer's dedup on the event id absorb it. Pass `producerConfig: { retry: { retries } }`
  (Confluent: `producerConfig: { kafkaJS: { retry: { retries } } }`) to bound client-side retries
  further.
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
- **Dead consumer** (kafkajs; Confluent never gives up on a consumer): the transport does not rely
  on the client to restart itself. When the client
  reports a crash with `restart=false`, the transport logs it and reconnects, re-subscribes and
  re-runs the consumer with bounded backoff (`restart.*`) until it joined again or the transport
  closed. `isReady()` is `false` meanwhile.
  We deliberately do not use `pause()` plus `seek()` plus return: the client resolves the offset of
  a returned `eachMessage` before applying the seek, which can commit past the record if the
  process dies in that window.
- **Malformed or rejected record**: a record that cannot be decoded, or that the bus rejects with
  any `NonRetryableError` (such as `InvalidEnvelopeError` or `EventValidationError`), is handled by
  `onInvalidMessage`. The default copies the untouched record to `<topic>.dead` with its original
  headers plus `relaybus-invalid-reason`, `relaybus-source-topic`, `relaybus-source-partition`,
  `relaybus-source-offset`, `relaybus-error-name` (the error class, such as
  `InvalidKafkaMessageError`) and, for relaybus errors, `relaybus-error-code` (such as
  `RELAYBUS_KAFKA_INVALID_MESSAGE`, see [Error codes](https://berkcansavur.github.io/relaybus/concepts/error-codes)),
  using the same idempotent producer, and commits only after the ack. The log line carries the
  same `errorName=` and `code=` fields.
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
| Confluent consumer config  | `rebalance_cb(error, assignment)` reports joins (`-175`, assigned) and rebalances (`-174`, revoked)      |
| `eachMessage` payload      | `topic`, `partition`, `message.{ key, value, headers, offset }`, `heartbeat()`                           |
| consumer events (optional) | `consumer.on(name, listener)` and `consumer.events.{ GROUP_JOIN, CRASH, REBALANCING, STOP, DISCONNECT }` |

With Confluent, `send` gets no `acks` and `subscribe` no `fromBeginning` (it rejects both; they are
set on the producer and consumer instead), and `on`/`events` are not used.

`on`/`events` are optional for other kafkajs-like clients: without `GROUP_JOIN` the transport cannot
wait for the join or observe crashes, so `subscribe` resolves when `run` returns and
`isConsuming()` only reflects processing stalls and `close()`. kafkajs 2.2.4 and
`@confluentinc/kafka-javascript` 1.11.0 are verified by the integration tests.

## Reusing the application's producer

An application that already has a connected producer, with its SSL and SASL settings, can hand it
to the transport instead of letting it open a second connection:

```ts
const transport = new KafkaTransport({
  producer: ordersService.producer, // already connected, owned by the application
  client: 'confluent', // when it is a Confluent producer
});
```

Without `kafka` the transport only publishes; give `kafka` and `groupId` as well to subscribe (the
consumer stays the transport's own). The transport calls only `send` on the injected producer: it
never connects or disconnects it, so it is usable before `bus.start()` and stays open after
`bus.stop()`; closing it is the application's job, after the bus stopped. The guarantees hold
only if that producer is configured like the transport's own: idempotent, `acks` -1 (all in-sync
replicas) and at most 5 in-flight requests (1 keeps retries from reordering a key). For kafkajs
that is `kafka.producer({ idempotent: true, maxInFlightRequests: 1 })`, for Confluent
`kafka.producer({ kafkaJS: { idempotent: true, acks: -1, maxInFlightRequests: 1 } })`. Because the
transport cannot apply `producerConfig` or `maxInFlightRequests` to it, giving them together with
`producer` is a configuration error.

## Choosing a client

**Use `@confluentinc/kafka-javascript`** (`client: 'confluent'`): it is maintained and built on
librdkafka, while `kafkajs` is no longer maintained. Every integration scenario runs against a real
broker with both clients (publish and subscribe, retries and redelivery, dead letters, codecs,
per-key order across partitions, start-up failure, close, interoperability and two services
through the bus); Confluent is tested with 1.11.0, the lowest version the peer range allows.

What the transport does differently for Confluent, because its KafkaJS-compatible API differs:

- `idempotent`, `maxInFlightRequests` and `acks: -1` go in the producer's `kafkaJS` block, and
  `groupId` and `fromBeginning` in the consumer's: Confluent rejects them at the top level, in
  `send` and in `subscribe`. They are also set as librdkafka properties (`enable.idempotence`,
  `max.in.flight`, `acks`, `group.id`, `auto.offset.reset`), which Confluent applies after the
  block. Your own `kafkaJS` block and properties are merged below them.
- The group join and rebalances are followed through `rebalance_cb` (Confluent has no
  instrumentation events), so `subscribe` still waits for the join and `isConsuming()` still turns
  `false` during a rebalance.
- Confluent never gives up on a consumer: a failed `eachMessage` makes it seek back and redeliver
  the record, and it reports no crash. The restart options therefore never apply.
- `heartbeat()` is a no-op there; librdkafka heartbeats in the background. Keep the time a record
  can spend in retries (`maxAttempts * maxBackoff`) well below `max.poll.interval.ms` (5 minutes by
  default) instead of `sessionTimeout`.
- Its producer partitions keys with librdkafka's `murmur2_random`, which librdkafka documents as
  Java compatible, like kafkajs' `DefaultPartitioner`. `createPartitioner` does not exist there.

Moving from kafkajs: create the client with `new KafkaJS.Kafka({ kafkaJS: { brokers, clientId } })`,
add `client: 'confluent'`, and move KafkaJS-style settings in `producerConfig` and `consumerConfig`
into a `kafkaJS` block (drop `createPartitioner`). The wire format is unchanged.

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
- **Concurrency**: one record at a time per partition (the default of both clients); scale with partitions and
  service replicas in the same group. With `partitionsConsumedConcurrently > 1` in `consumerConfig`
  the stall state is tracked per partition.
- **Retention vs. inbox**: Kafka may redeliver old records after rebalances or offset resets. The
  bus deduplicates on the event id for as long as it keeps completed deliveries
  (`retention.completed`, default 7 days), so keep that longer than your redelivery window.
- **Partitioner warning** (kafkajs 2.x only): pass `createPartitioner: Partitioners.DefaultPartitioner`
  in `producerConfig` to silence the legacy partitioner warning.

## Troubleshooting

- `did not join group ... within 30000ms` at start: broker unreachable, wrong credentials, or the
  group is rebalancing for longer than `startupTimeout`. Check the client's connection settings.
- `isConsuming()` stays `false` with `Giving up for now` logs: `RelayBus.receive` keeps failing
  (database down?). Fix the store; consumption resumes by itself.
- `Dead-lettered malformed message` logs: a producer on that topic is not writing CloudEvents binary
  mode with `content-type: application/json`. Inspect `<topic>.dead` and its `relaybus-invalid-reason`.
- `Consumer crashed ... restart=false` followed by `Consumer restarted`: the client gave up and the
  transport recovered it (kafkajs only). Repeated `Restart failed` logs mean the broker or credentials are the problem.
- `PublishTimeoutError`: no broker ack within `publishTimeout`; the outbox retries.
- Events appear twice in logs but handlers run once: expected, the inbox deduplicates on the id.

## When to use @relaybus/kafka, and when not to

Use it to send events between services through Kafka without a dual write between your database and the broker. It is a transport, not a store: it needs `@relaybus/core`, a store and one of the two clients, `@confluentinc/kafka-javascript` (recommended) or `kafkajs`.

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
