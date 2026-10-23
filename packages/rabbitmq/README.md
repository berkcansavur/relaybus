# @relaybus/rabbitmq

@relaybus/rabbitmq is the RabbitMQ transport of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: it publishes outbox events with publisher confirms and consumes them with manual acknowledgements and an idempotent inbox.

RabbitMQ transport for [relaybus](https://github.com/berkcansavur/relaybus). It publishes events
from the outbox with broker confirms and consumes them with manual acknowledgements, so an event is
never lost between services: it is either delivered, still waiting in the outbox, or in a
dead-letter queue.

- **Publish** resolves only after the broker confirmed the message was persisted and routed.
- **Consume** acknowledges only after your handler (the relaybus inbox write) succeeded.
- **Reconnects** by itself with capped, jittered backoff and restores channels, topology and the consumer.
- **Failing handlers** are retried with growing delays (default: up to one hour) through retry queues, so a short outage of your database never dead-letters an event; **poison messages** go to a dead-letter queue immediately.

## Install

```bash
pnpm add @relaybus/rabbitmq @relaybus/core amqplib
```

`amqplib` is a peer dependency (`>=0.10`). RabbitMQ 3.13 or 4.x is required (quorum queues).

## Usage

```ts
import { RelayBus } from '@relaybus/core';
import { RabbitMqTransport } from '@relaybus/rabbitmq';

const rabbit = new RabbitMqTransport({
  name: 'rabbit',
  url: 'amqp://user:password@broker:5672/vhost?heartbeat=30',
  exchange: 'events',
  consumerGroup: 'billing', // queue: relaybus.billing
});

const bus = new RelayBus({
  store,
  source: 'billing',
  transports: [rabbit],
  // Outbound: committed OrderPaid events are published to the exchange with this routing key.
  routes: [{ event: OrderPaid, transport: 'rabbit', destination: 'billing.order-paid' }],
  // Inbound: routing-key patterns bound to this service's queue.
  subscriptions: [{ transport: 'rabbit', destinations: ['orders.*'] }],
});

await bus.start(); // connects; rejects if the broker is unreachable
```

A destination is a routing key when publishing and a binding pattern when subscribing
(`orders.*`, `orders.#`, `order.placed.v1`).

Readiness probes: `rabbit.isReady()` is true while the transport can publish and, once subscribed,
is consuming (connection up, channels open, consumer registered). `rabbit.isConnected()` is the
narrower check of the connection alone. relaybus health checks use `isReady()`.

## Options

| Option               | Default                                           | Description                                                                                                                                                                                                                                                                                             |
| -------------------- | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`               | `'rabbitmq'`                                      | Name referenced by routes and subscriptions.                                                                                                                                                                                                                                                            |
| `url`                | -                                                 | `amqp://` URL or `{ hostname, port, username, password, vhost, heartbeat, protocol }`. Give exactly one of `url` and `connect`.                                                                                                                                                                         |
| `connect`            | -                                                 | Factory returning an amqplib connection. Called again after every loss. Use for TLS options or credentials.                                                                                                                                                                                             |
| `exchange`           | `'relaybus'`                                      | Durable topic exchange, created when missing.                                                                                                                                                                                                                                                           |
| `prefetch`           | `50` (`1` when `ordered`)                         | Unacknowledged messages held by the consumer; also the handler concurrency.                                                                                                                                                                                                                             |
| `queuePrefix`        | `'relaybus'`                                      | Prefix of derived queue names.                                                                                                                                                                                                                                                                          |
| `consumerGroup`      | -                                                 | Services sharing a group share one queue `<queuePrefix>.<consumerGroup>` and split the messages. Required to subscribe unless `queue` is set.                                                                                                                                                           |
| `queue`              | -                                                 | Explicit queue name; overrides `queuePrefix` and `consumerGroup`.                                                                                                                                                                                                                                       |
| `deadLetterExchange` | `'<exchange>.dead'`                               | Direct exchange poison messages are dead-lettered to.                                                                                                                                                                                                                                                   |
| `deadLetterQueue`    | `'<queue>.dead'`                                  | Quorum queue that keeps dead-lettered messages.                                                                                                                                                                                                                                                         |
| `deliveryLimit`      | `10`                                              | Quorum queue `x-delivery-limit`: a backstop that makes the broker dead-letter a message that crashes its consumer again and again. Failing handlers do not use it.                                                                                                                                      |
| `handlerRetry`       | 3 attempts, 200 ms, x2, 5 s cap, equal jitter     | Handler attempts, with short backoff, on one delivery before the message is scheduled for delayed redelivery (`RetryPolicyOptions`).                                                                                                                                                                    |
| `redelivery`         | 2 s, x2, 5 min cap, equal jitter, `maxAge` `'1h'` | Delayed redelivery after `handlerRetry`: `initialDelay`, `factor`, `maxDelay`, `jitter`, and `maxAge`, how long a message may keep failing before it is dead-lettered.                                                                                                                                  |
| `ordered`            | `false`                                           | Consume strictly in order: single active consumer, one message at a time, failing messages retried in place. See Ordering.                                                                                                                                                                              |
| `reconnect`          | 500 ms, x2, 30 s cap, equal jitter                | Reconnect backoff (`initialDelay`, `factor`, `maxDelay`, `jitter`; attempts are unlimited), or `false`.                                                                                                                                                                                                 |
| `failOnConnectError` | `true`                                            | `connect()` rejects when the broker is unreachable. When `false` it resolves and keeps trying in the background.                                                                                                                                                                                        |
| `allowUnroutable`    | `false`                                           | When `true`, publishing a message no queue is bound to resolves (the broker discards it) instead of rejecting.                                                                                                                                                                                          |
| `publishTimeout`     | `'30s'`                                           | How long `publish` waits for the confirm.                                                                                                                                                                                                                                                               |
| `shutdownTimeout`    | `'30s'`                                           | How long `close()` waits for running handlers.                                                                                                                                                                                                                                                          |
| `decode`             | strict binary mode                                | How consumed messages become events. `decodeCloudEvent` (binary and structured mode), `plainJsonDecoder({...})` (plain JSON from producers without relaybus) or your own function. See [Interoperating with services that do not use relaybus](#interoperating-with-services-that-do-not-use-relaybus). |
| `logger`             | `console`                                         | `log`/`warn`/`error`.                                                                                                                                                                                                                                                                                   |
| `random`             | `Math.random`                                     | Jitter source, for tests.                                                                                                                                                                                                                                                                               |

## Wire format

CloudEvents 1.0 in binary content mode, following the CloudEvents AMQP binding adapted to
RabbitMQ's AMQP 0-9-1:

- Body: the JSON payload, `contentType: application/json`, persistent (`deliveryMode: 2`).
- Message headers: `cloudEvents:specversion`, `cloudEvents:id`, `cloudEvents:type`,
  `cloudEvents:source`, `cloudEvents:time` (RFC 3339, millisecond precision),
  `cloudEvents:datacontenttype`, and when set `cloudEvents:subject` (the event key),
  `cloudEvents:causationid`, `cloudEvents:correlationid`.
- AMQP properties mirror them for broker tooling: `messageId`, `type`, `appId` (source),
  `correlationId`, `timestamp` (seconds).
- Event headers (such as `traceparent`) travel as `relaybus:<name>`, so they cannot collide with
  CloudEvents attributes or broker headers like `x-death`.

The envelope round-trips exactly (`occurredAt` as `Date`, `key`, `causationId`, headers, including a
header literally named `__proto__`). On receive the `cloudEvents:` headers win and the AMQP properties
are the fallback, so events from other CloudEvents producers work if they follow the binding. The
time must be strict RFC 3339 (`2026-01-01T10:20:30.456Z`, offsets allowed); the AMQP `timestamp`
is used only when the `cloudEvents:time` header is absent, never to paper over a malformed one.

**Payload validation.** The payload must survive `JSON.stringify` unchanged, like on the Kafka
transport. `publish` rejects with `UnserializablePayloadError` (a `NonRetryableError`, so the outbox
does not retry it forever) for `undefined`, functions, symbols, BigInt, NaN and Infinity, array
holes, symbol keys, cycles, and anything JSON would silently convert: `Date`, `Map`, `Set`,
typed arrays and class instances. Convert such values to plain data first. Use `null` for "no payload".

**AMQP limits.** AMQP 0-9-1 short strings are limited to 255 bytes (UTF-8). The event id (also the
AMQP `messageId`, which matches broker returns), every header name (`relaybus:<name>`, so at most
246 bytes for your own names) and the destination (routing key) must fit; otherwise `publish`
rejects with `AmqpLimitExceededError` (a `NonRetryableError`) instead of failing on every retry.
`type`, `appId` and `correlationId` are mirrored into AMQP properties only when they fit; the
`cloudEvents:` headers always carry them, so long values lose only the broker-tooling mirror.

## Interoperating with services that do not use relaybus

**relaybus producer, other consumer.** Bind a queue to the exchange and read the message as is: the
body is the JSON payload (UTF-8), the `messageId` and `type` properties carry the event id and type,
and the `cloudEvents:*` headers carry every attribute. A plain amqplib, Spring AMQP, Go or Python
consumer only needs to parse the body; deduplicate on `messageId`, because an outbox delivers at
least once. Event headers are prefixed: `traceparent` arrives as `relaybus:traceparent`.

**Other producer, relaybus consumer.** By default the transport accepts only binary-mode
CloudEvents; anything else is malformed and dead-lettered, so a misconfigured producer cannot slip
events in. Opt in per transport (one transport is one queue) with `decode`:

```ts
import {
  RabbitMqTransport,
  decodeCloudEvent,
  fromHeader,
  fromPayloadField,
  plainJsonDecoder,
  type RabbitMqMessageDecoder,
} from '@relaybus/rabbitmq';

// A plain amqplib / Spring AMQP producer: JSON body, event type in the `type` property.
const transport = new RabbitMqTransport({
  url,
  consumerGroup: 'billing',
  decode: plainJsonDecoder({ type: (message) => message.properties.type as string | undefined }),
});

// The type in a header, the ordering key in the payload.
const byHeader = plainJsonDecoder({
  type: fromHeader('event-type'),
  key: fromPayloadField('orderId'),
});

// A CloudEvents SDK producer in structured mode (`contentType: application/cloudevents+json`).
const cloudEvents: RabbitMqMessageDecoder = decodeCloudEvent;
```

| Decoder                  | Accepts                                                                                                                                                                                                                                             |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| default                  | Binary-mode CloudEvents only, as described above. Unchanged.                                                                                                                                                                                        |
| `decodeCloudEvent`       | Binary mode as the default, plus structured mode: a JSON body with `specversion` `1.0`, `id`, `type`, `source`, optional `time` (else the `timestamp` property, else receipt), `subject` (the key), `data` (the payload; `data_base64` is refused). |
| `plainJsonDecoder(opts)` | Any JSON body. A message that is a CloudEvent anyway (a `cloudEvents:specversion` header or a structured content type) goes through `decodeCloudEvent`, so relaybus and foreign producers can feed one queue.                                       |
| your function            | `(message) => ({ id, type, payload, ...optional })`. Missing fields are filled: `source` = `appId` (else routing key), `key` = null, `occurredAt` = `timestamp` property (else receipt), `headers` = `{}`.                                          |

`plainJsonDecoder` options (each extractor gets `{ routingKey, properties, headers, payload }`; an
extractor returning `undefined` or `''` selects the default):

| Option       | Default                                | Meaning                                                                                                                                       |
| ------------ | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`       | required                               | A fixed event type, or an extractor such as `fromHeader('event-type')`, `fromPayloadField('type')` or `(m) => m.properties.type`.             |
| `id`         | the `messageId` property               | Inbox dedup id. A message with neither an extracted id nor a `messageId` is malformed: AMQP has no other identity stable across redeliveries. |
| `key`        | none                                   | Ordering key, for example `fromHeader('customer')`.                                                                                           |
| `source`     | the `appId` property, else routing key | Fixed text or an extractor.                                                                                                                   |
| `occurredAt` | the `timestamp` property, else receipt | Extractor returning a `Date`.                                                                                                                 |

String message headers become event headers, except the broker's and the transport's own
(`x-first-death-*`, `x-last-death-*`, `x-relaybus-*`; non-string values such as `x-death` are skipped).

**Guarantees stay the same.** Decoding runs again on every redelivery (also after a retry-queue
round trip, which keeps `messageId` and restores the original routing key from the
`x-relaybus-routing-key` header it adds to retry copies) and must be deterministic; the inbox deduplicates on the id, so
a redelivered message is stored and handled once. A message that cannot be decoded (not JSON, no id,
no type, an invalid type, an extractor returning a non-string) and anything a custom decoder throws
are dead-lettered (`x-relaybus-dead-letter-reason: malformed message`, the detail is logged) and the
consumer keeps running. Messages are handled in queue order only with `ordered: true`.

**Give producers a `messageId`.** Spring AMQP generates one when the message converter has
`setCreateMessageIds(true)`; amqplib takes the `messageId` publish option. Without it, configure `id` to read a business id from a header or the
payload.

## Guarantees

- **No silent loss on publish.** `publish` rejects (never resolves) when the transport is
  disconnected or closed (`RabbitMqUnavailableError`), the broker nacks
  (`RabbitMqPublishRejectedError`), the confirm does not arrive within `publishTimeout`
  (`RabbitMqUnavailableError`) or no queue is bound to the routing key (`UnroutableMessageError`).
  The outbox keeps the delivery and retries it. A rejection after the broker already stored the
  message (lost confirm, timeout) leads to a duplicate on retry; consumers deduplicate on the event id.
- **Mandatory publishing trade-off.** Messages are published with the AMQP `mandatory` flag so an
  event nobody is bound to is reported instead of vanishing. The cost is that a publisher fails
  until at least one consumer queue is bound to the routing key (start consumers first, or
  declare queues in advance). Set `allowUnroutable: true` for fire-and-forget topics where "no
  subscriber" is fine.
- **Acknowledge after handling.** A message is acknowledged after `onMessage` resolved, which in
  relaybus means the event is in the inbox. A crash before that redelivers it; the inbox drops the duplicate.
- **Outages are waited out, not burned through.** A failing handler is retried on the same delivery
  with short backoff (`handlerRetry`). If it still fails, a copy of the message is published to a
  retry queue and the original is acknowledged **only after the broker confirmed the copy**
  (publisher confirms, mandatory flag), so the message is always in exactly one durable place at
  least. When the copy's delay is over, the broker moves it back to the main queue. Delays grow
  by `redelivery.factor` up to `redelivery.maxDelay`, with jitter. With the defaults a message is
  retried at about 2, 4, 8, 16, 32, 64, 128, 256 s, then every 5 min, and dead-lettered after 1 h
  of failing (`redelivery.maxAge`), about 18 redeliveries of 3 handler attempts each. A multi-minute
  database outage therefore delays events and loses nothing. If the copy cannot be confirmed, the
  original is requeued (after the same delay), never acknowledged.
- **Delay tiers.** Each distinct delay is its own quorum retry queue (`<queue>.retry.<ms>`, nine with
  the defaults) with a queue-level TTL, so messages in one queue expire in arrival order; one queue
  with per-message TTLs would let a long delay block shorter ones. The jittered delay is rounded to
  the tier below or above with the probability that keeps its average, so a burst of failures still
  spreads across tiers. A retry copy carries `x-relaybus-redeliveries` and
  `x-relaybus-first-failure-at`, which makes the window independent of which replica handles it.
- **Immediate dead-lettering** for messages that can never succeed: bodies that are not valid
  CloudEvents (logged at `error`) and handler errors that are `NonRetryableError`s, such as
  `InvalidEnvelopeError` from `bus.receive`. The consumer publishes a confirmed copy to the
  dead-letter exchange, then acknowledges the original; the copy keeps the message headers, drops
  the retry state (so replaying it starts a fresh retry window) and adds
  `x-relaybus-dead-letter-reason`. If that publish fails the consumer rejects the message without
  requeue and the broker dead-letters it (at-least-once strategy), so it still ends in `<queue>.dead`.
- **Broker-level backstop.** `x-delivery-limit` (`deliveryLimit`) dead-letters a message that keeps
  coming back because the consumer or its channel dies while holding it.
- **Reconnection.** On connection loss, a closed channel or a cancelled consumer the transport
  drops the connection, waits with capped, jittered backoff, reconnects and re-declares the
  exchange, queues, bindings and consumer. Handlers still running during the loss finish; their
  acks fail (logged) and the broker redelivers.
- **Ordering.** By default order is **not** preserved: `prefetch` handlers run concurrently,
  several replicas compete on the queue, and a delayed redelivery overtakes later messages, so
  `EventEnvelope.key` order does not hold across the broker. Handlers must tolerate reordering
  (versioned state, "ignore older" checks). When per-key order matters, set `ordered: true`: the queue
  gets `x-single-active-consumer` (one replica consumes, the others wait as standby and take over
  when it goes away), prefetch is 1, and a failing message is retried **in place** with the
  `redelivery` delays instead of moving behind newer ones, which blocks the queue until it succeeds or
  `redelivery.maxAge` passes and it is dead-lettered. The cost is throughput: one handler call at a
  time for the whole consumer group, and one stuck message stalls everything behind it. Order is
  queue-wide (a superset of per-key order) and holds for messages published by one producer on one
  connection; the broker cannot order across publishers.

## Topology

For a subscription with `queue = relaybus.billing`, `exchange = relaybus`:

| Object                        | Declaration                                                                                                                                                                                                                                              |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `relaybus` exchange           | topic, durable                                                                                                                                                                                                                                           |
| `relaybus.dead` exchange      | direct, durable                                                                                                                                                                                                                                          |
| `relaybus.billing.dead` queue | quorum, durable, bound to `relaybus.dead` with key `relaybus.billing`                                                                                                                                                                                    |
| `relaybus.billing.retry.<ms>` | one quorum queue per delay tier (not when `ordered`): `x-message-ttl`, dead-lettered through the default exchange to `relaybus.billing`, `x-dead-letter-strategy: at-least-once`, `x-overflow: reject-publish`                                           |
| `relaybus.billing` queue      | quorum, durable, `x-delivery-limit`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-dead-letter-strategy: at-least-once`, `x-overflow: reject-publish`, `x-single-active-consumer` when `ordered`; bound to the exchange once per destination |

`at-least-once` dead-lettering makes the quorum queue keep a message until the dead-letter target
confirmed it, so expiry, rejection and the delivery limit cannot lose messages while the target is
unavailable. RabbitMQ requires `reject-publish` overflow with it. Queues are unbounded by default,
so this only matters if you add a `max-length` policy: then publishers are refused instead of the
oldest message being dropped.

Declarations are idempotent. Redeclaring a queue with different arguments makes the broker answer
`PRECONDITION_FAILED` and `subscribe`/startup rejects; the transport never alters an existing queue.
Removing a destination from a subscription does not unbind it.

### Migrating an existing queue

Queues created by a pre-release version of this package lack `x-dead-letter-strategy` and
`x-overflow`, and queues created with another `ordered` setting differ in `x-single-active-consumer`.
RabbitMQ cannot change these arguments in place, so either:

1. use a new `queue` / `consumerGroup` name, let both queues receive events (the new one is bound by
   the new deployment), and delete the old queue once it is empty; or
2. stop the consumers, drain or shovel the old queue (`rabbitmqctl shovel` / the Shovel plugin) into
   the exchange, delete it (`rabbitmqctl delete_queue <name>`), and start the new version.

The dead-letter queue and exchange are unchanged and need no migration.

## Operational notes

- **Heartbeats.** When the URL carries no `heartbeat`, amqplib adopts the interval the broker
  proposes (RabbitMQ defaults to 60 s), in both the 0.10 and the 2.x range, so a dead TCP connection
  is noticed after roughly two intervals. Set `?heartbeat=30` (or `heartbeat` in the settings object)
  to pick a shorter interval; the lower of the two values is negotiated. Beware `heartbeat=0`: from
  amqplib 2.0 it explicitly **disables** heartbeats (before 2.0 it meant "use the server's value"), and
  without heartbeats only `publishTimeout` catches a silently dead connection for publishers while
  consumers can hang. Do not set it to 0.
- **Dead-letter queue.** Monitor `<queue>.dead`. To replay, move messages back to the exchange
  with the Shovel plugin or a script; the headers (`cloudEvents:*`) are intact and the inbox deduplicates by id.
- **One subscription per transport.** A transport owns one consumer group's queue. To consume
  several groups from one process create several `RabbitMqTransport` instances with different
  `name` and `consumerGroup`.
- **Startup.** `failOnConnectError: true` (default) makes `bus.start()` fail fast when the broker
  is down, so orchestrators restart the pod. Use `false` to boot degraded; publishes are rejected
  and retried by the outbox until the broker is back.
- **TLS.** Pass `connect: () => amqplib.connect('amqps://...', { ca: [...] })`.
- **Consumer scaling.** Add replicas with the same `consumerGroup`; they compete on one queue.

## Troubleshooting

| Symptom                                                 | Cause and fix                                                                                                                                                                      |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `UnroutableMessageError`                                | No queue is bound to the routing key. Start the consuming service first, check the destination, or set `allowUnroutable`.                                                          |
| `PRECONDITION_FAILED` on start                          | A queue or exchange exists with different arguments or type. Align the options or delete the old object.                                                                           |
| `Not connected to the broker`                           | Broker unreachable or reconnecting; the outbox retries. Check `isConnected()` and the `[ConnectionSupervisor]` log lines.                                                          |
| Messages pile up in `<queue>.dead`                      | Handler errors, malformed messages or a dependency that stayed down past `redelivery.maxAge`; the `error` log lines and the `x-relaybus-dead-letter-reason` header name the cause. |
| `UnserializablePayloadError` / `AmqpLimitExceededError` | The event can never be published as is: fix the payload (plain JSON data only) or shorten the id, header name or routing key.                                                      |
| Events arrive out of order                              | Expected without `ordered: true`; see Ordering.                                                                                                                                    |
| `isReady()` is false but `isConnected()` is true        | The consumer was cancelled (queue deleted, node failover) and is being restored, or the publisher channel closed.                                                                  |
| Publishes hang until `publishTimeout`                   | Broker is blocked (memory or disk alarm) or the connection died without heartbeats.                                                                                                |
| `Needs 'consumerGroup' or 'queue' to subscribe`         | Set `consumerGroup` (or `queue`) on the transport that subscribes.                                                                                                                 |

## When to use @relaybus/rabbitmq, and when not to

Use it to send events between services through RabbitMQ with publisher confirms, retry queues and dead-letter queues. It needs RabbitMQ 3.13 or 4.x (quorum queues) and does not preserve order unless you set `ordered: true`.

Documentation: <https://berkcansavur.github.io/relaybus/packages/rabbitmq>

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
