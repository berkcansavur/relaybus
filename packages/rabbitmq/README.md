# @relaybus/rabbitmq

RabbitMQ transport for [relaybus](https://github.com/berkcansavur/relaybus). It publishes events
from the outbox with broker confirms and consumes them with manual acknowledgements, so an event is
never lost between services: it is either delivered, still waiting in the outbox, or in a
dead-letter queue.

- **Publish** resolves only after the broker confirmed the message was persisted and routed.
- **Consume** acknowledges only after your handler (the relaybus inbox write) succeeded.
- **Reconnects** by itself with capped, jittered backoff and restores channels, topology and the consumer.
- **Poison messages** go to a dead-letter queue after a bounded number of deliveries instead of looping.

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

Readiness probe: `rabbit.isConnected()` is true while the connection is open and publishing and
consuming are set up.

## Options

| Option               | Default                                       | Description                                                                                                                                   |
| -------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`               | `'rabbitmq'`                                  | Name referenced by routes and subscriptions.                                                                                                  |
| `url`                | -                                             | `amqp://` URL or `{ hostname, port, username, password, vhost, heartbeat, protocol }`. Give exactly one of `url` and `connect`.               |
| `connect`            | -                                             | Factory returning an amqplib connection. Called again after every loss. Use for TLS options or credentials.                                   |
| `exchange`           | `'relaybus'`                                  | Durable topic exchange, created when missing.                                                                                                 |
| `prefetch`           | `50`                                          | Unacknowledged messages held by the consumer; also the handler concurrency.                                                                   |
| `queuePrefix`        | `'relaybus'`                                  | Prefix of derived queue names.                                                                                                                |
| `consumerGroup`      | -                                             | Services sharing a group share one queue `<queuePrefix>.<consumerGroup>` and split the messages. Required to subscribe unless `queue` is set. |
| `queue`              | -                                             | Explicit queue name; overrides `queuePrefix` and `consumerGroup`.                                                                             |
| `deadLetterExchange` | `'<exchange>.dead'`                           | Direct exchange poison messages are dead-lettered to.                                                                                         |
| `deadLetterQueue`    | `'<queue>.dead'`                              | Quorum queue that keeps dead-lettered messages.                                                                                               |
| `deliveryLimit`      | `10`                                          | Deliveries before a failing message is dead-lettered (also set as the queue's `x-delivery-limit`).                                            |
| `handlerRetry`       | 3 attempts, 200 ms, x2, 5 s cap, equal jitter | Handler attempts, with backoff, on one delivery before the message is requeued (`RetryPolicyOptions`).                                        |
| `reconnect`          | 500 ms, x2, 30 s cap, equal jitter            | Reconnect backoff (`initialDelay`, `factor`, `maxDelay`, `jitter`; attempts are unlimited), or `false`.                                       |
| `failOnConnectError` | `true`                                        | `connect()` rejects when the broker is unreachable. When `false` it resolves and keeps trying in the background.                              |
| `allowUnroutable`    | `false`                                       | When `true`, publishing a message no queue is bound to resolves (the broker discards it) instead of rejecting.                                |
| `publishTimeout`     | `'30s'`                                       | How long `publish` waits for the confirm.                                                                                                     |
| `shutdownTimeout`    | `'30s'`                                       | How long `close()` waits for running handlers.                                                                                                |
| `logger`             | `console`                                     | `log`/`warn`/`error`.                                                                                                                         |
| `random`             | `Math.random`                                 | Jitter source, for tests.                                                                                                                     |

## Wire format

CloudEvents 1.0 in binary content mode, following the CloudEvents AMQP binding adapted to
RabbitMQ's AMQP 0-9-1:

- Body: the JSON payload, `contentType: application/json`, persistent (`deliveryMode: 2`).
- Message headers: `cloudEvents:specversion`, `cloudEvents:id`, `cloudEvents:type`,
  `cloudEvents:source`, `cloudEvents:time` (ISO 8601, millisecond precision),
  `cloudEvents:datacontenttype`, and when set `cloudEvents:subject` (the event key),
  `cloudEvents:causationid`, `cloudEvents:correlationid`.
- AMQP properties mirror them for broker tooling: `messageId`, `type`, `appId` (source),
  `correlationId`, `timestamp` (seconds).
- Event headers (such as `traceparent`) travel as `relaybus:<name>`, so they cannot collide with
  CloudEvents attributes or broker headers like `x-death`.

The envelope round-trips exactly (`occurredAt` as `Date`, `key`, `causationId`, headers). On receive
the `cloudEvents:` headers win and the AMQP properties are the fallback, so events from other
CloudEvents producers work if they follow the binding.

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
- **No hot loops, no infinite loops.** A failing handler is retried with backoff on the same
  delivery (`handlerRetry`), then the message is requeued. The consumer counts deliveries from the
  `x-delivery-count` / `x-acquired-count` headers the broker stamps on redelivered messages and
  dead-letters the message on the `deliveryLimit`-th failed delivery (the queue's `x-delivery-limit`
  is set too, as a backstop for consumer crashes). Total handler runs for a permanently failing
  message are `handlerRetry.attempts x deliveryLimit`.
- **Immediate dead-lettering** for messages that can never succeed: bodies that are not valid
  CloudEvents (logged at `error`) and handler errors that are `NonRetryableError`s, such as
  `InvalidEnvelopeError` from `bus.receive`. They end in `<queue>.dead` with their headers intact.
- **Reconnection.** On connection loss, a closed channel or a cancelled consumer the transport
  drops the connection, waits with capped, jittered backoff, reconnects and re-declares the
  exchange, queues, bindings and consumer. Handlers still running during the loss finish; their
  acks fail (logged) and the broker redelivers.
- **Ordering.** With `prefetch > 1` handlers run concurrently and redeliveries reorder messages.
  Ordering per key is not guaranteed by this transport; set `prefetch: 1` for strict queue order.

## Topology

For a subscription with `queue = relaybus.billing`, `exchange = relaybus`:

| Object                        | Declaration                                                                                                                            |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `relaybus` exchange           | topic, durable                                                                                                                         |
| `relaybus.dead` exchange      | direct, durable                                                                                                                        |
| `relaybus.billing.dead` queue | quorum, durable, bound to `relaybus.dead` with key `relaybus.billing`                                                                  |
| `relaybus.billing` queue      | quorum, durable, `x-delivery-limit`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, bound to the exchange once per destination |

Declarations are idempotent. Redeclaring a queue with different arguments (for example a new
`deliveryLimit`) makes the broker answer `PRECONDITION_FAILED` and startup fails; delete or
migrate the queue (or use a new `queue` name) when changing those. Removing a destination from a
subscription does not unbind it.

## Operational notes

- **Heartbeats.** amqplib disables heartbeats unless the URL carries `?heartbeat=<seconds>`. Set
  it (30 is typical) so a silently dead TCP connection is noticed; otherwise only `publishTimeout`
  catches it for publishers and consumers can hang.
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

| Symptom                                         | Cause and fix                                                                                                             |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `UnroutableMessageError`                        | No queue is bound to the routing key. Start the consuming service first, check the destination, or set `allowUnroutable`. |
| `PRECONDITION_FAILED` on start                  | A queue or exchange exists with different arguments or type. Align the options or delete the old object.                  |
| `Not connected to the broker`                   | Broker unreachable or reconnecting; the outbox retries. Check `isConnected()` and the `[ConnectionSupervisor]` log lines. |
| Messages pile up in `<queue>.dead`              | Handler errors or malformed messages; the `error` log lines name the event id and reason.                                 |
| Publishes hang until `publishTimeout`           | Broker is blocked (memory or disk alarm) or the connection died without heartbeats.                                       |
| `Needs 'consumerGroup' or 'queue' to subscribe` | Set `consumerGroup` (or `queue`) on the transport that subscribes.                                                        |
