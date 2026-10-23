---
title: Dead letters - inspect and requeue failed events in relaybus
description: Deliveries that exhaust their retries or throw a non-retryable error become dead letters in relaybus. List them, inspect the last error and requeue them when the cause is fixed.
---

# Dead letters

A delivery becomes **dead** when it exhausts its attempts or its handler throws `NonRetryableError`. Dead letters are kept until you requeue or purge them (`retention.dead` is off by default).

## Inspect and requeue

```ts
const dead = await bus.listDeliveries({ status: 'dead', handler: 'Shipping.ship' });
await bus.requeue(dead.map((delivery) => delivery.id));
```

Each delivery keeps its last error as JSON. `bus.statistics()` reports the number of pending, processing, completed and dead deliveries and the oldest due time. In NestJS, `RelayBusHealth.check()` also reports `lagMilliseconds`, a good alert signal.

## Dead letters and ordering

For a keyed event, a dead-lettered head releases the events behind it. A requeued head runs again; if a later event already ran, the head runs after it.

## Broker dead letters

Transports have their own dead-letter handling for messages that can never succeed:

- Kafka copies malformed records to `<topic>.dead` by default.
- RabbitMQ dead-letters poison messages to `<queue>.dead`.

See [@relaybus/kafka](/packages/kafka) and [@relaybus/rabbitmq](/packages/rabbitmq).

## Related

- [Retries and backoff](/concepts/retries)
- [Deliveries](/concepts/deliveries)
