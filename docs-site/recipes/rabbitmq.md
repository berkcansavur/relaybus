---
title: RabbitMQ transactional outbox with publisher confirms - relaybus recipe
description: Publish outbox events to RabbitMQ with publisher confirms and consume them with manual acknowledgements and an idempotent inbox, with retry queues and dead-letter queues.
---

# RabbitMQ

The RabbitMQ transport publishes outbox events with broker confirms and consumes them with manual acknowledgements, so an event is never lost between services: it is either delivered, still waiting in the outbox, or in a dead-letter queue.

```sh
pnpm add @relaybus/core @relaybus/rabbitmq amqplib
```

RabbitMQ 3.13 or 4.x is required (quorum queues).

```ts
import { RelayBus } from '@relaybus/core';
import { RabbitMqTransport } from '@relaybus/rabbitmq';

const rabbit = new RabbitMqTransport({
  name: 'rabbit',
  url: process.env.AMQP_URL,
  exchange: 'events',
  consumerGroup: 'billing', // queue: relaybus.billing
});

const bus = new RelayBus({
  store,
  source: 'billing',
  transports: [rabbit],
  routes: [{ event: OrderPaid, transport: 'rabbit', destination: 'billing.order-paid' }],
  subscriptions: [{ transport: 'rabbit', destinations: ['orders.*'] }],
});
```

Events of a [catalog](/concepts/event-catalogs) that share a routing key need one `route(produce(OrderEvents, { events: [...] }), { transport: 'rabbit', destination: 'billing.events' })` instead of a route per event; its optional `wireTypes` name the types for a `jsonEnvelope` codec.

## What you get

- `publish` resolves only after the broker confirmed the message; otherwise the outbox keeps the delivery and retries.
- Messages are acknowledged after the inbox write. A crash before that redelivers; the inbox drops the duplicate.
- Failing handlers are retried through delay-tiered retry queues (about 1 hour by default) so a short database outage dead-letters nothing. Poison messages go to `<queue>.dead`.
- Reconnects with capped, jittered backoff.

Order is not preserved by default. Set `ordered: true` for per-queue ordering at the cost of throughput. Publishing uses the AMQP `mandatory` flag, so start consumers first or declare queues in advance. Details: [@relaybus/rabbitmq](/packages/rabbitmq).
