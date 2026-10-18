---
title: Send events between services with Kafka without losing them - relaybus recipe
description: Use relaybus routes and subscriptions with the Kafka transport to publish committed outbox events to a topic and consume them idempotently in another service, avoiding lost events between database and Kafka.
---

# Kafka between services

Service A writes an event in its database transaction. relaybus forwards it to a Kafka topic after commit. Service B stores received events once by id, then runs its handlers. The database-to-Kafka gap is covered by the outbox, and the Kafka-to-handler gap by the inbox.

```sh
pnpm add @relaybus/core @relaybus/kafka kafkajs
```

## Service A: route committed events to a topic

```ts
import { Kafka } from 'kafkajs';
import { RelayBus } from '@relaybus/core';
import { KafkaTransport } from '@relaybus/kafka';

const transport = new KafkaTransport({
  kafka: new Kafka({ clientId: 'orders', brokers: ['kafka:9092'] }),
});

const bus = new RelayBus({
  store,
  source: 'orders',
  transports: [transport],
  routes: [{ event: OrderPlaced, transport: 'kafka', destination: 'orders.events' }],
});
```

## Service B: subscribe and handle

```ts
const transport = new KafkaTransport({
  kafka: new Kafka({ clientId: 'billing', brokers: ['kafka:9092'] }),
  groupId: 'billing',
});

const bus = new RelayBus({
  store,
  source: 'billing',
  transports: [transport],
  subscriptions: [{ transport: 'kafka', destinations: ['orders.events'] }],
});
bus.subscribe(OrderPlaced, async (event) => createInvoice(event.payload.orderId), {
  id: 'billing.invoice',
});
```

## What you get

- **Publish**: an idempotent producer with `acks=-1`; the route is retried until the broker acknowledges (unlimited attempts by default).
- **Consume**: the offset is committed only after the event is stored in the inbox, so a crash redelivers instead of losing it, and a duplicate is stored once.
- **Wire format**: the CloudEvents Kafka binding in binary mode, readable by any CloudEvents-aware tool.
- **Malformed records** are copied to `<topic>.dead` by default.

Handlers must still be idempotent: delivery is at-least-once. See [@relaybus/kafka](/packages/kafka) for partitioning, ordering and the Confluent client status.
