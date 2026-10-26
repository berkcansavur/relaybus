---
title: Send events between services with Kafka without losing them - relaybus recipe
description: Use relaybus routes and subscriptions with the Kafka transport to publish committed outbox events to a topic and consume them idempotently in another service, avoiding lost events between database and Kafka.
---

# Kafka between services

Service A writes an event in its database transaction. relaybus forwards it to a Kafka topic after commit. Service B stores received events once by id, then runs its handlers. The database-to-Kafka gap is covered by the outbox, and the Kafka-to-handler gap by the inbox.

```sh
pnpm add @relaybus/core @relaybus/kafka @confluentinc/kafka-javascript
```

## Service A: route committed events to a topic

```ts
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { RelayBus } from '@relaybus/core';
import { KafkaTransport } from '@relaybus/kafka';

const transport = new KafkaTransport({
  kafka: new KafkaJS.Kafka({ kafkaJS: { clientId: 'orders', brokers: ['kafka:9092'] } }),
  client: 'confluent',
});

const bus = new RelayBus({
  store,
  source: 'orders',
  transports: [transport],
  routes: [{ event: OrderPlaced, transport: 'kafka', destination: 'orders.events' }],
});
```

### Several events of a catalog to one topic

When the events belong to a [catalog](/concepts/event-catalogs), one `route` sends the ones you pick with shared settings instead of one route per event:

```ts
import { produce, route } from '@relaybus/core';

const bus = new RelayBus({
  store,
  source: 'orders',
  transports: [transport],
  routes: [
    route(produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }), {
      transport: 'kafka',
      destination: 'orders.events',
      retry: { initialDelay: '500ms', maxDelay: '30s' },
      timeout: '10s',
    }),
  ],
});
```

Events of one key still reach the topic in commit order whatever their types, and `produce` checks count the events as routed. For a topic whose format names the types differently, add `wireTypes` to the route or the catalog (see [Wire names](/concepts/event-catalogs#wire-names-next-to-the-catalog-and-the-route)).

## Service B: subscribe and handle

```ts
const transport = new KafkaTransport({
  kafka: new KafkaJS.Kafka({ kafkaJS: { clientId: 'billing', brokers: ['kafka:9092'] } }),
  client: 'confluent',
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

Handlers must still be idempotent: delivery is at-least-once. See [@relaybus/kafka](/packages/kafka) for partitioning, ordering and using `kafkajs` instead of the Confluent client.
