---
title: Deliveries - per-handler event delivery state in relaybus
description: A relaybus delivery is one event for one handler or route, with its own status, attempts, lease and retry schedule, so one failing handler never re-runs the others.
---

# Deliveries

A **delivery** is one event for one handler (or one transport route). When you publish an event, relaybus inserts the event row and one delivery row for every handler or route subscribed to that event type. The `deliveries` table has a unique `(event_id, handler)` constraint.

## Status

A delivery is `pending` (waiting for its due time), `processing` (leased by a worker), `completed` or `dead`. Each delivery tracks its attempts, the time it becomes available again, the lease owner, token and expiry, and the last error.

## Why per handler

Each handler has its own delivery record, so one failing handler never re-runs the others. A slow or broken email handler cannot cause the stock handler to run twice, and each handler can have its own retry policy, timeout and concurrency:

```ts
@Subscribe(OrderPlaced, {
  retry: { attempts: 5, initialDelay: '2s', maxDelay: '5m' },
  timeout: '30s',
  concurrency: 4,
})
async reserveStock(event: EventEnvelope<{ orderId: string }>) {}
```

Register every handler in each process that publishes the event: deliveries are created at publish time.

## Routes are deliveries too

A route forwards an event to a transport (Kafka or RabbitMQ). It is retried until the broker accepts it (unlimited attempts by default, 30 second publish timeout), exactly like a handler.

## Inspecting deliveries

```ts
const dead = await bus.listDeliveries({ status: 'dead', handler: 'Shipping.ship' });
const stats = await bus.statistics(); // pending, processing, completed, dead, oldestDueAt
```

Completed deliveries are kept for 7 days by default (`retention.completed`), which is also the deduplication horizon for ids.

## Related

- [Leases and fencing](/concepts/leases)
- [Retries and backoff](/concepts/retries)
- [Dead letters](/concepts/dead-letters)
