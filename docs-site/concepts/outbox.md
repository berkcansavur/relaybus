---
title: Transactional outbox pattern in Node.js with relaybus
description: How the transactional outbox pattern avoids the dual-write problem and how relaybus implements it, with one delivery row per handler written in your database transaction.
---

# Transactional outbox

## The problem: dual writes

Publishing an event after a database write is a dual write. A crash, a broker outage or a rollback between the two writes either loses the event or announces a change that never happened. In-process emitters lose events on restart, and queue libraries on Redis cannot join your SQL transaction.

## The pattern

The transactional outbox writes the event to a table in the same database transaction as the business data. A separate relay reads the table after commit and delivers the events. The database transaction is the single source of truth: commit keeps both, rollback discards both.

## How relaybus implements it

1. `publish` writes the event and **one delivery per subscribed handler or route** into the active transaction. Events with a key get a per-key sequence that follows commit order.
2. The dispatcher in every process claims due deliveries with a [lease](/concepts/leases), runs the handler, renews the lease while it runs and marks the delivery completed.
3. A failure reschedules the delivery with [backoff](/concepts/retries), or [dead-letters](/concepts/dead-letters) it after the last attempt. A worker that crashes leaves an expired lease, and another worker takes the delivery over.
4. Routes forward events to Kafka or RabbitMQ after commit, so a broker outage only delays them. Subscriptions store received events by id before acknowledging them, which makes consumption idempotent (the inbox pattern).

## Which transaction does `publish` join?

`publish` finds the transaction in this order: the `transaction` option, the transaction opened by `bus.transaction()`, then your `transactionResolver` (for example from `@nestjs-cls/transactional`). With none, it throws `TransactionRequiredError` unless you opt into `transactionMode: 'optional'`, where the event stays durable but is no longer atomic with your writes.

```ts
await bus.transaction(async (tx) => {
  await tx.query('INSERT INTO orders ...');
  await bus.publish(new OrderPlaced({ orderId }), { key: orderId });
});
```

## Related

- [Deliveries](/concepts/deliveries)
- [Guarantees](/guarantees)
- [How to implement the transactional outbox pattern in NestJS?](/faq#how-to-implement-the-transactional-outbox-pattern-in-nestjs)
