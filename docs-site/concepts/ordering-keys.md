---
title: Ordering keys - per-key event ordering in relaybus
description: Events with the same key are handled one at a time per handler in commit order; relaybus assigns a per-key sequence inside your transaction. There is no global ordering.
---

# Ordering keys

Pass a `key` when publishing to get ordering for related events, for example one order or one account:

```ts
await bus.publish(new OrderPlaced({ orderId }), { key: orderId });
```

## What is guaranteed

Events with the same `key` are handled **one at a time per handler, in commit order**. There is no global ordering across keys.

## How it works

- On append, the key's counter row is incremented inside your transaction. The row lock is held until the transaction ends, so two transactions publishing the same key are serialized and the sequence equals the commit order.
- A keyed delivery is claimed only when no earlier delivery of the same handler and key is pending or processing.
- A retrying or leased head blocks its successors. A dead-lettered head releases them.

## Costs and tips

- Keep transactions that publish keyed events short: the counter row stays locked until commit.
- Publish all keys of a transaction in one `publishAll` call. The store locks keys in sorted order, which avoids deadlocks between transactions.
- Pick keys with bounded cardinality: one counter row is kept per key.
- Across Kafka, the key is the partition key. With RabbitMQ, order is not preserved unless you set `ordered: true`; see [@relaybus/rabbitmq](/packages/rabbitmq).

## Related

- [Retries and backoff](/concepts/retries)
- [Guarantees](/guarantees)
