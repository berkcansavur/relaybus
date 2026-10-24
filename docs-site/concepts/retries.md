---
title: Retries, backoff and jitter for event handlers in relaybus
description: relaybus retries each handler with capped exponential backoff and full jitter, supports timeouts, and dead-letters non-retryable errors immediately.
---

# Retries and backoff

Every handler delivery is retried on failure with **capped exponential backoff and full jitter**. The defaults are 10 attempts, a 1 second initial delay, a factor of 2 and a 10 minute cap.

```ts
@RelayBusHandler(OrderPlaced, {
  retry: { attempts: 5, initialDelay: '2s', maxDelay: '5m' },
  timeout: '30s',
})
async ship(event: EventEnvelope<{ orderId: string }>) {}
```

`attempts: Infinity` retries forever. Durations are milliseconds or strings such as `'250ms'`, `'30s'`, `'5m'`, `'1h'`.

## Non-retryable errors

Throw `NonRetryableError` to dead-letter a delivery at once. Schema validation failures are never retried.

## Timeouts and cancellation

A handler `timeout` aborts `context.signal`. Use the signal to stop work when a timeout, lease loss or shutdown happens.

## Routes

Routes to Kafka or RabbitMQ retry without a limit by default (1 second initial delay, factor 2, capped at 5 minutes) so a broker outage only delays events.

## Retries and ordering

For keyed events, a retrying head blocks its successors for the same handler and key; a dead-lettered head releases them. See [Ordering keys](/concepts/ordering-keys).

## Related

- [Dead letters](/concepts/dead-letters)
- [Testing with FakeClock](/recipes/testing-with-fakeclock)
