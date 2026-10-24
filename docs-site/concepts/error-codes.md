---
title: Error codes - stable identifiers of relaybus errors
description: Every relaybus error carries a stable, machine-readable code such as RELAYBUS_LEASE_LOST. The code is stored with dead letters and reported to observers, so alerts can branch on it.
---

# Error codes

Every error class of relaybus has a readonly `code` on its instances, next to its `name`. The code is stable: once released it is never renamed or reused, so alerts, dashboards and retry rules can branch on it. Messages may change between versions; codes do not.

```ts
import { RelayBusError } from '@relaybus/core';

try {
  await bus.publish(new OrderPlaced({ orderId }));
} catch (error) {
  if (error instanceof RelayBusError && error.code === 'RELAYBUS_TRANSACTION_REQUIRED') {
    // publish inside a transaction
  }
}
```

## Where codes appear

- **Dead letters and retries.** The `lastError` of a delivery (`bus.listDeliveries()`) has a `code` when the failure was a relaybus error, for example a transport that could not publish.
- **Observer events.** The `error` of `onDeliveryRetryScheduled`, `onDeliveryDead` and `onFailure` carries the same `code`.
- **`toErrorInfo(error)`** adds `code` for relaybus errors only. Errors of your handlers, drivers and brokers keep `name`, `message` and `stack` without a `code`, exactly as before.

The field is additive: records written before 0.3.0 have no `code`, so read it as optional.

## Code scheme

`RELAYBUS_` + the package for transport errors (`KAFKA_`, `RABBITMQ_`) + the error in upper snake case. An error class you derive from `RelayBusError` yourself inherits `RELAYBUS_ERROR` unless it sets its own code.

## Codes

| Code                                       | Error class                    | Package              | Meaning                                                                 |
| ------------------------------------------ | ------------------------------ | -------------------- | ----------------------------------------------------------------------- |
| `RELAYBUS_ERROR`                           | `RelayBusError`                | `@relaybus/core`     | Base class; any relaybus error without a more specific code.            |
| `RELAYBUS_CONFIGURATION`                   | `ConfigurationError`           | `@relaybus/core`     | Invalid options passed to a relaybus component.                         |
| `RELAYBUS_TRANSACTION_REQUIRED`            | `TransactionRequiredError`     | `@relaybus/core`     | `publish` ran outside a transaction with `transactionMode: 'required'`. |
| `RELAYBUS_NON_RETRYABLE`                   | `NonRetryableError`            | `@relaybus/core`     | A handler asked to dead-letter the delivery at once.                    |
| `RELAYBUS_EVENT_VALIDATION`                | `EventValidationError`         | `@relaybus/core`     | A payload failed its event schema. Never retried.                       |
| `RELAYBUS_INVALID_ENVELOPE`                | `InvalidEnvelopeError`         | `@relaybus/core`     | A message received from a broker is not a valid event. Never retried.   |
| `RELAYBUS_LEASE_LOST`                      | `LeaseLostError`               | `@relaybus/core`     | The worker no longer owns the delivery it was processing.               |
| `RELAYBUS_HANDLER_TIMEOUT`                 | `HandlerTimeoutError`          | `@relaybus/core`     | A handler did not settle within its timeout. Retried.                   |
| `RELAYBUS_DISPATCHER_STOPPED`              | `DispatcherStoppedError`       | `@relaybus/core`     | The bus stopped before the handler finished; the delivery is released.  |
| `RELAYBUS_KAFKA_INVALID_MESSAGE`           | `InvalidKafkaMessageError`     | `@relaybus/kafka`    | A Kafka record is not a valid event. Never retried.                     |
| `RELAYBUS_KAFKA_UNENCODABLE_EVENT`         | `UnencodableEventError`        | `@relaybus/kafka`    | An event cannot be written to Kafka. Never retried.                     |
| `RELAYBUS_KAFKA_PUBLISH_TIMEOUT`           | `PublishTimeoutError`          | `@relaybus/kafka`    | No broker ack within `publishTimeout`. The outbox retries.              |
| `RELAYBUS_KAFKA_HANDLER_FAILED`            | `HandlerFailedError`           | `@relaybus/kafka`    | A handler failure handed back to the Kafka client.                      |
| `RELAYBUS_RABBITMQ_UNAVAILABLE`            | `RabbitMqUnavailableError`     | `@relaybus/rabbitmq` | The broker cannot take or confirm a message right now. Retried.         |
| `RELAYBUS_RABBITMQ_PUBLISH_REJECTED`       | `RabbitMqPublishRejectedError` | `@relaybus/rabbitmq` | The broker nacked a published message. Retried.                         |
| `RELAYBUS_RABBITMQ_UNROUTABLE_MESSAGE`     | `UnroutableMessageError`       | `@relaybus/rabbitmq` | No queue is bound to the routing key. Retried.                          |
| `RELAYBUS_RABBITMQ_MALFORMED_MESSAGE`      | `MalformedMessageError`        | `@relaybus/rabbitmq` | A received message is not a valid relaybus CloudEvent. Dead-lettered.   |
| `RELAYBUS_RABBITMQ_UNSERIALIZABLE_PAYLOAD` | `UnserializablePayloadError`   | `@relaybus/rabbitmq` | A payload cannot be represented as JSON. Never retried.                 |
| `RELAYBUS_RABBITMQ_AMQP_LIMIT_EXCEEDED`    | `AmqpLimitExceededError`       | `@relaybus/rabbitmq` | A value exceeds an AMQP 0-9-1 protocol limit. Never retried.            |

A test keeps this table in step with the code: adding, renaming or removing a code fails it until the change is deliberate.
