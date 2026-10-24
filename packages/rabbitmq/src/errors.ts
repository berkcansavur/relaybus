import { NonRetryableError, RelayBusError } from '@relaybus/core';

/**
 * The broker cannot take or confirm a message right now: the transport is not
 * connected, was closed, lost its channel, or the confirm did not arrive in time.
 * Always retryable; the outbox keeps the delivery and tries again.
 */
export class RabbitMqUnavailableError extends RelayBusError {
  override name = 'RabbitMqUnavailableError';
  override readonly code: string = 'RELAYBUS_RABBITMQ_UNAVAILABLE';
}

/** The broker negatively acknowledged a published message. Retryable. */
export class RabbitMqPublishRejectedError extends RelayBusError {
  override name = 'RabbitMqPublishRejectedError';
  override readonly code: string = 'RELAYBUS_RABBITMQ_PUBLISH_REJECTED';
}

/**
 * The broker accepted the message but no queue is bound to its routing key, so
 * it would have been discarded. Retryable: it succeeds once a consumer binds.
 */
export class UnroutableMessageError extends RelayBusError {
  override name = 'UnroutableMessageError';
  override readonly code: string = 'RELAYBUS_RABBITMQ_UNROUTABLE_MESSAGE';
}

/** A received message is not a valid relaybus CloudEvent. It is dead-lettered. */
export class MalformedMessageError extends RelayBusError {
  override name = 'MalformedMessageError';
  override readonly code: string = 'RELAYBUS_RABBITMQ_MALFORMED_MESSAGE';
}

/**
 * The payload of an event cannot be represented as JSON without losing or
 * changing data (`undefined`, functions, symbols, BigInt, NaN, Infinity, Dates,
 * Maps, class instances, cycles). Never retried: the payload will not change.
 */
export class UnserializablePayloadError extends NonRetryableError {
  override name = 'UnserializablePayloadError';
  override readonly code: string = 'RELAYBUS_RABBITMQ_UNSERIALIZABLE_PAYLOAD';
}

/**
 * A value exceeds a protocol limit of AMQP 0-9-1, for example an event id or a
 * routing key longer than the 255 bytes of a short string. Never retried.
 */
export class AmqpLimitExceededError extends NonRetryableError {
  override name = 'AmqpLimitExceededError';
  override readonly code: string = 'RELAYBUS_RABBITMQ_AMQP_LIMIT_EXCEEDED';
}
