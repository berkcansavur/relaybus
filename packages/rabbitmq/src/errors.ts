import { RelayBusError } from '@relaybus/core';

/**
 * The broker cannot take or confirm a message right now: the transport is not
 * connected, was closed, lost its channel, or the confirm did not arrive in time.
 * Always retryable; the outbox keeps the delivery and tries again.
 */
export class RabbitMqUnavailableError extends RelayBusError {}

/** The broker negatively acknowledged a published message. Retryable. */
export class RabbitMqPublishRejectedError extends RelayBusError {}

/**
 * The broker accepted the message but no queue is bound to its routing key, so
 * it would have been discarded. Retryable: it succeeds once a consumer binds.
 */
export class UnroutableMessageError extends RelayBusError {}

/** A received message is not a valid relaybus CloudEvent. It is dead-lettered. */
export class MalformedMessageError extends RelayBusError {}
