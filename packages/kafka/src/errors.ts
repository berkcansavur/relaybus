import { NonRetryableError, RelayBusError } from '@relaybus/core';

/** A Kafka record that cannot be turned into an {@link EventEnvelope}. Never retried. */
export class InvalidKafkaMessageError extends RelayBusError {
  override name = 'InvalidKafkaMessageError';
  override readonly code: string = 'RELAYBUS_KAFKA_INVALID_MESSAGE';
}

/**
 * An event that cannot be written to Kafka: its payload is not representable as
 * JSON (`undefined`, a function, a symbol, a `BigInt`, a circular structure) or
 * its `occurredAt` is not an RFC 3339 instant (invalid date, year outside 0..9999).
 * Retrying cannot help, so it is a {@link NonRetryableError}.
 */
export class UnencodableEventError extends NonRetryableError {
  override name = 'UnencodableEventError';
  override readonly code: string = 'RELAYBUS_KAFKA_UNENCODABLE_EVENT';
}

/** `publish` did not get a broker ack within `publishTimeout`. The outbox retries it. */
export class PublishTimeoutError extends RelayBusError {
  override name = 'PublishTimeoutError';
  override readonly code: string = 'RELAYBUS_KAFKA_PUBLISH_TIMEOUT';
}

/** The transport was closed: it no longer publishes or subscribes. */
export class TransportClosedError extends RelayBusError {
  override name = 'TransportClosedError';
  override readonly code: string = 'RELAYBUS_KAFKA_TRANSPORT_CLOSED';
}

/** The consumer did not join its group within `startupTimeout`. */
export class GroupJoinTimeoutError extends RelayBusError {
  override name = 'GroupJoinTimeoutError';
  override readonly code: string = 'RELAYBUS_KAFKA_GROUP_JOIN_TIMEOUT';
}

/** A handler failure re-thrown to the Kafka client, stripped of anything that could make it fatal. */
export class HandlerFailedError extends RelayBusError {
  override name = 'HandlerFailedError';
  override readonly code: string = 'RELAYBUS_KAFKA_HANDLER_FAILED';
}
