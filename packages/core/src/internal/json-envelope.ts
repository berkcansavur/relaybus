import type { EventEnvelope, EventHeaders } from '../event.js';
import { compile } from './json-envelope-plan.js';
import { readMessage } from './json-envelope-read.js';
import { writeMessage } from './json-envelope-write.js';

/**
 * A field of the JSON body, by dotted path: `'eventName'`, `'meta.correlationId'`. A segment that
 * is a whole number indexes an array: `'messageType.0'` is the first element of `messageType`.
 */
export type BodyField = { readonly body: string };
/** A message header (Kafka record header, AMQP message header), by exact name. */
export type HeaderField = { readonly header: string };
/** Where one envelope value lives on the wire. */
export type WireField = BodyField | HeaderField;
/** The one event type every message of the route carries: written nowhere, used for every message read. */
export type FixedType = { readonly fixed: string };

type MappingFields<TField extends WireField, TTransportId extends string> = {
  /** Where the event type lives, or the one type of the route. Required. */
  readonly type: TField | FixedType;
  /**
   * Where the event id lives, or the transport's own message identity (see the transport's
   * mapping type). Required, so the inbox dedup key is always a deliberate choice. A message
   * whose mapped id is missing is malformed.
   */
  readonly id: TField | TTransportId;
  /** Ordering key. Default: the transport's own (the Kafka record key; none on RabbitMQ). */
  readonly key?: TField;
  /** Producing service. Default: the transport's own (Kafka topic; RabbitMQ `appId`, else the routing key). */
  readonly source?: TField;
  /** RFC 3339 time. Default: the transport's own (Kafka record timestamp; RabbitMQ `timestamp`, seconds). */
  readonly occurredAt?: TField;
  readonly correlationId?: TField;
  readonly causationId?: TField;
  /** Media type written with the message. Default `application/json`. Not checked on read. */
  readonly contentType?: string;
};

/**
 * A JSON object that wraps the payload with event metadata, such as
 * `{ "eventName": ..., "messageId": ..., "data": {...}, "meta": {...} }`.
 * Each value lives at a body path or in a header.
 */
export type BodyEnvelopeMapping<TTransportId extends string> = MappingFields<
  WireField,
  TTransportId
> & {
  /** Body path of the payload. Required. */
  readonly payload: BodyField;
  /**
   * Event headers: a body path holding an object of strings, or `'message-headers'` (default),
   * every message header that no other field maps.
   */
  readonly headers?: BodyField | 'message-headers';
};

/**
 * The body is the payload itself, as a plain JSON producer writes it; every other value lives in
 * a header, so body paths are a compile error here.
 */
export type PayloadOnlyMapping<TTransportId extends string> = MappingFields<
  HeaderField,
  TTransportId
> & {
  readonly payload: 'whole-body';
  /** Event headers: every message header that no other field maps. */
  readonly headers?: 'message-headers';
};

/** Where every envelope value lives in a JSON message. `type`, `id` and `payload` are required. */
export type JsonEnvelopeMapping<TTransportId extends string> =
  BodyEnvelopeMapping<TTransportId> | PayloadOnlyMapping<TTransportId>;

/** A JSON message: the parsed body and the text headers. */
export type JsonWireMessage = { readonly body: unknown; readonly headers: EventHeaders };

/** The values read from a message. An absent optional value is left to the transport's default. */
export type JsonEnvelopeFields = {
  readonly id?: string;
  readonly type: string;
  readonly payload: unknown;
  readonly key?: string;
  readonly source?: string;
  readonly headers: EventHeaders;
  readonly correlationId: string | null;
  readonly causationId: string | null;
  readonly occurredAt?: Date;
};

/** The errors of the calling transport. */
export type JsonEnvelopeFailures = {
  /** For a message that cannot be read: it goes to the invalid-message path. */
  readonly invalid: (reason: string) => Error;
  /** For an event that cannot be written: never retried. */
  readonly unencodable: (reason: string) => Error;
};

/** Writes and reads one mapping. Created once per codec; both methods are pure. */
export interface JsonEnvelopeFormat {
  /** The mapped headers and the body; the id is left out when the transport supplies it. */
  write(envelope: EventEnvelope): JsonWireMessage;
  read(message: JsonWireMessage): JsonEnvelopeFields;
}

/**
 * Compiles `mapping` once. `transportIds` lists the transport's own id strategies.
 *
 * @throws ConfigurationError for a mapping that is incomplete, names a header twice, nests one
 * body path inside another or mixes array indexes and names under one body field.
 */
export function createJsonEnvelopeFormat(
  mapping: JsonEnvelopeMapping<string>,
  {
    transportIds,
    failures,
  }: { readonly transportIds: readonly string[]; readonly failures: JsonEnvelopeFailures },
): JsonEnvelopeFormat {
  const plan = compile(mapping, transportIds);
  return {
    write: (envelope) => writeMessage(envelope, { plan, failures }),
    read: (message) => readMessage(message, { plan, failures }),
  };
}
