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

/**
 * How `occurredAt` travels: RFC 3339 text (default), or whole epoch milliseconds or seconds, a
 * number in the body and decimal text in a header. Epoch seconds drop the milliseconds.
 */
export type TimeFormat = 'rfc3339' | 'epoch-millis' | 'epoch-seconds';
/** Where the event time lives, and in which format. */
export type TimeField<TField extends WireField> = TField & { readonly format?: TimeFormat };

/** A JSON value written as it is. */
export type JsonConstant =
  | string
  | number
  | boolean
  | null
  | readonly JsonConstant[]
  | { readonly [name: string]: JsonConstant };
/** A value written with every message: a constant, or derived from the event. */
export type ExtraValue<TValue> = TValue | ((envelope: EventEnvelope) => TValue);
/** Extra message headers, by name. */
export type ExtraHeaders = Readonly<Record<string, ExtraValue<string>>>;
/** Extra body values, by dotted path. */
export type ExtraBody = Readonly<Record<string, ExtraValue<JsonConstant>>>;

type MappingFields<TField extends WireField, TTransportId extends string> = {
  /** Where the event type lives, or the one type of the route. Required. */
  readonly type: TField | FixedType;
  /**
   * Where the event id lives, or the transport's own message identity (see the transport's
   * mapping type). Required, so the inbox dedup key is always a deliberate choice. A message
   * whose mapped id is missing is malformed, unless `whenMissing` names the transport's own id
   * to use instead: `{ body: 'id', whenMissing: 'record-position' }`.
   */
  readonly id: TField | TTransportId | (TField & { readonly whenMissing: TTransportId });
  /** Ordering key. Default: the transport's own (the Kafka record key; none on RabbitMQ). */
  readonly key?: TField;
  /** Producing service. Default: the transport's own (Kafka topic; RabbitMQ `appId`, else the routing key). */
  readonly source?: TField;
  /**
   * Event time, RFC 3339 unless `format` says otherwise. Default: the transport's own (Kafka
   * record timestamp; RabbitMQ `timestamp`, seconds).
   */
  readonly occurredAt?: TimeField<TField>;
  readonly correlationId?: TField;
  readonly causationId?: TField;
  /** Media type written with the message. Default `application/json`. Not checked on read. */
  readonly contentType?: string;
  /**
   * Wire names of event types, for a format whose type values are not relaybus event types or
   * name them otherwise: `{ 'transfer.auto-dispatched.v1': 'Transfer Auto Dispatched' }`. A listed
   * type is written as its wire name and read back from it; any other type travels as itself. One
   * route then carries many types without a `fixed` type per transport. Not with a `fixed` type.
   */
  readonly wireTypes?: WireTypes;
};

/** Event type to the name the wire uses for it; see `wireTypes` of a jsonEnvelope mapping. */
export type WireTypes = Readonly<Record<string, string>>;

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
   * Event headers: a body path holding an object of strings, `'message-headers'` (default),
   * every message header that no other field maps, or `'top-level-fields'`, spread over the top
   * level of the body: every top-level text field that no other value uses is read as one.
   */
  readonly headers?: BodyField | 'message-headers' | 'top-level-fields';
  /**
   * Values written with every message besides the mapped ones, such as a constant
   * `provider: 'orders-api'` or a second copy of the time. They are never read; extra headers
   * are left out of the event headers on read.
   */
  readonly extra?: { readonly body?: ExtraBody; readonly headers?: ExtraHeaders };
  /**
   * Top-level body fields to write first, in this order; the others follow in the order they
   * are written. Only the bytes change, for formats compared byte for byte.
   */
  readonly bodyOrder?: readonly string[];
  /**
   * Keeps the top-level body fields that no other value uses (and that are not read as event
   * headers) with every JSON type, in body order, as one object under `payloadField` of the
   * payload: `{ payloadField: 'wire' }` reads `{ "data": { "a": 1 }, "attempt": 2 }` as the payload
   * `{ "a": 1, "wire": { "attempt": 2 } }`, so the handler's schema validates them with the payload.
   * Writing spreads that object back over the top level. Default: such fields are left out on read.
   */
  readonly otherFields?: OtherFields;
};

/** Where {@link BodyEnvelopeMapping.otherFields} keeps the unused top-level fields. */
export type OtherFields = { readonly payloadField: string };

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
  /** Message headers written with every message; never read, and left out of the event headers. */
  readonly extra?: { readonly headers?: ExtraHeaders };
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
