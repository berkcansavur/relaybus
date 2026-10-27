import { ConfigurationError, type EventEnvelope } from '@relaybus/core';
import {
  createJsonEnvelopeFormat,
  mergeWireTypes,
  type JsonEnvelopeFormat,
  type JsonEnvelopeMapping,
} from '@relaybus/core/internal';
import {
  CONTENT_TYPE,
  decodeEnvelope,
  encodeEnvelope,
  encodeStructuredEnvelope,
  formatTime,
  invalid,
  parsePayload,
  serializeJson,
} from './cloudevents-codec.js';
import { UnencodableEventError } from './errors.js';
import {
  decodeCloudEvent,
  readHeadersLeniently,
  type DecodedEvent,
  type KafkaInboundRecord,
  type KafkaMessageDecoder,
} from './inbound-decoders.js';
import type { KafkaOutgoingMessage } from './kafka-client.js';

/** The record a codec produces for one event. `timestamp` is epoch milliseconds in a string. */
export type KafkaEncodedRecord = KafkaOutgoingMessage & { readonly timestamp?: string };

/**
 * The wire format of a {@link KafkaTransport}: how an event becomes a record
 * (`encode`, on publish) and a consumed record becomes an event (`decode`).
 *
 * Both must be synchronous and free of side effects. What `encode` throws
 * fails the publish: the outbox retries it, unless it is a `NonRetryableError`
 * such as {@link UnencodableEventError}. What `decode` throws marks the record
 * as malformed: it goes to `onInvalidMessage` (dead-letter by default) and the
 * consumer moves on. See {@link KafkaMessageDecoder} for the decode contract.
 */
export interface KafkaCodec {
  readonly encode: (envelope: EventEnvelope) => KafkaEncodedRecord;
  readonly decode: KafkaMessageDecoder;
  /**
   * Returns this codec with more wire names for event types, which a route or catalog declares
   * (`wireTypes` of `route(...)` or `defineCatalog`). Only a codec that maps type names has it
   * ({@link jsonEnvelope}); declaring wire names for a transport whose codec lacks it fails at boot.
   */
  readonly withWireTypes?: (wireTypes: Readonly<Record<string, string>>) => KafkaCodec;
}

/**
 * CloudEvents 1.0 binary content mode, the default: the value is the JSON
 * payload, the attributes travel as `ce_*` headers ({@link encodeEnvelope}).
 * Decoding is strict: any other record is malformed ({@link decodeEnvelope}).
 */
export const cloudEventsBinary: KafkaCodec = Object.freeze({
  encode: encodeEnvelope,
  decode: decodeEnvelope,
});

/**
 * CloudEvents 1.0 structured content mode: the value is one
 * `application/cloudevents+json` document with the attributes and `data`.
 * Decoding accepts structured and binary mode ({@link decodeCloudEvent}), so a
 * topic can move from one to the other while both are in flight.
 */
export const cloudEventsStructured: KafkaCodec = Object.freeze({
  encode: encodeStructuredEnvelope,
  decode: decodeCloudEvent,
});

/**
 * Where each value of a JSON message lives, for {@link jsonEnvelope}. Besides
 * `{ body }` and `{ header }`, the id may be `'record-position'`: a consumed
 * record gets `kafka:<topic>:<partition>:<offset>`, stable across redeliveries
 * but different for two sends of one event. Records relaybus writes carry the
 * event id in a `relaybus-event-id` header, which wins on read, so an outbox
 * retry that sends an event twice is still deduplicated.
 */
export type KafkaJsonEnvelopeMapping = JsonEnvelopeMapping<'record-position'> & KafkaRecordOptions;

/**
 * The record key of an event: `'event-key'` (the event key, default), `'none'` (no key) or a
 * function of the event. The event key itself, and with it the outbox's per-key order, is
 * unchanged: only the wire key differs. Records without a key are spread over partitions by the
 * producer, so the per-key order holds up to the broker but not across partitions.
 */
export type KafkaRecordKey = 'event-key' | 'none' | ((envelope: EventEnvelope) => string | null);

/** Record-level choices of {@link jsonEnvelope} besides the mapping. */
export interface KafkaRecordOptions {
  /** Default `'event-key'`. */
  readonly recordKey?: KafkaRecordKey;
  /** Whether the record gets a `content-type` header. Default true. */
  readonly contentTypeHeader?: boolean;
}

const RECORD_POSITION = 'record-position';
/** Carries the event id of a record relaybus wrote under `'record-position'`. */
const EVENT_ID_HEADER = 'relaybus-event-id';
const DEFAULT_CONTENT_TYPE = 'application/json';

/**
 * A JSON wire format declared by a mapping, in both directions: for topics
 * shared with services that wrap events in their own envelope
 * (`{ "eventName": ..., "data": {...} }`, MassTransit) or send the bare
 * payload. The record key is the event key unless `recordKey` says otherwise;
 * when `occurredAt` is not mapped, the record timestamp carries it. Event headers that are message
 * headers are written as they are, so a name the mapping uses for another
 * field is refused with {@link UnencodableEventError}, as are `content-type`
 * and `relaybus-event-id`.
 *
 * ```ts
 * codec: jsonEnvelope({
 *   type: { body: 'eventName' },
 *   id: { body: 'messageId' },
 *   payload: { body: 'data' },
 *   correlationId: { body: 'meta.correlationId' },
 * })
 * ```
 *
 * @throws ConfigurationError for an incomplete or self-overlapping mapping.
 */
export function jsonEnvelope(mapping: KafkaJsonEnvelopeMapping): KafkaCodec {
  const format = createJsonEnvelopeFormat(mapping, {
    transportIds: [RECORD_POSITION],
    failures: { invalid, unencodable: (reason) => new UnencodableEventError(reason) },
  });
  const contentType: Record<string, string> =
    mapping.contentTypeHeader === false
      ? {}
      : { [CONTENT_TYPE]: mapping.contentType ?? DEFAULT_CONTENT_TYPE };
  const stampsTime = mapping.occurredAt === undefined;
  const ownHeaders = (envelope: EventEnvelope): Record<string, string> =>
    mapping.id === RECORD_POSITION
      ? { ...contentType, [EVENT_ID_HEADER]: envelope.id }
      : contentType;
  const recordKey = resolveRecordKey(mapping.recordKey);
  return {
    withWireTypes: (declared) =>
      jsonEnvelope({ ...mapping, wireTypes: mergeWireTypes(mapping, declared) }),
    encode: (envelope) => encodeJson(envelope, { format, stampsTime, ownHeaders, recordKey }),
    decode: (record) => decodeJson(record, format),
  };
}

function resolveRecordKey(
  recordKey: KafkaRecordKey | undefined,
): (envelope: EventEnvelope) => string | null {
  if (recordKey === undefined || recordKey === 'event-key') {
    return (envelope) => envelope.key;
  }
  if (recordKey === 'none') {
    return () => null;
  }
  if (typeof recordKey === 'function') {
    return recordKey;
  }
  throw new ConfigurationError(
    `jsonEnvelope recordKey must be 'event-key', 'none' or a function, received '${String(recordKey)}'`,
  );
}

/** How {@link jsonEnvelope} writes: by the format, with its own headers, and the time as timestamp when unmapped. */
type JsonEncoding = {
  readonly format: JsonEnvelopeFormat;
  readonly stampsTime: boolean;
  readonly ownHeaders: (envelope: EventEnvelope) => Record<string, string>;
  readonly recordKey: (envelope: EventEnvelope) => string | null;
};

function encodeJson(
  envelope: EventEnvelope,
  { format, stampsTime, ownHeaders, recordKey }: JsonEncoding,
): KafkaEncodedRecord {
  assertRepresentable(envelope);
  const { body, headers } = format.write(envelope);
  const taken = [CONTENT_TYPE, EVENT_ID_HEADER].find((name) => Object.hasOwn(headers, name));
  if (taken !== undefined) {
    throw new UnencodableEventError(
      `Event ${envelope.id} has a header '${taken}', which the codec writes itself`,
    );
  }
  return {
    key: recordKey(envelope),
    value: serializeJson(envelope, body),
    headers: { ...headers, ...ownHeaders(envelope) },
    ...(stampsTime ? { timestamp: recordTimestamp(envelope) } : {}),
  };
}

function decodeJson(record: KafkaInboundRecord, format: JsonEnvelopeFormat): DecodedEvent {
  const { [EVENT_ID_HEADER]: eventId, ...headers } = withoutContentType(
    readHeadersLeniently(record.headers),
  );
  const fields = format.read({ body: parsePayload(record.value), headers });
  return {
    ...fields,
    id:
      fields.id ??
      (eventId === undefined || eventId === ''
        ? `kafka:${record.topic}:${record.partition}:${record.offset}`
        : eventId),
  };
}

function withoutContentType(headers: Readonly<Record<string, string>>): Record<string, string> {
  return Object.fromEntries(Object.entries(headers).filter(([name]) => name !== CONTENT_TYPE));
}

/** A payload JSON would drop from the body instead of refusing; nested ones fail when serialized. */
function assertRepresentable(envelope: EventEnvelope): void {
  const kind = typeof envelope.payload;
  if (kind === 'undefined' || kind === 'function' || kind === 'symbol') {
    throw new UnencodableEventError(
      `Event ${envelope.id} has a payload that JSON cannot represent (undefined, a function or a symbol)`,
    );
  }
}

/** The occurredAt as the record timestamp, epoch milliseconds; refuses an invalid date. */
function recordTimestamp(envelope: EventEnvelope): string {
  formatTime(envelope);
  return String(envelope.occurredAt.getTime());
}
