import { assertEventType, type EventEnvelope, type EventHeaders } from '@relaybus/core';
import {
  DecodedEventReader,
  createCloudEventReaders,
  isJsonObject,
  mediaTypeOf,
  observeRejection,
} from '@relaybus/core/internal';
import {
  CONTENT_TYPE,
  collectUserHeaders,
  decodeEnvelope,
  invalid,
  parsePayload,
  parseTime,
  readHeaders,
  readRecordKey,
  readRecordTimestamp,
  type DecodableMessage,
} from './cloudevents-codec.js';
import type { KafkaBytes, KafkaIncomingHeaders } from './kafka-client.js';

const { optionalAttribute, requiredAttribute, assertValidType, assertJsonDataContentType } =
  createCloudEventReaders(invalid);

/** A consumed record as handed to a {@link KafkaMessageDecoder}. */
export interface KafkaInboundRecord extends DecodableMessage {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
}

/**
 * The event a decoder extracted from a record. `id`, `type` and `payload` are
 * required; every other field may be left out and is then filled by the
 * transport: `source` with the topic, `key` with the record key (UTF-8),
 * `headers` with none, `correlationId` and `causationId` with null, and
 * `occurredAt` with the record timestamp.
 */
export interface DecodedEvent {
  readonly id: string;
  readonly type: string;
  readonly payload: unknown;
  readonly source?: string;
  readonly key?: string | null;
  readonly headers?: EventHeaders;
  readonly correlationId?: string | null;
  readonly causationId?: string | null;
  readonly occurredAt?: Date;
}

/**
 * Turns a consumed record into an event, for topics written by producers that
 * do not use relaybus. Must be synchronous and free of side effects: it runs
 * again on every redelivery, so the same record must always give the same
 * event, above all the same `id`, which is what the inbox deduplicates on.
 *
 * Anything it throws marks the record as malformed: it goes to the transport's
 * `onInvalidMessage` policy (dead-letter by default) and the consumer moves on.
 * An event the bus then rejects (empty id, invalid type or time) is
 * dead-lettered the same way.
 */
export type KafkaMessageDecoder = (record: KafkaInboundRecord) => DecodedEvent;

/** A plain JSON record, decoded for the extractor options of {@link plainJsonDecoder}. */
export interface PlainJsonRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  /**
   * The record key as UTF-8 text, base64 when its bytes are not valid UTF-8
   * (for example a key written by Java's `LongSerializer`), null when absent.
   */
  readonly key: string | null;
  /**
   * Every record header as text: the last value of a repeated header; a value
   * that is not valid UTF-8 is left out.
   */
  readonly headers: EventHeaders;
  /** The record value parsed as JSON. */
  readonly payload: unknown;
  /** Record timestamp in epoch milliseconds. */
  readonly timestamp: string | undefined;
}

/** Options of {@link plainJsonDecoder}. Extractors returning undefined or `''` select the default. */
export interface PlainJsonDecoderOptions {
  /**
   * Event type handlers subscribe to: a fixed name for a topic that carries one
   * kind of event, or an extractor such as `fromHeader('event-type')` or
   * `fromPayloadField('type')`. Required; a record without a type is invalid.
   */
  readonly type: string | ((record: PlainJsonRecord) => string | undefined);
  /**
   * Event id the inbox deduplicates on, for example `fromHeader('message-id')`.
   * Default, and fallback when the extractor finds none:
   * `kafka:<topic>:<partition>:<offset>`, which is stable across redeliveries
   * of the record. Prefer an id set by the producer when it may send the same
   * event twice: two sends are two offsets, so only a producer id dedupes them.
   */
  readonly id?: (record: PlainJsonRecord) => string | undefined;
  /** Ordering key. Default: the record key. When given, its result is final (undefined means no key). */
  readonly key?: (record: PlainJsonRecord) => string | null | undefined;
  /** Logical name of the producer. Default: the topic. */
  readonly source?: string | ((record: PlainJsonRecord) => string | undefined);
  /** When the event happened. Default: the record timestamp. */
  readonly occurredAt?: (record: PlainJsonRecord) => Date | undefined;
}

const STRUCTURED_MEDIA_TYPE = 'application/cloudevents+json';
/** Refuses invalid UTF-8 and keeps a leading BOM, so different header bytes never decode equal. */
const exactUtf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const SPEC_VERSION = '1.0';
const SPEC_VERSION_HEADER = 'ce_specversion';
/** Attributes of a structured event that map to envelope fields rather than headers. */
const STRUCTURED_ATTRIBUTES: ReadonlySet<string> = new Set([
  'specversion',
  'id',
  'type',
  'source',
  'time',
  'subject',
  'datacontenttype',
  'dataschema',
  'data',
  'data_base64',
  'correlationid',
  'causationid',
]);

/**
 * Decodes a CloudEvents 1.0 record in either content mode of the Kafka
 * protocol binding:
 * - structured (`content-type: application/cloudevents+json`): the value is
 *   a JSON object with `specversion`, `id`, `type`, `source` and optionally
 *   `time`, `subject` (the key, else the record key), `data` (the payload,
 *   null when absent) and a JSON `datacontenttype`. `data_base64` is refused,
 *   because relaybus payloads are JSON. Extension attributes with string
 *   values (`traceparent`, ...) become event headers, as do the record
 *   headers; `correlationid` and `causationid` fill their envelope fields.
 * - binary (anything else): exactly {@link decodeEnvelope}, the default decoder.
 *
 * @throws InvalidKafkaMessageError for a record that is neither.
 */
export function decodeCloudEvent(record: DecodableMessage): EventEnvelope {
  const headers = readHeaders(record.headers);
  if (mediaTypeOf(headers.get(CONTENT_TYPE)) !== STRUCTURED_MEDIA_TYPE) {
    return decodeEnvelope(record);
  }
  const body = parsePayload(record.value);
  if (!isJsonObject(body)) {
    throw invalid('A structured CloudEvent must be a JSON object');
  }
  if (body['specversion'] !== SPEC_VERSION) {
    throw invalid(`specversion must be '${SPEC_VERSION}'`);
  }
  const type = requiredAttribute(body, 'type');
  assertValidType(type, 'type');
  assertJsonDataContentType(optionalAttribute(body, 'datacontenttype'));
  if (Object.hasOwn(body, 'data_base64')) {
    throw invalid('data_base64 is not supported: relaybus payloads are JSON, send them as data');
  }
  const time = optionalAttribute(body, 'time');
  return {
    id: requiredAttribute(body, 'id'),
    type,
    source: requiredAttribute(body, 'source'),
    key: optionalAttribute(body, 'subject') ?? readRecordKey(record.key),
    payload: Object.hasOwn(body, 'data') ? body['data'] : null,
    headers: { ...collectUserHeaders(headers), ...extensionAttributes(body) },
    correlationId: optionalAttribute(body, 'correlationid') ?? null,
    causationId: optionalAttribute(body, 'causationid') ?? null,
    occurredAt: time === undefined ? requireRecordTimestamp(record) : parseTime(time, 'time'),
  };
}

/**
 * Creates a decoder for records whose value is plain JSON, as written by a
 * service that does not use relaybus (a plain kafkajs producer, Spring Kafka's
 * `JsonSerializer`, a Go service): the value becomes the payload, the record
 * headers become event headers, and the options say where the event type and
 * id come from.
 *
 * A record that is a CloudEvent anyway (a `ce_specversion` header, or a
 * structured `application/cloudevents+json` value) is decoded with
 * {@link decodeCloudEvent} instead, so relaybus and foreign producers can share
 * a topic. A missing or non-JSON value, a missing or invalid type, and an
 * extractor returning anything but a string are invalid.
 *
 * ```ts
 * new KafkaTransport({
 *   kafka,
 *   groupId: 'billing',
 *   decode: plainJsonDecoder({ type: fromHeader('event-type'), id: fromHeader('message-id') }),
 * });
 * ```
 *
 * @throws ConfigurationError when a fixed `type` is not a valid event type.
 */
export function plainJsonDecoder(options: PlainJsonDecoderOptions): KafkaMessageDecoder {
  if (typeof options.type === 'string') {
    assertEventType(options.type);
  }
  return (record) => {
    const headers = readHeadersLeniently(record.headers);
    if (
      record.headers?.[SPEC_VERSION_HEADER] !== undefined ||
      mediaTypeOf(headers[CONTENT_TYPE]) === STRUCTURED_MEDIA_TYPE
    ) {
      return decodeCloudEvent(record);
    }
    const plain: PlainJsonRecord = {
      topic: record.topic,
      partition: record.partition,
      offset: record.offset,
      key: readKeyLeniently(record.key),
      headers,
      payload: parsePayload(record.value),
      timestamp: record.timestamp,
    };
    const type = KAFKA_EVENTS.text(options.type, { plain, name: 'type' });
    if (type === undefined) {
      throw invalid('Record has no event type: the type option found none');
    }
    assertValidType(type, 'The event type');
    return {
      id:
        KAFKA_EVENTS.text(options.id, { plain, name: 'id' }) ??
        `kafka:${record.topic}:${record.partition}:${record.offset}`,
      type,
      source: KAFKA_EVENTS.text(options.source, { plain, name: 'source' }) ?? record.topic,
      key:
        options.key === undefined
          ? plain.key
          : (KAFKA_EVENTS.text(options.key, { plain, name: 'key' }) ?? null),
      payload: plain.payload,
      headers,
      correlationId: null,
      causationId: null,
      occurredAt: resolveOccurredAt(options.occurredAt, { plain, record }),
    };
  };
}

/** The inbound plumbing of Kafka records: malformed records and the record's own defaults. */
class KafkaDecodedEvents extends DecodedEventReader<KafkaInboundRecord, PlainJsonRecord> {
  invalid(reason: string): Error {
    return invalid(reason);
  }

  /** The topic. */
  protected defaultSource(record: KafkaInboundRecord): string {
    return record.topic;
  }

  /** The record key as UTF-8 text. */
  protected defaultKey(record: KafkaInboundRecord): string | null {
    return readRecordKey(record.key);
  }

  /** The record timestamp; a record without a usable one is malformed. */
  protected defaultTime(record: KafkaInboundRecord): Date {
    return requireRecordTimestamp(record);
  }
}

const KAFKA_EVENTS = new KafkaDecodedEvents();

/**
 * Fills the fields a decoder left out (see {@link DecodedEvent}) and guards
 * against a decoder that returned no object, which would otherwise surface as
 * a retried `TypeError` instead of a malformed record.
 */
export function completeDecodedEvent(decoded: unknown, record: KafkaInboundRecord): EventEnvelope {
  return KAFKA_EVENTS.complete(decoded, record);
}

function extensionAttributes(body: Record<string, unknown>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(body).filter(
      (entry): entry is [string, string] =>
        !STRUCTURED_ATTRIBUTES.has(entry[0]) && typeof entry[1] === 'string',
    ),
  );
}

function requireRecordTimestamp(record: DecodableMessage): Date {
  const time = readRecordTimestamp(record.timestamp);
  if (time === undefined) {
    throw invalid('The event has no time and the record has no usable timestamp');
  }
  return time;
}

/**
 * Headers of a foreign record, never rejected: the last value of a repeated
 * header wins, and a value that is not valid UTF-8 is left out, because
 * replacing its bytes could make two different ids or types look equal.
 */
export function readHeadersLeniently(incoming: KafkaIncomingHeaders | undefined): EventHeaders {
  const entries: [string, string][] = [];
  for (const [name, raw] of Object.entries(incoming ?? {})) {
    const value = Array.isArray(raw)
      ? (raw as (KafkaBytes | string)[]).at(-1)
      : (raw as KafkaBytes | string | undefined);
    const text = value === undefined ? undefined : decodeUtf8OrUndefined(value);
    if (text !== undefined) {
      entries.push([name, text]);
    }
  }
  return Object.fromEntries(entries);
}

function decodeUtf8OrUndefined(value: KafkaBytes | string): string | undefined {
  if (typeof value === 'string') {
    return value;
  }
  try {
    return exactUtf8.decode(value);
  } catch {
    return undefined;
  }
}

function readKeyLeniently(key: KafkaBytes | null): string | null {
  if (key === null) {
    return null;
  }
  try {
    return readRecordKey(key);
  } catch {
    return Buffer.from(key).toString('base64');
  }
}

function resolveOccurredAt(
  option: PlainJsonDecoderOptions['occurredAt'],
  { plain, record }: { readonly plain: PlainJsonRecord; readonly record: KafkaInboundRecord },
): Date {
  const value: unknown = option?.(plain);
  if (value === undefined) {
    return requireRecordTimestamp(record);
  }
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    observeRejection(value);
    throw invalid('The occurredAt option returned no valid Date');
  }
  return value;
}
