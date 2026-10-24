import { assertEventType, type EventEnvelope } from '@relaybus/core';
import { InvalidKafkaMessageError, UnencodableEventError } from './errors.js';
import type { KafkaBytes, KafkaIncomingHeaders, KafkaOutgoingHeaders } from './kafka-client.js';

/** The wire form of one event: a Kafka record in CloudEvents binary content mode. */
export interface EncodedMessage {
  readonly key: string | null;
  readonly value: string;
  readonly headers: Record<string, string>;
}

/** The parts of an incoming Kafka record the decoder reads. */
export interface DecodableMessage {
  readonly key: KafkaBytes | null;
  readonly value: KafkaBytes | null;
  readonly headers?: KafkaIncomingHeaders | undefined;
  /** Record timestamp in epoch milliseconds, used when `ce_time` is absent. */
  readonly timestamp?: string | undefined;
}

const SPEC_VERSION = '1.0';
/** Name of the Kafka header that carries the media type of the record value. */
export const CONTENT_TYPE = 'content-type';
const JSON_MEDIA_TYPE = 'application/json';
const CLOUDEVENTS_PREFIX = 'ce_';
const ESCAPE_PREFIX = 'x-relaybus-h-';
const RESERVED_PREFIXES = [CLOUDEVENTS_PREFIX, 'x-relaybus-'];

const HEADER = {
  specVersion: 'ce_specversion',
  id: 'ce_id',
  type: 'ce_type',
  source: 'ce_source',
  time: 'ce_time',
  subject: 'ce_subject',
  correlationId: 'ce_correlationid',
  causationId: 'ce_causationid',
} as const;

const PROTOCOL_HEADERS: ReadonlySet<string> = new Set([...Object.values(HEADER), CONTENT_TYPE]);

/** RFC 3339 years have four digits. */
const MAXIMUM_RFC3339_YEAR = 9999;
const RFC3339_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/i;

const strictUtf8 = new TextDecoder('utf-8', { fatal: true });
const lenientUtf8 = new TextDecoder('utf-8');

/**
 * Encodes an envelope as a CloudEvents Kafka binary-mode record: the value is
 * the JSON payload, the event attributes travel as `ce_*` headers.
 *
 * User headers that would collide with protocol names (`ce_*`, `content-type`,
 * `x-relaybus-*`) are stored under an `x-relaybus-h-` prefix and restored on
 * decode, so every header survives a round trip unchanged.
 */
export function encodeEnvelope(envelope: EventEnvelope): EncodedMessage {
  const value = serializePayload(envelope);
  const entries: [string, string][] = Object.entries(envelope.headers).map(
    ([name, headerValue]) => [escapeUserHeaderName(name), headerValue],
  );
  entries.push(
    [HEADER.specVersion, SPEC_VERSION],
    [HEADER.id, envelope.id],
    [HEADER.type, envelope.type],
    [HEADER.source, envelope.source],
    [HEADER.time, formatTime(envelope)],
    [CONTENT_TYPE, JSON_MEDIA_TYPE],
  );
  if (envelope.key !== null) {
    entries.push([HEADER.subject, envelope.key]);
  }
  if (envelope.correlationId !== null) {
    entries.push([HEADER.correlationId, envelope.correlationId]);
  }
  if (envelope.causationId !== null) {
    entries.push([HEADER.causationId, envelope.causationId]);
  }
  // fromEntries defines own properties, so a header named `__proto__` is kept as data.
  return { key: envelope.key, value, headers: Object.fromEntries(entries) };
}

function serializePayload(envelope: EventEnvelope): string {
  let value: string | undefined;
  try {
    value = JSON.stringify(envelope.payload);
  } catch (error) {
    throw new UnencodableEventError(
      `Event ${envelope.id} has a payload that JSON cannot represent: ${(error as Error).message}`,
    );
  }
  if (value === undefined) {
    throw new UnencodableEventError(
      `Event ${envelope.id} has a payload that JSON cannot represent (undefined, a function or a symbol)`,
    );
  }
  return value;
}

function formatTime(envelope: EventEnvelope): string {
  const year = envelope.occurredAt.getUTCFullYear();
  if (Number.isNaN(year) || year < 0 || year > MAXIMUM_RFC3339_YEAR) {
    throw new UnencodableEventError(
      `Event ${envelope.id} has an occurredAt that is not an RFC 3339 instant (invalid date or year outside 0..9999)`,
    );
  }
  return envelope.occurredAt.toISOString();
}

/**
 * Decodes a Kafka record strictly: the CloudEvents attributes `ce_specversion`
 * (`1.0`), `ce_id`, `ce_type`, `ce_source`, a JSON `content-type` and a JSON
 * value are required. `ce_time` is optional in CloudEvents: when absent, the
 * Kafka record timestamp is used; when present it must be RFC 3339.
 *
 * @throws InvalidKafkaMessageError for anything else. It is the only error
 * thrown for malformed input, so callers can tell poison messages from bugs.
 */
export function decodeEnvelope(message: DecodableMessage): EventEnvelope {
  const headers = readHeaders(message.headers);
  if (headers.get(HEADER.specVersion) !== SPEC_VERSION) {
    throw invalid(`${HEADER.specVersion} must be '${SPEC_VERSION}'`);
  }
  const type = requireHeader(headers, HEADER.type);
  try {
    assertEventType(type);
  } catch (error) {
    throw invalid(`${HEADER.type} is not a valid event type: ${(error as Error).message}`);
  }
  assertJsonContentType(headers.get(CONTENT_TYPE));
  return {
    id: requireHeader(headers, HEADER.id),
    type,
    source: requireHeader(headers, HEADER.source),
    key: resolveKey(headers.get(HEADER.subject), message.key),
    payload: parsePayload(message.value),
    headers: collectUserHeaders(headers),
    correlationId: headers.get(HEADER.correlationId) ?? null,
    causationId: headers.get(HEADER.causationId) ?? null,
    occurredAt: resolveTime(headers.get(HEADER.time), message.timestamp),
  };
}

/** Creates the error thrown for every malformed record. */
export function invalid(reason: string): InvalidKafkaMessageError {
  return new InvalidKafkaMessageError(reason);
}

function escapeUserHeaderName(name: string): string {
  const lowered = name.toLowerCase();
  const reserved = lowered === CONTENT_TYPE || RESERVED_PREFIXES.some((p) => lowered.startsWith(p));
  return reserved ? `${ESCAPE_PREFIX}${name}` : name;
}

/** Reads headers strictly: a repeated header or a protocol header that is not UTF-8 is invalid. */
export function readHeaders(incoming: KafkaIncomingHeaders | undefined): Map<string, string> {
  const headers = new Map<string, string>();
  for (const [name, raw] of Object.entries(incoming ?? {})) {
    if (raw === undefined) {
      continue;
    }
    if (typeof raw !== 'string' && !(raw instanceof Uint8Array)) {
      throw invalid(`Header '${name}' is repeated`);
    }
    headers.set(name, decodeText(raw, PROTOCOL_HEADERS.has(name), `Header '${name}'`));
  }
  return headers;
}

/** Decodes header or value bytes as UTF-8; `strict` rejects invalid sequences. */
export function decodeText(raw: KafkaBytes | string, strict: boolean, label: string): string {
  if (typeof raw === 'string') {
    return raw;
  }
  try {
    return (strict ? strictUtf8 : lenientUtf8).decode(raw);
  } catch {
    throw invalid(`${label} is not valid UTF-8`);
  }
}

function requireHeader(headers: Map<string, string>, name: string): string {
  const value = headers.get(name);
  if (value === undefined || value.length === 0) {
    throw invalid(`Header '${name}' is missing or empty`);
  }
  return value;
}

function assertJsonContentType(contentType: string | undefined): void {
  const mediaType = contentType?.split(';')[0]?.trim().toLowerCase();
  if (mediaType !== JSON_MEDIA_TYPE) {
    throw invalid(`${CONTENT_TYPE} must be '${JSON_MEDIA_TYPE}', received '${contentType ?? ''}'`);
  }
}

function resolveTime(text: string | undefined, recordTimestamp: string | undefined): Date {
  if (text !== undefined && text.length > 0) {
    return parseTime(text);
  }
  const time = readRecordTimestamp(recordTimestamp);
  if (time === undefined) {
    throw invalid(`${HEADER.time} is missing and the record has no usable timestamp`);
  }
  return time;
}

/** The record timestamp (epoch milliseconds in a string) as a date, or undefined when unusable. */
export function readRecordTimestamp(recordTimestamp: string | undefined): Date | undefined {
  const time = new Date(Number(recordTimestamp));
  return recordTimestamp === undefined || recordTimestamp === '' || Number.isNaN(time.getTime())
    ? undefined
    : time;
}

/** Parses an RFC 3339 timestamp; anything else is invalid. `label` names the attribute in the error. */
export function parseTime(text: string, label: string = HEADER.time): Date {
  const time = new Date(text);
  if (!RFC3339_PATTERN.test(text) || Number.isNaN(time.getTime())) {
    throw invalid(`${label} is not an RFC 3339 timestamp: '${text}'`);
  }
  return time;
}

/** Parses the record value as UTF-8 JSON; a missing value or invalid JSON is invalid. */
export function parsePayload(value: KafkaBytes | null): unknown {
  if (value === null) {
    throw invalid('Message has no value');
  }
  const text = decodeText(value, true, 'Message value');
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw invalid('Message value is not valid JSON');
  }
}

function resolveKey(subject: string | undefined, recordKey: KafkaBytes | null): string | null {
  const keyText = readRecordKey(recordKey);
  if (subject !== undefined && keyText !== null && subject !== keyText) {
    throw invalid(`${HEADER.subject} does not match the message key`);
  }
  return subject ?? keyText;
}

/** The record key as UTF-8 text, or null when the record has none. Invalid UTF-8 is invalid. */
export function readRecordKey(recordKey: KafkaBytes | null): string | null {
  return recordKey === null ? null : decodeText(recordKey, true, 'Message key');
}

/** Headers that are not CloudEvents attributes, with escaped names restored. */
export function collectUserHeaders(headers: Map<string, string>): Record<string, string> {
  const entries: [string, string][] = [];
  for (const [name, value] of headers) {
    if (!PROTOCOL_HEADERS.has(name)) {
      entries.push([
        name.startsWith(ESCAPE_PREFIX) ? name.slice(ESCAPE_PREFIX.length) : name,
        value,
      ]);
    }
  }
  return Object.fromEntries(entries);
}

/** Converts incoming headers to what a producer accepts, keeping every value byte-for-byte. */
export function toOutgoingHeaders(
  incoming: KafkaIncomingHeaders | undefined,
): KafkaOutgoingHeaders {
  const entries: [string, Buffer | string | (Buffer | string)[]][] = [];
  for (const [name, raw] of Object.entries(incoming ?? {})) {
    if (raw !== undefined) {
      entries.push([
        name,
        Array.isArray(raw)
          ? (raw as (KafkaBytes | string)[]).map(toBufferOrString)
          : toBufferOrString(raw as KafkaBytes | string),
      ]);
    }
  }
  return Object.fromEntries(entries);
}

function toBufferOrString(value: KafkaBytes | string): Buffer | string {
  return typeof value === 'string' ? value : Buffer.from(value);
}
