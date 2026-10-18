import { RelayBusError, assertEventType, type EventEnvelope } from '@relaybus/core';
import type { KafkaBytes, KafkaIncomingHeaders, KafkaOutgoingHeaders } from './kafka-client.js';

/** A Kafka record that cannot be turned into an {@link EventEnvelope}. Never retried. */
export class InvalidKafkaMessageError extends RelayBusError {}

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
}

const SPEC_VERSION = '1.0';
const CONTENT_TYPE = 'content-type';
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
  const value = JSON.stringify(envelope.payload);
  if (value === undefined) {
    throw new RelayBusError(`Event ${envelope.id} has a payload that JSON cannot represent`);
  }
  const headers: Record<string, string> = {};
  for (const [name, headerValue] of Object.entries(envelope.headers)) {
    headers[escapeUserHeaderName(name)] = headerValue;
  }
  headers[HEADER.specVersion] = SPEC_VERSION;
  headers[HEADER.id] = envelope.id;
  headers[HEADER.type] = envelope.type;
  headers[HEADER.source] = envelope.source;
  headers[HEADER.time] = envelope.occurredAt.toISOString();
  headers[CONTENT_TYPE] = JSON_MEDIA_TYPE;
  if (envelope.key !== null) {
    headers[HEADER.subject] = envelope.key;
  }
  if (envelope.correlationId !== null) {
    headers[HEADER.correlationId] = envelope.correlationId;
  }
  if (envelope.causationId !== null) {
    headers[HEADER.causationId] = envelope.causationId;
  }
  return { key: envelope.key, value, headers };
}

/**
 * Decodes a Kafka record strictly: the CloudEvents attributes `ce_specversion`
 * (`1.0`), `ce_id`, `ce_type`, `ce_source` and an RFC 3339 `ce_time`, a JSON
 * `content-type` and a JSON value are all required.
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
    occurredAt: parseTime(requireHeader(headers, HEADER.time)),
  };
}

function invalid(reason: string): InvalidKafkaMessageError {
  return new InvalidKafkaMessageError(reason);
}

function escapeUserHeaderName(name: string): string {
  const lowered = name.toLowerCase();
  const reserved = lowered === CONTENT_TYPE || RESERVED_PREFIXES.some((p) => lowered.startsWith(p));
  return reserved ? `${ESCAPE_PREFIX}${name}` : name;
}

function readHeaders(incoming: KafkaIncomingHeaders | undefined): Map<string, string> {
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

function decodeText(raw: KafkaBytes | string, strict: boolean, label: string): string {
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

function parseTime(text: string): Date {
  const time = new Date(text);
  if (!RFC3339_PATTERN.test(text) || Number.isNaN(time.getTime())) {
    throw invalid(`${HEADER.time} is not an RFC 3339 timestamp: '${text}'`);
  }
  return time;
}

function parsePayload(value: KafkaBytes | null): unknown {
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
  const keyText = recordKey === null ? null : decodeText(recordKey, true, 'Message key');
  if (subject !== undefined && keyText !== null && subject !== keyText) {
    throw invalid(`${HEADER.subject} does not match the message key`);
  }
  return subject ?? keyText;
}

function collectUserHeaders(headers: Map<string, string>): Record<string, string> {
  const user: Record<string, string> = {};
  for (const [name, value] of headers) {
    if (PROTOCOL_HEADERS.has(name)) {
      continue;
    }
    user[name.startsWith(ESCAPE_PREFIX) ? name.slice(ESCAPE_PREFIX.length) : name] = value;
  }
  return user;
}

/** Converts incoming headers to what a producer accepts, keeping every value byte-for-byte. */
export function toOutgoingHeaders(
  incoming: KafkaIncomingHeaders | undefined,
): KafkaOutgoingHeaders {
  const outgoing: Record<string, Buffer | string | (Buffer | string)[]> = {};
  for (const [name, raw] of Object.entries(incoming ?? {})) {
    if (raw === undefined) {
      continue;
    }
    outgoing[name] = Array.isArray(raw)
      ? (raw as (KafkaBytes | string)[]).map(toBufferOrString)
      : toBufferOrString(raw as KafkaBytes | string);
  }
  return outgoing;
}

function toBufferOrString(value: KafkaBytes | string): Buffer | string {
  return typeof value === 'string' ? value : Buffer.from(value);
}
