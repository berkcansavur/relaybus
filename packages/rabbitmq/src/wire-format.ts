import type { EventEnvelope, EventHeaders } from '@relaybus/core';
import { writeStructuredCloudEvent } from '@relaybus/core/internal';
import type { AmqpMessage, AmqpPublishOptions } from './amqp-types.js';
import {
  AmqpLimitExceededError,
  MalformedMessageError,
  RabbitMqUnencodableEventError,
} from './errors.js';
import { assertJsonPayload } from './json-payload.js';

/** AMQP `timestamp` properties count seconds. */
const MILLISECONDS_PER_SECOND = 1_000;

/**
 * Wire format: CloudEvents 1.0 in binary content mode, following the CloudEvents
 * AMQP protocol binding adapted to RabbitMQ's AMQP 0-9-1.
 *
 * - Body: the JSON-encoded payload (`contentType` `application/json`).
 * - Headers `cloudEvents:specversion`, `cloudEvents:id`, `cloudEvents:type`,
 *   `cloudEvents:source`, `cloudEvents:time` (RFC 3339, millisecond precision),
 *   `cloudEvents:datacontenttype`, and when present `cloudEvents:subject` (the
 *   key) and the extension attributes `cloudEvents:causationid` and
 *   `cloudEvents:correlationid`.
 * - AMQP properties mirror the same values for broker tooling: `messageId`,
 *   `type`, `appId` (source), `correlationId`, `timestamp` (seconds), plus
 *   `deliveryMode` 2 (persistent).
 * - Event headers travel as `relaybus:<name>` message headers, so they cannot
 *   collide with CloudEvents attributes or broker headers such as `x-death`.
 *
 * On receive the `cloudEvents:` headers win; the AMQP properties are the
 * fallback for events produced by other CloudEvents libraries (the AMQP
 * timestamp only when `cloudEvents:time` is absent; a present but malformed
 * time is rejected).
 *
 * AMQP 0-9-1 short strings (message properties, header names, routing keys) are
 * limited to 255 bytes. The event id, a header name or a routing key beyond
 * that is refused with {@link AmqpLimitExceededError}, because the event could
 * never be published. The `type`, `appId` and `correlationId` mirrors are
 * skipped when they do not fit; the `cloudEvents:` headers always carry them.
 */
const ATTRIBUTE_PREFIX = 'cloudEvents:';
const EVENT_HEADER_PREFIX = 'relaybus:';
const SPEC_VERSION = '1.0';
const JSON_CONTENT_TYPE = 'application/json';
const STRUCTURED_CONTENT_TYPE = 'application/cloudevents+json';
/** Header name prefixes the broker or the transport own; an event header there would be lost on read. */
const RESERVED_HEADER_PREFIXES = ['x-death', 'x-first-death-', 'x-last-death-', 'x-relaybus-'];
const SHORT_STRING_LIMIT_BYTES = 255;
/** RFC 3339 date-time, the format of the CloudEvents `time` attribute. */
export const RFC3339_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/i;

/** A serialized event, ready for `channel.publish`. */
export interface EncodedMessage {
  readonly content: Buffer;
  readonly options: AmqpPublishOptions;
}

/**
 * Serializes an envelope.
 *
 * @throws {UnserializablePayloadError} the payload is not representable as JSON
 * @throws {AmqpLimitExceededError} the id or a header name exceeds 255 bytes
 */
export function encodeEnvelope(envelope: EventEnvelope): EncodedMessage {
  assertJsonPayload(envelope.payload, envelope.id);
  assertShortString('Event id', envelope.id);
  const headers: Record<string, string> = {
    [`${ATTRIBUTE_PREFIX}specversion`]: SPEC_VERSION,
    [`${ATTRIBUTE_PREFIX}id`]: envelope.id,
    [`${ATTRIBUTE_PREFIX}type`]: envelope.type,
    [`${ATTRIBUTE_PREFIX}source`]: envelope.source,
    [`${ATTRIBUTE_PREFIX}time`]: envelope.occurredAt.toISOString(),
    [`${ATTRIBUTE_PREFIX}datacontenttype`]: JSON_CONTENT_TYPE,
  };
  if (envelope.key !== null) {
    headers[`${ATTRIBUTE_PREFIX}subject`] = envelope.key;
  }
  if (envelope.causationId !== null) {
    headers[`${ATTRIBUTE_PREFIX}causationid`] = envelope.causationId;
  }
  if (envelope.correlationId !== null) {
    headers[`${ATTRIBUTE_PREFIX}correlationid`] = envelope.correlationId;
  }
  for (const [name, value] of Object.entries(envelope.headers)) {
    assertShortString(
      `Header name '${EVENT_HEADER_PREFIX}${name}'`,
      `${EVENT_HEADER_PREFIX}${name}`,
    );
    headers[`${EVENT_HEADER_PREFIX}${name}`] = value;
  }
  return {
    content: Buffer.from(JSON.stringify(envelope.payload), 'utf8'),
    options: {
      contentType: JSON_CONTENT_TYPE,
      persistent: true,
      messageId: envelope.id,
      timestamp: Math.floor(envelope.occurredAt.getTime() / MILLISECONDS_PER_SECOND),
      ...mirror('type', envelope.type),
      ...mirror('appId', envelope.source),
      ...(envelope.correlationId === null ? {} : mirror('correlationId', envelope.correlationId)),
      headers,
    },
  };
}

/**
 * Serializes an envelope in CloudEvents structured content mode: the body is
 * one `application/cloudevents+json` document with the attributes and `data`.
 * The AMQP properties are those of binary mode; event headers travel as
 * message headers under their own names, as structured-mode readers expect.
 *
 * @throws {UnserializablePayloadError} the payload is not representable as JSON
 * @throws {AmqpLimitExceededError} the id or a header name exceeds 255 bytes
 * @throws {RabbitMqUnencodableEventError} a header name the broker or the transport reserves
 */
export function encodeStructuredEnvelope(envelope: EventEnvelope): EncodedMessage {
  assertJsonPayload(envelope.payload, envelope.id);
  assertShortString('Event id', envelope.id);
  const document = writeStructuredCloudEvent(envelope, {
    time: envelope.occurredAt.toISOString(),
    data: JSON.stringify(envelope.payload),
  });
  return {
    content: Buffer.from(document, 'utf8'),
    options: {
      ...messageProperties(envelope),
      contentType: STRUCTURED_CONTENT_TYPE,
      headers: plainHeaders(envelope, envelope.headers),
    },
  };
}

/** The AMQP properties every relaybus format writes; `contentType` and `headers` are the format's own. */
export function messageProperties(
  envelope: EventEnvelope,
): Omit<AmqpPublishOptions, 'headers' | 'contentType'> {
  return {
    persistent: true,
    messageId: envelope.id,
    timestamp: Math.floor(envelope.occurredAt.getTime() / MILLISECONDS_PER_SECOND),
    ...mirror('type', envelope.type),
    ...mirror('appId', envelope.source),
    ...(envelope.correlationId === null ? {} : mirror('correlationId', envelope.correlationId)),
  };
}

/**
 * Message headers under their own names, for formats read by other services.
 * A name the broker or the transport reserves is refused, because the reader
 * would drop it; a name beyond 255 bytes is refused by AMQP.
 */
export function plainHeaders(
  envelope: EventEnvelope,
  headers: EventHeaders,
): Record<string, string> {
  for (const name of Object.keys(headers)) {
    if (RESERVED_HEADER_PREFIXES.some((prefix) => name.startsWith(prefix))) {
      throw new RabbitMqUnencodableEventError(
        `Event ${envelope.id} has a header '${name}', a name the broker or the transport reserves`,
      );
    }
    assertShortString(`Header name '${name}'`, name);
  }
  return Object.fromEntries(Object.entries(headers));
}

/**
 * Throws {@link AmqpLimitExceededError} when `value` does not fit an AMQP short
 * string (255 bytes of UTF-8), which amqplib would otherwise reject on every
 * attempt.
 */
export function assertShortString(label: string, value: string): void {
  const bytes = Buffer.byteLength(value, 'utf8');
  if (bytes > SHORT_STRING_LIMIT_BYTES) {
    throw new AmqpLimitExceededError(
      `${label} is ${bytes} bytes, above the AMQP limit of ${SHORT_STRING_LIMIT_BYTES} bytes`,
    );
  }
}

function mirror<TName extends 'type' | 'appId' | 'correlationId'>(
  name: TName,
  value: string,
): { [Name in TName]?: string } {
  return Buffer.byteLength(value, 'utf8') > SHORT_STRING_LIMIT_BYTES
    ? {}
    : ({ [name]: value } as { [Name in TName]?: string });
}

/** Rebuilds an envelope. Throws {@link MalformedMessageError} when the message is not a relaybus event. */
export function decodeEnvelope(message: AmqpMessage): EventEnvelope {
  const { properties } = message;
  const attributes = properties.headers ?? {};
  if (attributes[`${ATTRIBUTE_PREFIX}specversion`] !== SPEC_VERSION) {
    throw new MalformedMessageError(
      `Message is not a CloudEvents ${SPEC_VERSION} event (header ${ATTRIBUTE_PREFIX}specversion)`,
    );
  }
  return {
    id: required('id', text(attributes[`${ATTRIBUTE_PREFIX}id`]) ?? text(properties.messageId)),
    type: required('type', text(attributes[`${ATTRIBUTE_PREFIX}type`]) ?? text(properties.type)),
    source: required(
      'source',
      text(attributes[`${ATTRIBUTE_PREFIX}source`]) ?? text(properties.appId),
    ),
    key: text(attributes[`${ATTRIBUTE_PREFIX}subject`]) ?? null,
    payload: parsePayload(message.content),
    headers: extractEventHeaders(attributes),
    correlationId:
      text(attributes[`${ATTRIBUTE_PREFIX}correlationid`]) ??
      text(properties.correlationId) ??
      null,
    causationId: text(attributes[`${ATTRIBUTE_PREFIX}causationid`]) ?? null,
    occurredAt: parseTime(attributes[`${ATTRIBUTE_PREFIX}time`], properties.timestamp),
  };
}

function extractEventHeaders(attributes: Readonly<Record<string, unknown>>): EventHeaders {
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(attributes)) {
    if (name.startsWith(EVENT_HEADER_PREFIX) && typeof value === 'string') {
      // defineProperty, because assigning `__proto__` would set the prototype and lose the header.
      Object.defineProperty(headers, name.slice(EVENT_HEADER_PREFIX.length), {
        value,
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
  }
  return headers;
}

function parsePayload(content: Buffer): unknown {
  try {
    return JSON.parse(content.toString('utf8')) as unknown;
  } catch {
    throw new MalformedMessageError('Message body is not valid JSON');
  }
}

function parseTime(rawTime: unknown, timestampSeconds: unknown): Date {
  if (rawTime === undefined) {
    const fallback =
      typeof timestampSeconds === 'number'
        ? new Date(timestampSeconds * MILLISECONDS_PER_SECOND)
        : undefined;
    if (fallback === undefined || Number.isNaN(fallback.getTime())) {
      throw new MalformedMessageError('Message has no valid time (header cloudEvents:time)');
    }
    return fallback;
  }
  const occurredAt =
    typeof rawTime === 'string' && RFC3339_PATTERN.test(rawTime) ? new Date(rawTime) : undefined;
  if (occurredAt === undefined || Number.isNaN(occurredAt.getTime())) {
    throw new MalformedMessageError(
      'Message time (header cloudEvents:time) is not an RFC 3339 timestamp',
    );
  }
  return occurredAt;
}

function text(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function required(attribute: string, value: string | undefined): string {
  if (value === undefined || value.length === 0) {
    throw new MalformedMessageError(
      `Message has no ${attribute} (header ${ATTRIBUTE_PREFIX}${attribute})`,
    );
  }
  return value;
}
