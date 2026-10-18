import type { EventEnvelope, EventHeaders } from '@relaybus/core';
import type { AmqpMessage, AmqpPublishOptions } from './amqp-types.js';
import { MalformedMessageError } from './errors.js';

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
 * fallback for events produced by other CloudEvents libraries.
 */
const ATTRIBUTE_PREFIX = 'cloudEvents:';
const EVENT_HEADER_PREFIX = 'relaybus:';
const SPEC_VERSION = '1.0';
const JSON_CONTENT_TYPE = 'application/json';

/** A serialized event, ready for `channel.publish`. */
export interface EncodedMessage {
  readonly content: Buffer;
  readonly options: AmqpPublishOptions;
}

/** Serializes an envelope. Throws when the payload cannot be turned into JSON. */
export function encodeEnvelope(envelope: EventEnvelope): EncodedMessage {
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
    headers[`${EVENT_HEADER_PREFIX}${name}`] = value;
  }
  return {
    content: Buffer.from(JSON.stringify(envelope.payload ?? null), 'utf8'),
    options: {
      contentType: JSON_CONTENT_TYPE,
      persistent: true,
      messageId: envelope.id,
      type: envelope.type,
      appId: envelope.source,
      timestamp: Math.floor(envelope.occurredAt.getTime() / 1_000),
      ...(envelope.correlationId === null ? {} : { correlationId: envelope.correlationId }),
      headers,
    },
  };
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
    occurredAt: parseTime(text(attributes[`${ATTRIBUTE_PREFIX}time`]), properties.timestamp),
  };
}

function extractEventHeaders(attributes: Readonly<Record<string, unknown>>): EventHeaders {
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(attributes)) {
    if (name.startsWith(EVENT_HEADER_PREFIX) && typeof value === 'string') {
      headers[name.slice(EVENT_HEADER_PREFIX.length)] = value;
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

function parseTime(isoTime: string | undefined, timestampSeconds: unknown): Date {
  const occurredAt =
    isoTime !== undefined
      ? new Date(isoTime)
      : typeof timestampSeconds === 'number'
        ? new Date(timestampSeconds * 1_000)
        : new Date(Number.NaN);
  if (Number.isNaN(occurredAt.getTime())) {
    throw new MalformedMessageError('Message has no valid time (header cloudEvents:time)');
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
