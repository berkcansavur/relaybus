import { assertEventType, type EventEnvelope, type EventHeaders } from '@relaybus/core';
import { createCloudEventReaders, isJsonObject, mediaTypeOf } from '@relaybus/core/internal';
import type { AmqpMessage, AmqpMessageProperties } from './amqp-types.js';
import { MalformedMessageError } from './errors.js';
import { RFC3339_PATTERN, decodeEnvelope } from './wire-format.js';

/** AMQP `timestamp` properties count seconds. */
const MILLISECONDS_PER_SECOND = 1_000;

const { optionalAttribute, requiredAttribute, assertValidType, assertJsonDataContentType } =
  createCloudEventReaders((reason) => new MalformedMessageError(reason));

/**
 * The event a decoder extracted from a message. `id`, `type` and `payload` are
 * required; every other field may be left out and is then filled by the
 * transport: `source` with the `appId` property (else the routing key), `key`
 * with null, `headers` with none, `correlationId` and `causationId` with null,
 * and `occurredAt` with the `timestamp` property (else the time of receipt).
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
 * Turns a consumed message into an event, for queues fed by producers that do
 * not use relaybus. Must be synchronous and free of side effects: it runs again
 * on every redelivery, so the same message must always give the same event,
 * above all the same `id`, which is what the inbox deduplicates on.
 *
 * Anything it throws marks the message as malformed: it is dead-lettered and
 * the consumer moves on. An event the bus then rejects (empty id, invalid type
 * or time) is dead-lettered the same way.
 */
export type RabbitMqMessageDecoder = (message: AmqpMessage) => DecodedEvent;

/** A plain JSON message, decoded for the extractor options of {@link plainJsonDecoder}. */
export interface PlainJsonMessage {
  readonly routingKey: string;
  /** The AMQP properties as delivered (`messageId`, `type`, `appId`, `timestamp`, ...). */
  readonly properties: AmqpMessageProperties;
  /**
   * The message headers with string values, without the broker's and the
   * transport's own (`x-death`, `x-first-death-*`, `x-last-death-*`, `x-relaybus-*`).
   */
  readonly headers: EventHeaders;
  /** The body parsed as JSON. */
  readonly payload: unknown;
}

/** Options of {@link plainJsonDecoder}. Extractors returning undefined or `''` select the default. */
export interface PlainJsonDecoderOptions {
  /**
   * Event type handlers subscribe to: a fixed name for a queue that carries one
   * kind of event, or an extractor such as `fromHeader('event-type')`,
   * `fromPayloadField('type')` or `(message) => message.properties.type`.
   * Required; a message without a type is malformed.
   */
  readonly type: string | ((message: PlainJsonMessage) => string | undefined);
  /**
   * Event id the inbox deduplicates on. Default, and fallback when the
   * extractor finds none: the `messageId` property, which survives
   * redeliveries. A message with neither is malformed, because AMQP has no
   * other identity that is stable across redeliveries; give producers a
   * `messageId` (Spring AMQP can generate one with `setCreateMessageIds(true)`).
   */
  readonly id?: (message: PlainJsonMessage) => string | undefined;
  /** Ordering key. Default: none. */
  readonly key?: (message: PlainJsonMessage) => string | null | undefined;
  /** Logical name of the producer. Default: the `appId` property, else the routing key. */
  readonly source?: string | ((message: PlainJsonMessage) => string | undefined);
  /** When the event happened. Default: the `timestamp` property, else the time of receipt. */
  readonly occurredAt?: (message: PlainJsonMessage) => Date | undefined;
}

const STRUCTURED_MEDIA_TYPE = 'application/cloudevents+json';
const strictUtf8 = new TextDecoder('utf-8', { fatal: true });
const SPEC_VERSION = '1.0';
const SPEC_VERSION_HEADER = 'cloudEvents:specversion';
/** Headers the broker or this transport add on their own; never event headers. */
const INTERNAL_HEADER_PREFIXES = ['x-relaybus-', 'x-first-death-', 'x-last-death-'];
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
 * Decodes a CloudEvents 1.0 message in either content mode:
 * - structured (`contentType` property `application/cloudevents+json`): the
 *   body is a JSON object with `specversion`, `id`, `type`, `source` and
 *   optionally `time` (else the `timestamp` property, else the time of
 *   receipt), `subject` (the key), `data` (the payload, null when absent) and
 *   a JSON `datacontenttype`. `data_base64` is refused, because relaybus
 *   payloads are JSON. Extension attributes with string values (`traceparent`,
 *   ...) become event headers, as do the string message headers;
 *   `correlationid` and `causationid` fill their envelope fields.
 * - binary (anything else): exactly the default decoder, `cloudEvents:*`
 *   headers with the AMQP properties as fallback.
 *
 * @throws MalformedMessageError for a message that is neither.
 */
export function decodeCloudEvent(message: AmqpMessage): EventEnvelope {
  if (mediaTypeOf(message.properties.contentType) !== STRUCTURED_MEDIA_TYPE) {
    return decodeEnvelope(message);
  }
  const body = parseJsonBody(message.content);
  if (!isJsonObject(body)) {
    throw new MalformedMessageError('A structured CloudEvent must be a JSON object');
  }
  if (body['specversion'] !== SPEC_VERSION) {
    throw new MalformedMessageError(`specversion must be '${SPEC_VERSION}'`);
  }
  const type = requiredAttribute(body, 'type');
  assertValidType(type, 'type');
  assertJsonDataContentType(optionalAttribute(body, 'datacontenttype'));
  if (Object.hasOwn(body, 'data_base64')) {
    throw new MalformedMessageError(
      'data_base64 is not supported: relaybus payloads are JSON, send them as data',
    );
  }
  const time = optionalAttribute(body, 'time');
  return {
    id: requiredAttribute(body, 'id'),
    type,
    source: requiredAttribute(body, 'source'),
    key: optionalAttribute(body, 'subject') ?? null,
    payload: Object.hasOwn(body, 'data') ? body['data'] : null,
    headers: { ...readForeignHeaders(message.properties), ...extensionAttributes(body) },
    correlationId: optionalAttribute(body, 'correlationid') ?? null,
    causationId: optionalAttribute(body, 'causationid') ?? null,
    occurredAt: time === undefined ? propertyTimeOrNow(message.properties) : parseTime(time),
  };
}

/**
 * Creates a decoder for messages whose body is plain JSON, as published by a
 * service that does not use relaybus (plain amqplib, Spring AMQP's
 * `Jackson2JsonMessageConverter`, a Go or Python service): the body becomes the
 * payload, the string headers become event headers, and the options say where
 * the event type and id come from.
 *
 * A message that is a CloudEvent anyway (a `cloudEvents:specversion` header,
 * or a structured `application/cloudevents+json` body) is decoded with
 * {@link decodeCloudEvent} instead, so relaybus and foreign producers can feed
 * one queue. A body that is not JSON, a missing or invalid type, a missing id
 * and an extractor returning anything but a string are malformed.
 *
 * ```ts
 * new RabbitMqTransport({
 *   url,
 *   consumerGroup: 'billing',
 *   decode: plainJsonDecoder({ type: (message) => message.properties.type as string }),
 * });
 * ```
 *
 * @throws ConfigurationError when a fixed `type` is not a valid event type.
 */
export function plainJsonDecoder(options: PlainJsonDecoderOptions): RabbitMqMessageDecoder {
  if (typeof options.type === 'string') {
    assertEventType(options.type);
  }
  return (message) => {
    const { properties } = message;
    if (
      properties.headers?.[SPEC_VERSION_HEADER] !== undefined ||
      mediaTypeOf(properties.contentType) === STRUCTURED_MEDIA_TYPE
    ) {
      return decodeCloudEvent(message);
    }
    const plain: PlainJsonMessage = {
      routingKey: message.fields.routingKey,
      properties,
      headers: readForeignHeaders(properties),
      payload: parseJsonBody(message.content),
    };
    const type = resolveText(options.type, { message: plain, name: 'type' });
    if (type === undefined) {
      throw new MalformedMessageError('Message has no event type: the type option found none');
    }
    assertValidType(type, 'The event type');
    const id =
      resolveText(options.id, { message: plain, name: 'id' }) ?? nonEmptyText(properties.messageId);
    if (id === undefined) {
      throw new MalformedMessageError(
        'Message has no id: set the messageId property on the producer or configure the id option',
      );
    }
    return {
      id,
      type,
      source:
        resolveText(options.source, { message: plain, name: 'source' }) ?? defaultSource(message),
      key: resolveText(options.key, { message: plain, name: 'key' }) ?? null,
      payload: plain.payload,
      headers: plain.headers,
      correlationId: null,
      causationId: null,
      occurredAt: resolveOccurredAt(options.occurredAt, plain),
    };
  };
}

/**
 * Fills the fields a decoder left out (see {@link DecodedEvent}) and guards
 * against a decoder that returned no object.
 */
export function completeDecodedEvent(decoded: unknown, message: AmqpMessage): EventEnvelope {
  if (typeof decoded !== 'object' || decoded === null) {
    throw new MalformedMessageError('The decoder must return an event object synchronously');
  }
  if ('then' in decoded) {
    observeRejection(decoded);
    throw new MalformedMessageError('The decoder must return an event object synchronously');
  }
  const event = decoded as DecodedEvent;
  return {
    id: event.id,
    type: event.type,
    source: event.source ?? defaultSource(message),
    key: event.key ?? null,
    payload: event.payload,
    headers: event.headers ?? {},
    correlationId: event.correlationId ?? null,
    causationId: event.causationId ?? null,
    occurredAt: event.occurredAt ?? propertyTimeOrNow(message.properties),
  };
}

/** Parses the body as UTF-8 JSON, refusing invalid UTF-8 instead of replacing it. */
export function parseJsonBody(content: Buffer): unknown {
  let text: string;
  try {
    text = strictUtf8.decode(content);
  } catch {
    throw new MalformedMessageError('Message body is not valid UTF-8');
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new MalformedMessageError('Message body is not valid JSON');
  }
}

export function nonEmptyText(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function defaultSource(message: AmqpMessage): string {
  return nonEmptyText(message.properties.appId) ?? message.fields.routingKey;
}

function parseTime(text: string): Date {
  const time = new Date(text);
  if (!RFC3339_PATTERN.test(text) || Number.isNaN(time.getTime())) {
    throw new MalformedMessageError(`time is not an RFC 3339 timestamp: '${text}'`);
  }
  return time;
}

/** The `timestamp` property (seconds), else the time of receipt. */
function propertyTimeOrNow(properties: AmqpMessageProperties): Date {
  const { timestamp } = properties;
  return typeof timestamp === 'number' && Number.isFinite(timestamp)
    ? new Date(timestamp * MILLISECONDS_PER_SECOND)
    : new Date();
}

function extensionAttributes(body: Record<string, unknown>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(body).filter(
      (entry): entry is [string, string] =>
        !STRUCTURED_ATTRIBUTES.has(entry[0]) && typeof entry[1] === 'string',
    ),
  );
}

export function readForeignHeaders(properties: AmqpMessageProperties): Record<string, string> {
  return Object.fromEntries(
    Object.entries(properties.headers ?? {}).filter(
      (entry): entry is [string, string] =>
        typeof entry[1] === 'string' &&
        !INTERNAL_HEADER_PREFIXES.some((prefix) => entry[0].startsWith(prefix)),
    ),
  );
}

function resolveText<TValue extends string | null>(
  option: TValue | ((message: PlainJsonMessage) => TValue | undefined) | undefined,
  { message, name }: { readonly message: PlainJsonMessage; readonly name: string },
): string | undefined {
  const value: unknown = typeof option === 'function' ? option(message) : option;
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== 'string') {
    observeRejection(value);
    throw new MalformedMessageError(
      `The ${name} option returned ${typeof value}, expected a string`,
    );
  }
  return value.length === 0 ? undefined : value;
}

function resolveOccurredAt(
  option: PlainJsonDecoderOptions['occurredAt'],
  message: PlainJsonMessage,
): Date {
  const value: unknown = option?.(message);
  if (value === undefined) {
    return propertyTimeOrNow(message.properties);
  }
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    observeRejection(value);
    throw new MalformedMessageError('The occurredAt option returned no valid Date');
  }
  return value;
}

/**
 * Observes the rejection of a promise an async decoder or extractor returned by
 * mistake: the value is refused as malformed, and an unobserved rejection would
 * otherwise crash the process.
 */
function observeRejection(value: unknown): void {
  if (typeof value === 'object' && value !== null && 'then' in value) {
    Promise.resolve(value).catch(() => undefined);
  }
}
