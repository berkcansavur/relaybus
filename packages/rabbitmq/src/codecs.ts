import type { EventEnvelope } from '@relaybus/core';
import { createJsonEnvelopeFormat, type JsonEnvelopeMapping } from '@relaybus/core/internal';
import type { AmqpMessage } from './amqp-types.js';
import { MalformedMessageError, RabbitMqUnencodableEventError } from './errors.js';
import {
  completeDecodedEvent,
  decodeCloudEvent,
  nonEmptyText,
  parseJsonBody,
  readForeignHeaders,
  type RabbitMqMessageDecoder,
} from './inbound-decoders.js';
import { assertJsonPayload } from './json-payload.js';
import {
  assertShortString,
  decodeEnvelope,
  encodeEnvelope,
  encodeStructuredEnvelope,
  messageProperties,
  plainHeaders,
  type EncodedMessage,
} from './wire-format.js';

/**
 * The wire format of a {@link RabbitMqTransport}: how an event becomes a
 * message (`encode`, on publish) and a consumed message becomes an event
 * (`decode`).
 *
 * Both must be synchronous and free of side effects. What `encode` throws
 * fails the publish: the outbox retries it, unless it is a `NonRetryableError`
 * such as {@link RabbitMqUnencodableEventError}. Give every message a
 * `messageId`: the transport matches broker returns of unroutable messages by
 * it. What `decode` throws marks the message as malformed: it is
 * dead-lettered and the consumer moves on. See {@link RabbitMqMessageDecoder}
 * for the decode contract.
 */
export interface RabbitMqCodec {
  readonly encode: (envelope: EventEnvelope) => EncodedMessage;
  readonly decode: RabbitMqMessageDecoder;
}

/**
 * CloudEvents 1.0 binary content mode, the default: the body is the JSON
 * payload, the attributes travel as `cloudEvents:*` headers mirrored in the
 * AMQP properties. Decoding is strict: any other message is malformed.
 */
export const cloudEventsBinary: RabbitMqCodec = Object.freeze({
  encode: encodeEnvelope,
  decode: decodeEnvelope,
});

/**
 * CloudEvents 1.0 structured content mode: the body is one
 * `application/cloudevents+json` document with the attributes and `data`.
 * Decoding accepts structured and binary mode ({@link decodeCloudEvent}).
 * Event headers travel under their own names; names starting with `x-death`,
 * `x-first-death-`, `x-last-death-` or `x-relaybus-` are refused.
 */
export const cloudEventsStructured: RabbitMqCodec = Object.freeze({
  encode: encodeStructuredEnvelope,
  decode: decodeCloudEvent,
});

/**
 * Where each value of a JSON message lives, for {@link jsonEnvelope}. Besides
 * `{ body }` and `{ header }`, the id may be `'message-id-property'`: the
 * AMQP `messageId` property, which the codec always writes and which survives
 * redeliveries; a message without one is malformed.
 */
export type RabbitMqJsonEnvelopeMapping = JsonEnvelopeMapping<'message-id-property'>;

const MESSAGE_ID_PROPERTY = 'message-id-property';
const DEFAULT_CONTENT_TYPE = 'application/json';

/**
 * A JSON wire format declared by a mapping, in both directions: for queues
 * shared with services that wrap events in their own envelope
 * (`{ "eventName": ..., "data": {...} }`, MassTransit) or send the bare
 * payload. Every message also gets the AMQP properties of binary mode:
 * `messageId` (the event id), `type`, `appId` (the source), `correlationId`,
 * `timestamp` (seconds) and `persistent`. Event
 * headers that are message headers are written as they are, so a name the
 * mapping uses for another field, or one the broker reserves, is refused
 * with {@link RabbitMqUnencodableEventError}.
 *
 * ```ts
 * codec: jsonEnvelope({
 *   type: { body: 'eventName' },
 *   id: 'message-id-property',
 *   payload: { body: 'data' },
 * })
 * ```
 *
 * @throws ConfigurationError for an incomplete or self-overlapping mapping.
 */
export function jsonEnvelope(mapping: RabbitMqJsonEnvelopeMapping): RabbitMqCodec {
  const format = createJsonEnvelopeFormat(mapping, {
    transportIds: [MESSAGE_ID_PROPERTY],
    failures: {
      invalid: (reason) => new MalformedMessageError(reason),
      unencodable: (reason) => new RabbitMqUnencodableEventError(reason),
    },
  });
  const contentType = mapping.contentType ?? DEFAULT_CONTENT_TYPE;
  return {
    encode: (envelope) => {
      assertJsonPayload(envelope.payload, envelope.id);
      assertShortString('Event id', envelope.id);
      const { body, headers } = format.write(envelope);
      return {
        content: Buffer.from(JSON.stringify(body), 'utf8'),
        options: {
          ...messageProperties(envelope),
          contentType,
          headers: plainHeaders(envelope, headers),
        },
      };
    },
    decode: (message) => {
      const fields = format.read({
        body: parseJsonBody(message.content),
        headers: readForeignHeaders(message.properties),
      });
      return { ...fields, id: fields.id ?? requireMessageId(message) };
    },
  };
}

function requireMessageId(message: AmqpMessage): string {
  const id = nonEmptyText(message.properties.messageId);
  if (id === undefined) {
    throw new MalformedMessageError('Message has no id: the messageId property is missing');
  }
  return id;
}

/**
 * The event a {@link RabbitMqTransport} with `codec` hands to the bus for `message`: the codec's
 * `decode`, with what it leaves out filled in (see `DecodedEvent`). Throws for a malformed
 * message. For codec contract tests (`rabbitMqCodecContract` of `@relaybus/testing`).
 */
export function decodeRabbitMqMessage(codec: RabbitMqCodec, message: AmqpMessage): EventEnvelope {
  if (codec === cloudEventsBinary) {
    return decodeEnvelope(message);
  }
  return completeDecodedEvent(codec.decode(message), message);
}
