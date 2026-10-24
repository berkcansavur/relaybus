import fc from 'fast-check';
import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError, NonRetryableError, type EventEnvelope } from '@relaybus/core';
import {
  AmqpLimitExceededError,
  MalformedMessageError,
  RabbitMqTransport,
  RabbitMqUnencodableEventError,
  UnserializablePayloadError,
  cloudEventsBinary,
  cloudEventsStructured,
  jsonEnvelope,
  plainJsonDecoder,
  type AmqpMessage,
  type EncodedMessage,
  type RabbitMqCodec,
  type RabbitMqJsonEnvelopeMapping,
  type RabbitMqTransportOptions,
} from '../src/index.js';
import { resolveSettings } from '../src/options.js';
import { decodeEnvelope, encodeEnvelope } from '../src/wire-format.js';
import { FakeConnection, createEnvelope, createLogger, waitFor } from './fakes.js';

async function connected(options: Partial<RabbitMqTransportOptions> = {}) {
  const connections: FakeConnection[] = [];
  const transport = new RabbitMqTransport({
    connect: async () => {
      const connection = new FakeConnection();
      connections.push(connection);
      return connection;
    },
    consumerGroup: 'billing',
    logger: createLogger(),
    handlerRetry: { attempts: 1 },
    ...options,
  });
  await transport.connect();
  const received: EventEnvelope[] = [];
  await transport.subscribe(['orders.*'], async (envelope) => {
    received.push(envelope);
  });
  const connection = connections[0]!;
  return {
    transport,
    received,
    publisher: connection.publisher,
    consumer: connection.consumerChannel,
  };
}

/** A published message as a consumer receives it. */
function toDelivery({ content, options }: EncodedMessage): AmqpMessage {
  return { content, fields: { routingKey: 'orders.placed' }, properties: { ...options } };
}

/** What the transport hands the bus for a message: the codec's decode, completed. */
function decodeThrough(codec: RabbitMqCodec, message: AmqpMessage): EventEnvelope {
  return resolveSettings({ url: 'amqp://localhost', codec }).decode(message);
}

/** A company monorepo envelope: `{ eventName, messageId, data, meta }`. */
const BODY_ENVELOPE: RabbitMqJsonEnvelopeMapping = {
  type: { body: 'eventName' },
  id: { body: 'messageId' },
  payload: { body: 'data' },
  key: { body: 'meta.key' },
  source: { body: 'meta.source' },
  occurredAt: { body: 'meta.occurredAt' },
  correlationId: { body: 'meta.correlationId' },
  causationId: { body: 'meta.causationId' },
  headers: { body: 'meta.headers' },
};

/** MassTransit's JSON envelope. */
const MASS_TRANSIT: RabbitMqJsonEnvelopeMapping = {
  type: { body: 'messageType.0' },
  id: { body: 'messageId' },
  payload: { body: 'message' },
  key: { body: 'headers.ordering-key' },
  correlationId: { body: 'correlationId' },
  causationId: { body: 'initiatorId' },
  occurredAt: { body: 'sentTime' },
  contentType: 'application/vnd.masstransit+json',
};

/** A producer that writes the bare payload, with the type in a header and the AMQP messageId. */
const PLAIN_JSON: RabbitMqJsonEnvelopeMapping = {
  type: { header: 'event-type' },
  id: 'message-id-property',
  payload: 'whole-body',
};

describe('cloudEventsBinary, the default codec', () => {
  // Golden message: the wire format of 0.2 byte for byte. Change it only on purpose.
  const GOLDEN = {
    routingKey: 'orders.placed',
    content: Buffer.from('{"orderId":"1"}'),
    options: {
      contentType: 'application/json',
      persistent: true,
      messageId: '0190a000-0000-7000-8000-000000000001',
      timestamp: 1767262830,
      type: 'order.placed.v1',
      appId: 'orders-service',
      correlationId: 'flow-1',
      headers: {
        'cloudEvents:specversion': '1.0',
        'cloudEvents:id': '0190a000-0000-7000-8000-000000000001',
        'cloudEvents:type': 'order.placed.v1',
        'cloudEvents:source': 'orders-service',
        'cloudEvents:time': '2026-01-01T10:20:30.456Z',
        'cloudEvents:datacontenttype': 'application/json',
        'cloudEvents:subject': 'order-1',
        'cloudEvents:causationid': 'cause-1',
        'cloudEvents:correlationid': 'flow-1',
        'relaybus:traceparent': '00-abc-def-01',
      },
      mandatory: true,
    },
  };

  it.each<[string, Partial<RabbitMqTransportOptions>]>([
    ['no codec option', {}],
    ['codec: cloudEventsBinary', { codec: cloudEventsBinary }],
    ['the deprecated decode option', { decode: plainJsonDecoder({ type: 'order.placed.v1' }) }],
  ])('publishes the golden message with %s', async (_label, options) => {
    const { transport, publisher } = await connected(options);
    await transport.publish(createEnvelope(), 'orders.placed');
    expect(publisher.published).toEqual([GOLDEN]);
    expect(JSON.stringify(publisher.published[0])).toBe(JSON.stringify(GOLDEN));
    await transport.close();
  });

  it('decodes with the strict binary decoder, unwrapped', () => {
    expect(resolveSettings({ url: 'amqp://localhost', codec: cloudEventsBinary })).toMatchObject({
      encode: encodeEnvelope,
      decode: decodeEnvelope,
    });
  });

  it('refuses codec and decode together', () => {
    expect(
      () =>
        new RabbitMqTransport({
          url: 'amqp://localhost',
          codec: cloudEventsStructured,
          decode: plainJsonDecoder({ type: 'a.b' }),
        }),
    ).toThrow(ConfigurationError);
  });
});

describe('cloudEventsStructured', () => {
  it('writes one application/cloudevents+json document with the binary-mode properties', () => {
    const { content, options } = cloudEventsStructured.encode(createEnvelope());
    expect(content.toString('utf8')).toBe(
      '{"specversion":"1.0","id":"0190a000-0000-7000-8000-000000000001","type":"order.placed.v1","source":"orders-service","time":"2026-01-01T10:20:30.456Z","datacontenttype":"application/json","subject":"order-1","correlationid":"flow-1","causationid":"cause-1","data":{"orderId":"1"}}',
    );
    expect(options).toEqual({
      persistent: true,
      messageId: '0190a000-0000-7000-8000-000000000001',
      timestamp: 1767262830,
      type: 'order.placed.v1',
      appId: 'orders-service',
      correlationId: 'flow-1',
      contentType: 'application/cloudevents+json',
      headers: { traceparent: '00-abc-def-01' },
    });
  });

  it('reads structured and binary messages', async () => {
    const { transport, consumer, received } = await connected({ codec: cloudEventsStructured });
    consumer.deliver!(toDelivery(cloudEventsStructured.encode(createEnvelope())));
    consumer.deliver!(toDelivery(encodeEnvelope(createEnvelope({ id: 'binary-1' }))));
    await waitFor(() => received.length === 2);
    expect(received).toEqual(
      expect.arrayContaining([createEnvelope(), createEnvelope({ id: 'binary-1' })]),
    );
    await transport.close();
  });

  it.each([
    ['x-death', RabbitMqUnencodableEventError],
    ['x-relaybus-attempt', RabbitMqUnencodableEventError],
    ['h'.repeat(256), AmqpLimitExceededError],
  ])('refuses the event header %s', (name, ErrorClass) => {
    const attempt = (): unknown =>
      cloudEventsStructured.encode(createEnvelope({ headers: { [name]: 'x' } }));
    expect(attempt).toThrow(ErrorClass);
    expect(attempt).toThrow(NonRetryableError);
  });

  it('refuses a payload JSON cannot represent', () => {
    expect(() => cloudEventsStructured.encode(createEnvelope({ payload: new Date() }))).toThrow(
      UnserializablePayloadError,
    );
  });
});

describe('jsonEnvelope', () => {
  it('writes a body envelope with the binary-mode properties', () => {
    const { content, options } = jsonEnvelope(BODY_ENVELOPE).encode(createEnvelope());
    expect(JSON.parse(content.toString('utf8'))).toEqual({
      eventName: 'order.placed.v1',
      messageId: '0190a000-0000-7000-8000-000000000001',
      data: { orderId: '1' },
      meta: {
        key: 'order-1',
        source: 'orders-service',
        occurredAt: '2026-01-01T10:20:30.456Z',
        correlationId: 'flow-1',
        causationId: 'cause-1',
        headers: { traceparent: '00-abc-def-01' },
      },
    });
    expect(options).toMatchObject({
      contentType: 'application/json',
      persistent: true,
      messageId: '0190a000-0000-7000-8000-000000000001',
      appId: 'orders-service',
      headers: {},
    });
  });

  it('writes the MassTransit envelope with its content type', () => {
    const { content, options } = jsonEnvelope(MASS_TRANSIT).encode(createEnvelope());
    expect(JSON.parse(content.toString('utf8'))).toEqual({
      messageId: '0190a000-0000-7000-8000-000000000001',
      messageType: ['order.placed.v1'],
      message: { orderId: '1' },
      headers: { 'ordering-key': 'order-1' },
      correlationId: 'flow-1',
      initiatorId: 'cause-1',
      sentTime: '2026-01-01T10:20:30.456Z',
    });
    expect(options.contentType).toBe('application/vnd.masstransit+json');
    expect(options.headers).toEqual({ traceparent: '00-abc-def-01' });
  });

  it('writes the bare payload with the type header', () => {
    const { content, options } = jsonEnvelope(PLAIN_JSON).encode(createEnvelope());
    expect(content.toString('utf8')).toBe('{"orderId":"1"}');
    expect(options.headers).toEqual({
      traceparent: '00-abc-def-01',
      'event-type': 'order.placed.v1',
    });
  });

  it('reads a plain JSON message with the messageId property and the transport defaults', async () => {
    const { transport, consumer, received } = await connected({ codec: jsonEnvelope(PLAIN_JSON) });
    consumer.deliver!({
      content: Buffer.from('{"orderId":"9"}'),
      fields: { routingKey: 'orders.placed' },
      properties: {
        messageId: 'msg-9',
        timestamp: 1767262830,
        headers: { 'event-type': 'order.placed.v1', origin: 'legacy-java', 'x-death': [] },
      },
    });
    await waitFor(() => received.length === 1);
    expect(received[0]).toEqual({
      id: 'msg-9',
      type: 'order.placed.v1',
      source: 'orders.placed',
      key: null,
      payload: { orderId: '9' },
      headers: { origin: 'legacy-java' },
      correlationId: null,
      causationId: null,
      occurredAt: new Date(1767262830_000),
    });
    await transport.close();
  });

  it.each([
    [
      'a header the mapping uses',
      createEnvelope({ headers: { 'event-type': 'x' } }),
      RabbitMqUnencodableEventError,
    ],
    [
      'a header the broker reserves',
      createEnvelope({ headers: { 'x-first-death-queue': 'q' } }),
      RabbitMqUnencodableEventError,
    ],
    [
      'a type the fixed route does not carry',
      createEnvelope({ type: 'other.v1' }),
      RabbitMqUnencodableEventError,
    ],
    [
      'a payload JSON cannot represent',
      createEnvelope({ payload: new Map() }),
      UnserializablePayloadError,
    ],
    ['an id beyond 255 bytes', createEnvelope({ id: 'i'.repeat(256) }), AmqpLimitExceededError],
  ])('refuses %s with a non-retryable error', (_label, envelope, ErrorClass) => {
    const codec = jsonEnvelope({
      ...PLAIN_JSON,
      type: envelope.type === 'other.v1' ? { fixed: 'order.placed.v1' } : PLAIN_JSON.type,
    });
    const attempt = (): unknown => codec.encode(envelope);
    expect(attempt).toThrow(ErrorClass);
    expect(attempt).toThrow(NonRetryableError);
  });
});

describe('invalid messages under every codec', () => {
  const crashing: RabbitMqCodec = {
    encode: encodeEnvelope,
    decode: (message) => {
      if (message.content.toString('utf8') === '{}') {
        throw new TypeError('boom');
      }
      return decodeEnvelope(message);
    },
  };

  it.each<[string, RabbitMqCodec, AmqpMessage['properties'], string, string]>([
    [
      'structured: a JSON array',
      cloudEventsStructured,
      { contentType: 'application/cloudevents+json' },
      '[1]',
      'MalformedMessageError',
    ],
    [
      'body envelope: no type',
      jsonEnvelope(BODY_ENVELOPE),
      {},
      '{"messageId":"m","data":{}}',
      'MalformedMessageError',
    ],
    ['MassTransit: not JSON', jsonEnvelope(MASS_TRANSIT), {}, '{nope', 'MalformedMessageError'],
    [
      'plain JSON: no messageId',
      jsonEnvelope(PLAIN_JSON),
      { headers: { 'event-type': 'a.b' } },
      '{}',
      'MalformedMessageError',
    ],
    ['custom: a decoder that crashes', crashing, {}, '{}', 'TypeError'],
  ])('dead-letters %s and keeps consuming', async (_label, codec, properties, body, errorName) => {
    const { transport, consumer, publisher, received } = await connected({ codec });
    consumer.deliver!({
      content: Buffer.from(body),
      fields: { routingKey: 'orders.placed' },
      properties,
    });
    consumer.deliver!(toDelivery(codec.encode(createEnvelope())));
    await waitFor(() => received.length === 1 && publisher.published.length === 1);
    expect(publisher.published[0]!.routingKey).toBe('relaybus.billing');
    expect(publisher.published[0]!.options.headers).toMatchObject({
      'x-relaybus-dead-letter-error-name': errorName,
    });
    await transport.close();
  });

  it('reports a malformed message with the transport error', () => {
    const message: AmqpMessage = {
      content: Buffer.from('{}'),
      fields: { routingKey: 'r' },
      properties: {},
    };
    expect(() => jsonEnvelope(BODY_ENVELOPE).decode(message)).toThrow(MalformedMessageError);
  });
});

describe('round trip', () => {
  const name = fc
    .stringMatching(/^[a-z][a-z0-9_-]{0,15}$/)
    .filter((header) => !header.startsWith('x-'));
  const text = fc.string({ minLength: 1, maxLength: 24 });
  const envelopes = fc.record({
    id: text,
    type: fc.stringMatching(/^[a-z][a-z0-9._:/-]{0,30}$/),
    source: text,
    key: fc.option(text, { nil: null }),
    payload: fc.jsonValue(),
    headers: fc.dictionary(name, fc.string(), { maxKeys: 4 }),
    correlationId: fc.option(text, { nil: null }),
    causationId: fc.option(text, { nil: null }),
    occurredAt: fc.date({
      min: new Date('0000-01-01T00:00:00Z'),
      max: new Date('9999-12-31T23:59:59Z'),
      noInvalidDate: true,
    }),
  });
  const normalized = (event: EventEnvelope): EventEnvelope => ({
    ...event,
    payload: JSON.parse(JSON.stringify(event.payload)) as unknown,
  });

  it.each<[string, RabbitMqCodec]>([
    ['cloudEventsBinary', cloudEventsBinary],
    ['cloudEventsStructured', cloudEventsStructured],
    ['jsonEnvelope with a body envelope', jsonEnvelope(BODY_ENVELOPE)],
    ['jsonEnvelope with MassTransit', jsonEnvelope(MASS_TRANSIT)],
    [
      'jsonEnvelope with header fields and the messageId property',
      jsonEnvelope({
        type: { header: 'x-type' },
        id: 'message-id-property',
        payload: 'whole-body',
        key: { header: 'x-key' },
        occurredAt: { header: 'x-time' },
        correlationId: { header: 'x-correlation' },
        causationId: { header: 'x-causation' },
      }),
    ],
  ])('decode(encode(event)) is the event for %s', (_label, codec) => {
    fc.assert(
      fc.property(envelopes, (event) => {
        expect(decodeThrough(codec, toDelivery(codec.encode(event)))).toEqual(normalized(event));
      }),
    );
  });
});

describe('custom codec', () => {
  it('uses both functions of an object codec', async () => {
    const encode = vi.fn((envelope: EventEnvelope) => ({
      content: Buffer.from(String(envelope.payload)),
      options: {
        contentType: 'text/plain',
        persistent: true,
        messageId: envelope.id,
        headers: { kind: envelope.type },
      },
    }));
    const codec: RabbitMqCodec = {
      encode,
      decode: (message) => ({
        id: String(message.properties.messageId),
        type: String(message.properties.headers?.['kind']),
        payload: message.content.toString('utf8'),
      }),
    };
    const { transport, publisher, consumer, received } = await connected({ codec });
    await transport.publish(createEnvelope({ payload: 'hello' }), 'orders.placed');
    const { content, options } = publisher.published[0]!;
    consumer.deliver!(toDelivery({ content, options }));
    await waitFor(() => received.length === 1);
    expect(received[0]).toMatchObject({ type: 'order.placed.v1', payload: 'hello', key: null });
    await transport.close();
  });
});
