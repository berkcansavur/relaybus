import fc from 'fast-check';
import { describe, expect, it, vi } from 'vitest';
import { ConfigurationError, NonRetryableError, type EventEnvelope } from '@relaybus/core';
import {
  InvalidKafkaMessageError,
  KafkaTransport,
  UnencodableEventError,
  cloudEventsBinary,
  cloudEventsStructured,
  decodeEnvelope,
  encodeEnvelope,
  jsonEnvelope,
  plainJsonDecoder,
  type KafkaCodec,
  type KafkaEncodedRecord,
  type KafkaInboundRecord,
  type KafkaJsonEnvelopeMapping,
  type KafkaTransportOptions,
} from '../src/index.js';
import { completeDecodedEvent } from '../src/inbound-decoders.js';
import { FakeKafka, createEnvelope, createLogger } from './fakes.js';

function setup(options: Partial<KafkaTransportOptions> = {}) {
  const kafka = new FakeKafka();
  const transport = new KafkaTransport({
    kafka,
    groupId: 'billing',
    logger: createLogger(),
    processing: { maxAttempts: 2, initialBackoff: 1, maxBackoff: 1 },
    ...options,
  });
  return { transport, producer: kafka.producer_, consumer: kafka.consumer_ };
}

async function subscribed(options: Partial<KafkaTransportOptions> = {}) {
  const context = setup(options);
  const received: EventEnvelope[] = [];
  await context.transport.subscribe(['orders'], async (envelope) => {
    received.push(envelope);
  });
  return { ...context, received };
}

/** A produced record as the consumer of `topic` receives it. */
function toInbound(record: KafkaEncodedRecord, offset = '7'): KafkaInboundRecord {
  const bytes = (value: string | Buffer): Buffer => Buffer.from(value);
  return {
    topic: 'orders',
    partition: 0,
    offset,
    key: record.key === null ? null : bytes(record.key),
    value: record.value === null ? null : bytes(record.value),
    headers: Object.fromEntries(
      Object.entries(record.headers).map(([name, value]) => [name, bytes(value as string)]),
    ),
    timestamp: record.timestamp ?? '1767225600000',
  };
}

/** What the transport hands the bus for a record: the codec's decode, completed. */
function decodeThrough(codec: KafkaCodec, record: KafkaInboundRecord): EventEnvelope {
  return completeDecodedEvent(codec.decode(record), record);
}

/** A company monorepo envelope: `{ eventName, messageId, data, meta }`. */
const BODY_ENVELOPE: KafkaJsonEnvelopeMapping = {
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
const MASS_TRANSIT: KafkaJsonEnvelopeMapping = {
  type: { body: 'messageType.0' },
  id: { body: 'messageId' },
  payload: { body: 'message' },
  correlationId: { body: 'correlationId' },
  causationId: { body: 'initiatorId' },
  occurredAt: { body: 'sentTime' },
  headers: { body: 'headers' },
  contentType: 'application/vnd.masstransit+json',
};

/** A producer that writes the bare payload and names the type in a header. */
const PLAIN_JSON: KafkaJsonEnvelopeMapping = {
  type: { header: 'event-type' },
  id: 'record-position',
  payload: 'whole-body',
};

describe('cloudEventsBinary, the default codec', () => {
  // Golden record: the wire format of 0.2 byte for byte. Change it only on purpose.
  const GOLDEN = {
    topic: 'orders.events',
    acks: -1,
    messages: [
      {
        key: 'order-1',
        value: '{"orderId":"1"}',
        headers: {
          traceparent: '00-abc-def-01',
          ce_specversion: '1.0',
          ce_id: '0190a000-0000-7000-8000-0000000000aa',
          ce_type: 'order.placed.v1',
          ce_source: 'orders',
          ce_time: '2026-01-01T00:00:00.000Z',
          'content-type': 'application/json',
          ce_subject: 'order-1',
          ce_correlationid: 'flow-1',
          ce_causationid: 'cause-1',
        },
      },
    ],
  };

  it.each<[string, Partial<KafkaTransportOptions>]>([
    ['no codec option', {}],
    ['codec: cloudEventsBinary', { codec: cloudEventsBinary }],
    ['the deprecated decode option', { decode: plainJsonDecoder({ type: 'order.placed.v1' }) }],
  ])('produces the golden record with %s', async (_label, options) => {
    const { transport, producer } = setup(options);
    await transport.publish(createEnvelope(), 'orders.events');
    expect(producer.sent).toEqual([GOLDEN]);
    expect(JSON.stringify(producer.sent[0])).toBe(JSON.stringify(GOLDEN));
  });

  it('keeps reading strictly: a structured CloudEvent is dead-lettered', async () => {
    const { consumer, received, producer } = await subscribed({ codec: cloudEventsBinary });
    await consumer.deliver(toInbound(cloudEventsStructured.encode(createEnvelope())));
    expect(received).toEqual([]);
    expect(producer.sent[0]!.topic).toBe('orders.dead');
  });

  it('refuses codec and decode together', () => {
    expect(() =>
      setup({ codec: cloudEventsStructured, decode: plainJsonDecoder({ type: 'a.b' }) }),
    ).toThrow(ConfigurationError);
  });
});

describe('cloudEventsStructured', () => {
  it('produces one application/cloudevents+json document, the key and the event headers', async () => {
    const { transport, producer } = setup({ codec: cloudEventsStructured });
    await transport.publish(createEnvelope({ headers: { 'content-type': 'x' } }), 'orders.events');
    expect(producer.sent[0]!.messages).toEqual([
      {
        key: 'order-1',
        value:
          '{"specversion":"1.0","id":"0190a000-0000-7000-8000-0000000000aa","type":"order.placed.v1","source":"orders","time":"2026-01-01T00:00:00.000Z","datacontenttype":"application/json","subject":"order-1","correlationid":"flow-1","causationid":"cause-1","data":{"orderId":"1"}}',
        headers: {
          'x-relaybus-h-content-type': 'x',
          'content-type': 'application/cloudevents+json',
        },
      },
    ]);
  });

  it('reads structured and binary records', async () => {
    const { consumer, received } = await subscribed({ codec: cloudEventsStructured });
    await consumer.deliver(toInbound(cloudEventsStructured.encode(createEnvelope())));
    await consumer.deliver(toInbound(encodeEnvelope(createEnvelope({ id: 'binary-1' }))));
    expect(received).toEqual([createEnvelope(), createEnvelope({ id: 'binary-1' })]);
  });

  it.each([
    ['an undefined payload', createEnvelope({ payload: undefined })],
    ['an invalid date', createEnvelope({ occurredAt: new Date(Number.NaN) })],
  ])('refuses %s with a non-retryable error', (_label, envelope) => {
    expect(() => cloudEventsStructured.encode(envelope)).toThrow(UnencodableEventError);
  });
});

describe('jsonEnvelope', () => {
  it('writes a body envelope with the record key and no record timestamp', () => {
    const record = jsonEnvelope(BODY_ENVELOPE).encode(createEnvelope());
    expect({ ...record, value: JSON.parse(record.value as string) as unknown }).toEqual({
      key: 'order-1',
      value: {
        eventName: 'order.placed.v1',
        messageId: '0190a000-0000-7000-8000-0000000000aa',
        data: { orderId: '1' },
        meta: {
          key: 'order-1',
          source: 'orders',
          occurredAt: '2026-01-01T00:00:00.000Z',
          correlationId: 'flow-1',
          causationId: 'cause-1',
          headers: { traceparent: '00-abc-def-01' },
        },
      },
      headers: { 'content-type': 'application/json' },
    });
  });

  it('writes the MassTransit envelope with its content type', () => {
    const record = jsonEnvelope(MASS_TRANSIT).encode(createEnvelope());
    expect(JSON.parse(record.value as string)).toEqual({
      messageId: '0190a000-0000-7000-8000-0000000000aa',
      messageType: ['order.placed.v1'],
      message: { orderId: '1' },
      correlationId: 'flow-1',
      initiatorId: 'cause-1',
      sentTime: '2026-01-01T00:00:00.000Z',
      headers: { traceparent: '00-abc-def-01' },
    });
    expect(record.headers).toEqual({ 'content-type': 'application/vnd.masstransit+json' });
  });

  it('writes the bare payload, the type header and the time as the record timestamp', async () => {
    const { transport, producer } = setup({ codec: jsonEnvelope(PLAIN_JSON) });
    await transport.publish(createEnvelope(), 'orders.events');
    expect(producer.sent[0]!.messages).toEqual([
      {
        key: 'order-1',
        value: '{"orderId":"1"}',
        headers: {
          traceparent: '00-abc-def-01',
          'event-type': 'order.placed.v1',
          'content-type': 'application/json',
          'relaybus-event-id': '0190a000-0000-7000-8000-0000000000aa',
        },
        timestamp: '1767225600000',
      },
    ]);
  });

  it('reads a plain JSON record with the record position as id and the transport defaults', async () => {
    const { consumer, received } = await subscribed({ codec: jsonEnvelope(PLAIN_JSON) });
    await consumer.deliver({
      key: Buffer.from('customer-1'),
      value: Buffer.from('{"orderId":"9"}'),
      headers: {
        'event-type': Buffer.from('order.placed.v1'),
        'content-type': Buffer.from('application/json'),
        origin: Buffer.from('legacy-java'),
      },
      timestamp: '1767225600000',
    });
    expect(received).toEqual([
      {
        id: 'kafka:orders:0:7',
        type: 'order.placed.v1',
        source: 'orders',
        key: 'customer-1',
        payload: { orderId: '9' },
        headers: { origin: 'legacy-java' },
        correlationId: null,
        causationId: null,
        occurredAt: new Date(1767225600000),
      },
    ]);
  });

  it.each([
    [
      'a header the codec writes itself',
      createEnvelope({ headers: { 'content-type': 'x' } }),
      /content-type/,
    ],
    ['a header the mapping uses', createEnvelope({ headers: { 'event-type': 'x' } }), /event-type/],
    [
      'the event id header of the codec',
      createEnvelope({ headers: { 'relaybus-event-id': 'x' } }),
      /relaybus-event-id/,
    ],
    ['an undefined payload', createEnvelope({ payload: undefined }), /cannot represent/],
    ['a function payload', createEnvelope({ payload: () => 1 }), /cannot represent/],
    ['a symbol payload', createEnvelope({ payload: Symbol('x') }), /cannot represent/],
    [
      'a BigInt inside the payload',
      createEnvelope({ payload: { total: 10n } }),
      /cannot represent/,
    ],
    ['an invalid date', createEnvelope({ occurredAt: new Date(Number.NaN) }), /RFC 3339/],
  ])('refuses %s with a non-retryable error', (_label, envelope, message) => {
    const attempt = (): unknown => jsonEnvelope(PLAIN_JSON).encode(envelope);
    expect(attempt).toThrow(UnencodableEventError);
    expect(attempt).toThrow(NonRetryableError);
    expect(attempt).toThrow(message);
  });

  it('reads the event id relaybus wrote under record-position, so a resent event dedupes', async () => {
    const { consumer, received } = await subscribed({ codec: jsonEnvelope(PLAIN_JSON) });
    const record = toInbound(jsonEnvelope(PLAIN_JSON).encode(createEnvelope()));
    await consumer.deliver(record);
    await consumer.deliver({ ...record, offset: '8' });
    await consumer.deliver({
      ...record,
      offset: '9',
      headers: { ...record.headers, 'relaybus-event-id': Buffer.from('') },
    });
    expect(received.map((event) => event.id)).toEqual([
      '0190a000-0000-7000-8000-0000000000aa',
      '0190a000-0000-7000-8000-0000000000aa',
      'kafka:orders:0:9',
    ]);
    expect(received[0]!.headers).toEqual({ traceparent: '00-abc-def-01' });
  });

  it('writes no event id header when the mapping places the id', () => {
    const codec = jsonEnvelope({ ...PLAIN_JSON, id: { header: 'message-id' } });
    expect(codec.encode(createEnvelope()).headers).not.toHaveProperty('relaybus-event-id');
  });

  it('refuses an incomplete mapping at construction', () => {
    expect(() =>
      jsonEnvelope({ type: { fixed: 'a.b' } } as unknown as KafkaJsonEnvelopeMapping),
    ).toThrow(ConfigurationError);
  });
});

describe('invalid records under every codec', () => {
  it.each<[string, KafkaCodec, Record<string, string>, string | null, RegExp]>([
    [
      'structured: a JSON array',
      cloudEventsStructured,
      { 'content-type': 'application/cloudevents+json' },
      '[1]',
      /must be a JSON object/,
    ],
    [
      'structured: no value',
      cloudEventsStructured,
      { 'content-type': 'application/cloudevents+json' },
      null,
      /no value/,
    ],
    [
      'body envelope: no type',
      jsonEnvelope(BODY_ENVELOPE),
      {},
      '{"messageId":"m","data":{}}',
      /no type at 'eventName'/,
    ],
    ['body envelope: not JSON', jsonEnvelope(BODY_ENVELOPE), {}, '{nope', /not valid JSON/],
    [
      'MassTransit: no message',
      jsonEnvelope(MASS_TRANSIT),
      {},
      '{"messageId":"m","messageType":["a.b"]}',
      /no payload at 'message'/,
    ],
    [
      'plain JSON: no type header',
      jsonEnvelope(PLAIN_JSON),
      {},
      '{"orderId":"1"}',
      /no type at header 'event-type'/,
    ],
    [
      'custom: a decoder that crashes',
      {
        encode: encodeEnvelope,
        decode: (record) => {
          if (String(record.value) === '{}') {
            throw new TypeError('boom');
          }
          return decodeEnvelope(record);
        },
      },
      {},
      '{}',
      /boom/,
    ],
  ])('dead-letters %s and keeps consuming', async (_label, codec, headers, value, reason) => {
    const { consumer, received, producer } = await subscribed({ codec });
    await consumer.deliver({
      value: value === null ? null : Buffer.from(value),
      headers: Object.fromEntries(
        Object.entries(headers).map(([name, text]) => [name, Buffer.from(text)]),
      ),
      timestamp: '1767225600000',
    });
    await consumer.deliver({ ...toInbound(codec.encode(createEnvelope()), '8') });
    expect(producer.sent[0]!.topic).toBe('orders.dead');
    expect(producer.sent[0]!.messages[0]!.headers['relaybus-invalid-reason']).toMatch(reason);
    expect(received).toHaveLength(1);
  });

  it('reports a malformed record with the transport error', () => {
    const record = toInbound({ key: null, value: '{}', headers: {} });
    expect(() => jsonEnvelope(PLAIN_JSON).decode(record)).toThrow(InvalidKafkaMessageError);
  });
});

describe('round trip', () => {
  const name = fc
    .stringMatching(/^[a-z][a-z0-9_-]{0,15}$/)
    .filter((header) => !header.startsWith('x-') && header !== 'content-type');
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

  it.each<[string, KafkaCodec]>([
    ['cloudEventsBinary', cloudEventsBinary],
    ['cloudEventsStructured', cloudEventsStructured],
    ['jsonEnvelope with a body envelope', jsonEnvelope(BODY_ENVELOPE)],
    [
      'jsonEnvelope with header fields and the record timestamp',
      jsonEnvelope({
        type: { header: 'x-type' },
        id: { header: 'x-id' },
        payload: 'whole-body',
        source: { header: 'x-source' },
        correlationId: { header: 'x-correlation' },
        causationId: { header: 'x-causation' },
      }),
    ],
  ])('decode(encode(event)) is the event for %s', (_label, codec) => {
    fc.assert(
      fc.property(envelopes, (event) => {
        expect(decodeThrough(codec, toInbound(codec.encode(event)))).toEqual(normalized(event));
      }),
    );
  });

  it('decodes MassTransit back to the event except the source, which is the topic', () => {
    fc.assert(
      fc.property(envelopes, (event) => {
        const codec = jsonEnvelope(MASS_TRANSIT);
        expect(decodeThrough(codec, toInbound(codec.encode(event)))).toEqual({
          ...normalized(event),
          source: 'orders',
        });
      }),
    );
  });
});

describe('custom codec', () => {
  it('uses both functions of an object codec', async () => {
    const encode = vi.fn((envelope: EventEnvelope) => ({
      key: envelope.key,
      value: Buffer.from(String(envelope.payload)),
      headers: { kind: envelope.type },
    }));
    const codec: KafkaCodec = {
      encode,
      decode: (record) => ({
        id: `custom:${record.offset}`,
        type: String(record.headers?.['kind']),
        payload: String(record.value),
      }),
    };
    const { transport, producer, consumer, received } = await subscribed({ codec });
    await transport.publish(createEnvelope({ payload: 'hello' }), 'orders');
    await consumer.deliver(toInbound(producer.sent[0]!.messages[0]!));
    expect(received).toMatchObject([
      { id: 'custom:7', type: 'order.placed.v1', payload: 'hello', key: 'order-1' },
    ]);
  });
});
