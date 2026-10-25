import fc from 'fast-check';
import { describe, expect, expectTypeOf, it } from 'vitest';
import { ConfigurationError, type EventEnvelope } from '@relaybus/core';
import {
  createJsonEnvelopeFormat,
  writeStructuredCloudEvent,
  type JsonEnvelopeFormat,
  type JsonEnvelopeMapping,
} from '@relaybus/core/internal';

class Invalid extends Error {}
class Unencodable extends Error {}

const failures = {
  invalid: (reason: string) => new Invalid(reason),
  unencodable: (reason: string) => new Unencodable(reason),
};

type Mapping = JsonEnvelopeMapping<'transport-id'>;

function format(mapping: Mapping): JsonEnvelopeFormat {
  return createJsonEnvelopeFormat(mapping, { transportIds: ['transport-id'], failures });
}

function envelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: 'evt-1',
    type: 'order.placed.v1',
    source: 'orders',
    key: 'order-1',
    payload: { orderId: '1' },
    headers: { traceparent: '00-abc-def-01' },
    correlationId: 'flow-1',
    causationId: 'cause-1',
    occurredAt: new Date('2026-01-01T10:20:30.456Z'),
    ...overrides,
  };
}

/** A body envelope as company monorepos use it: metadata beside `data`, context under `meta`. */
const BODY_ENVELOPE: Mapping = {
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

describe('jsonEnvelope mapping types', () => {
  it('requires type, id and payload, and keeps body paths out of a payload-only mapping', () => {
    // @ts-expect-error the type location is required
    expectTypeOf({ id: 'transport-id', payload: 'whole-body' } as const).toExtend<Mapping>();
    // @ts-expect-error the id strategy is required
    expectTypeOf({ type: { fixed: 'a.b' }, payload: 'whole-body' } as const).toExtend<Mapping>();
    // @ts-expect-error the payload location is required
    expectTypeOf({ type: { fixed: 'a.b' }, id: 'transport-id' } as const).toExtend<Mapping>();
    const bodyPathInPayloadOnly = {
      type: { body: 't' },
      id: 'transport-id',
      payload: 'whole-body',
    } as const;
    // @ts-expect-error a payload-only body has no other fields
    expectTypeOf(bodyPathInPayloadOnly).toExtend<Mapping>();
    const unknownId = { type: { fixed: 'a.b' }, id: 'other', payload: 'whole-body' } as const;
    // @ts-expect-error only the transport's own id strategies are accepted
    expectTypeOf(unknownId).toExtend<Mapping>();
    expectTypeOf(BODY_ENVELOPE).toExtend<Mapping>();
  });
});

describe('createJsonEnvelopeFormat configuration', () => {
  it.each<[string, unknown, RegExp]>([
    ['no mapping', undefined, /needs a mapping object/],
    ['no type', { id: 'transport-id', payload: 'whole-body' }, /needs a type/],
    [
      'an invalid fixed type',
      { type: { fixed: 'no spaces' }, id: 'transport-id', payload: 'whole-body' },
      /Event type/,
    ],
    ['a type of nothing', { type: {}, id: 'transport-id', payload: 'whole-body' }, /type must be/],
    [
      'an empty header name',
      { type: { header: '' }, id: 'transport-id', payload: 'whole-body' },
      /type must be/,
    ],
    [
      'no id',
      { type: { fixed: 'a.b' }, payload: 'whole-body' },
      /needs an id: .* one of 'transport-id'/,
    ],
    [
      'an unknown id strategy',
      { type: { fixed: 'a.b' }, id: 'other', payload: 'whole-body' },
      /needs an id/,
    ],
    ['no payload', { type: { fixed: 'a.b' }, id: 'transport-id' }, /needs a payload/],
    [
      'a payload of nothing',
      { type: { fixed: 'a.b' }, id: 'transport-id', payload: {} },
      /needs a payload/,
    ],
    [
      'an invalid headers option',
      { type: { fixed: 'a.b' }, id: 'transport-id', payload: { body: 'data' }, headers: 'all' },
      /headers must be/,
    ],
    [
      'an empty path segment',
      { type: { body: 'meta..type' }, id: 'transport-id', payload: { body: 'data' } },
      /non-empty names, received 'meta..type'/,
    ],
    [
      'a header mapped twice',
      { type: { header: 'h' }, id: { header: 'h' }, payload: 'whole-body' },
      /header 'h' twice/,
    ],
    [
      'equal body paths',
      { type: { body: 'data' }, id: 'transport-id', payload: { body: 'data' } },
      /'data' and 'data' overlap/,
    ],
    [
      'a path inside another',
      { type: { fixed: 'a.b' }, id: { body: 'data.id' }, payload: { body: 'data' } },
      /'data.id' and 'data' overlap/,
    ],
    [
      'a path around another',
      {
        type: { fixed: 'a.b' },
        id: { body: 'meta' },
        payload: { body: 'data' },
        headers: { body: 'meta.headers' },
      },
      /'meta' and 'meta.headers' overlap/,
    ],
    [
      'an index beside a name',
      { type: { body: 'list.0' }, id: { body: 'list.name' }, payload: { body: 'data' } },
      /mix an array index and a name/,
    ],
  ])('refuses %s', (_label, mapping, message) => {
    const attempt = (): unknown => format(mapping as Mapping);
    expect(attempt).toThrow(ConfigurationError);
    expect(attempt).toThrow(message);
  });

  it('accepts the explicit default headers option', () => {
    const plain = format({
      type: { fixed: 'a.b' },
      id: 'transport-id',
      payload: 'whole-body',
      headers: 'message-headers',
    });
    expect(plain.read({ body: 1, headers: { a: 'x' } }).headers).toEqual({ a: 'x' });
  });
});

describe('writing', () => {
  it('writes a body envelope with nested metadata', () => {
    expect(format(BODY_ENVELOPE).write(envelope())).toEqual({
      body: {
        eventName: 'order.placed.v1',
        messageId: 'evt-1',
        data: { orderId: '1' },
        meta: {
          key: 'order-1',
          source: 'orders',
          occurredAt: '2026-01-01T10:20:30.456Z',
          correlationId: 'flow-1',
          causationId: 'cause-1',
          headers: { traceparent: '00-abc-def-01' },
        },
      },
      headers: {},
    });
  });

  it('writes header fields and event headers as message headers, leaving out null values', () => {
    const mapping: Mapping = {
      type: { header: 'event-type' },
      id: { header: 'message-id' },
      payload: 'whole-body',
      key: { header: 'key' },
      correlationId: { header: 'correlation-id' },
    };
    expect(format(mapping).write(envelope({ key: null }))).toEqual({
      body: { orderId: '1' },
      headers: {
        traceparent: '00-abc-def-01',
        'event-type': 'order.placed.v1',
        'message-id': 'evt-1',
        'correlation-id': 'flow-1',
      },
    });
  });

  it('writes nothing for a transport id and a fixed type the event has', () => {
    const plain = format({
      type: { fixed: 'order.placed.v1' },
      id: 'transport-id',
      payload: 'whole-body',
    });
    expect(plain.write(envelope({ headers: {} }))).toEqual({ body: { orderId: '1' }, headers: {} });
  });

  it('refuses an event whose type differs from the fixed type of the route', () => {
    const plain = format({
      type: { fixed: 'order.shipped.v1' },
      id: 'transport-id',
      payload: 'whole-body',
    });
    expect(() => plain.write(envelope())).toThrow(Unencodable);
    expect(() => plain.write(envelope())).toThrow(/carries only 'order.shipped.v1'/);
  });

  it('refuses an event header whose name a mapped field uses', () => {
    const plain = format({
      type: { header: 'event-type' },
      id: 'transport-id',
      payload: 'whole-body',
    });
    const attempt = (): unknown => plain.write(envelope({ headers: { 'event-type': 'x' } }));
    expect(attempt).toThrow(Unencodable);
    expect(attempt).toThrow(/header 'event-type', which the mapping uses/);
  });

  it.each([
    new Date(Number.NaN),
    new Date('+010000-01-01T00:00:00Z'),
    new Date('-000001-01-01T00:00:00Z'),
  ])('refuses occurredAt %s', (occurredAt) => {
    expect(() => format(BODY_ENVELOPE).write(envelope({ occurredAt }))).toThrow(Unencodable);
  });

  it('creates an array before an index segment, as MassTransit expects for messageType', () => {
    const massTransit = format({
      type: { body: 'messageType.0' },
      id: { body: 'messageId' },
      payload: { body: 'message' },
    });
    expect(massTransit.write(envelope()).body).toEqual({
      messageType: ['order.placed.v1'],
      messageId: 'evt-1',
      message: { orderId: '1' },
    });
  });

  it('keeps a field named __proto__ as data', () => {
    const odd = format({
      type: { body: '__proto__.type' },
      id: { body: 'id' },
      payload: { body: 'data' },
    });
    const { body } = odd.write(envelope({ headers: { ['__proto__']: 'x' } }));
    expect(Object.getPrototypeOf(body)).toBe(Object.prototype);
    expect(JSON.parse(JSON.stringify(body))).toEqual(
      JSON.parse('{"__proto__":{"type":"order.placed.v1"},"id":"evt-1","data":{"orderId":"1"}}'),
    );
  });
});

describe('reading', () => {
  const read = (mapping: Mapping, body: unknown, headers: Record<string, string> = {}) =>
    format(mapping).read({ body, headers });

  it('reads a body envelope and leaves unmapped values to the transport', () => {
    const fields = read(
      { type: { body: 'eventName' }, id: { body: 'messageId' }, payload: { body: 'data' } },
      { eventName: 'order.placed.v1', messageId: 42, data: null },
      { origin: 'legacy' },
    );
    expect(fields).toEqual({
      id: '42',
      type: 'order.placed.v1',
      payload: null,
      headers: { origin: 'legacy' },
      correlationId: null,
      causationId: null,
    });
  });

  it('reads header fields and drops them from the event headers', () => {
    const fields = read(
      {
        type: { header: 'event-type' },
        id: 'transport-id',
        payload: 'whole-body',
        key: { header: 'key' },
      },
      [1, 2],
      { 'event-type': 'a.b', key: 'k', origin: 'legacy' },
    );
    expect(fields).toEqual({
      type: 'a.b',
      payload: [1, 2],
      key: 'k',
      headers: { origin: 'legacy' },
      correlationId: null,
      causationId: null,
    });
  });

  it('reads the event headers of a body path, strings only, and none from a non-object', () => {
    const mapping: Mapping = {
      type: { fixed: 'a.b' },
      id: { body: 'id' },
      payload: { body: 'data' },
      headers: { body: 'meta' },
    };
    expect(read(mapping, { id: 'i', data: 1, meta: { a: 'x', b: 2 } }, { c: 'y' }).headers).toEqual(
      { a: 'x' },
    );
    expect(read(mapping, { id: 'i', data: 1, meta: 'nope' }).headers).toEqual({});
    expect(read(mapping, { id: 'i', data: 1 }).headers).toEqual({});
  });

  it.each<[string, unknown, Record<string, string>, RegExp]>([
    ['a body that is not an object', [1], {}, /body must be a JSON object/],
    ['a missing type', { messageId: 'm', data: 1 }, {}, /no type at 'eventName'/],
    ['an empty type', { eventName: '', messageId: 'm', data: 1 }, {}, /no type at 'eventName'/],
    ['a null id', { eventName: 'a.b', messageId: null, data: 1 }, {}, /no id at 'messageId'/],
    [
      'an invalid type',
      { eventName: 'not a type', messageId: 'm', data: 1 },
      {},
      /not a valid event type/,
    ],
    [
      'a type that is no string',
      { eventName: { name: 'a.b' }, messageId: 'm', data: 1 },
      {},
      /type at 'eventName' must be a string/,
    ],
    [
      'an id beyond 2^53',
      { eventName: 'a.b', messageId: 2 ** 60, data: 1 },
      {},
      /id at 'messageId' must be a string/,
    ],
    ['a missing payload', { eventName: 'a.b', messageId: 'm' }, {}, /no payload at 'data'/],
    [
      'a time that is not RFC 3339',
      { eventName: 'a.b', messageId: 'm', data: 1, at: 'yesterday' },
      {},
      /not an RFC 3339 timestamp: 'yesterday'/,
    ],
    [
      'a time that does not exist',
      { eventName: 'a.b', messageId: 'm', data: 1, at: '2026-13-45T00:00:00Z' },
      {},
      /not an RFC 3339/,
    ],
  ])('refuses %s as invalid', (_label, body, headers, message) => {
    const mapping: Mapping = {
      type: { body: 'eventName' },
      id: { body: 'messageId' },
      payload: { body: 'data' },
      occurredAt: { body: 'at' },
    };
    const attempt = (): unknown => read(mapping, body, headers);
    expect(attempt).toThrow(Invalid);
    expect(attempt).toThrow(message);
  });

  it('names a missing header field by its header', () => {
    expect(() =>
      read({ type: { header: 'event-type' }, id: 'transport-id', payload: 'whole-body' }, {}),
    ).toThrow("The message has no type at header 'event-type'");
  });

  it('reads through arrays by index and treats a path through a scalar as absent', () => {
    const mapping: Mapping = {
      type: { body: 'messageType.0' },
      id: { body: 'messageId' },
      payload: { body: 'message' },
      key: { body: 'tag.x' },
    };
    expect(
      read(mapping, { messageType: ['a.b'], messageId: 'm', message: {}, tag: 'scalar' }),
    ).not.toHaveProperty('key');
  });
});

describe('round trip', () => {
  const header = fc.stringMatching(/^[a-z][a-z0-9-]{0,15}$/);
  const text = fc.string({ minLength: 1, maxLength: 20 });
  const envelopes = fc.record({
    id: text,
    type: fc.stringMatching(/^[a-z][a-z0-9._:/-]{0,30}$/),
    source: text,
    key: fc.option(text, { nil: null }),
    payload: fc.jsonValue(),
    headers: fc.dictionary(header, fc.string(), { maxKeys: 4 }),
    correlationId: fc.option(text, { nil: null }),
    causationId: fc.option(text, { nil: null }),
    occurredAt: fc.date({
      min: new Date('0000-01-01T00:00:00Z'),
      max: new Date('9999-12-31T23:59:59Z'),
      noInvalidDate: true,
    }),
  });

  it('reads back every value a complete body envelope wrote', () => {
    const body = format(BODY_ENVELOPE);
    fc.assert(
      fc.property(envelopes, (event) => {
        const wire = JSON.parse(JSON.stringify(body.write(event))) as {
          body: unknown;
          headers: Record<string, string>;
        };
        const fields = body.read(wire);
        expect({ ...fields, key: fields.key ?? null, occurredAt: fields.occurredAt }).toEqual({
          ...event,
          payload: JSON.parse(JSON.stringify(event.payload)) as unknown,
        });
      }),
    );
  });
});

describe('writeStructuredCloudEvent', () => {
  it('writes the attributes, then the serialized data', () => {
    expect(
      writeStructuredCloudEvent(envelope({ key: null, causationId: null }), {
        time: 'T',
        data: '{"a":1}',
      }),
    ).toBe(
      '{"specversion":"1.0","id":"evt-1","type":"order.placed.v1","source":"orders","time":"T","datacontenttype":"application/json","correlationid":"flow-1","data":{"a":1}}',
    );
  });
});
