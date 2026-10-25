import { describe, expect, expectTypeOf, it } from 'vitest';
import { ConfigurationError, type EventEnvelope } from '@relaybus/core';
import {
  createJsonEnvelopeFormat,
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
    type: 'order_created',
    source: 'billing-api',
    key: 'order-1',
    payload: { orderId: 1 },
    headers: {},
    correlationId: 'req-1',
    causationId: null,
    occurredAt: new Date('2026-10-25T14:00:00.123Z'),
    ...overrides,
  };
}

const BASE: Mapping = {
  type: { body: 'type' },
  id: 'transport-id',
  payload: { body: 'data' },
};

describe('jsonEnvelope time formats', () => {
  it('writes epoch milliseconds as a number in the body and reads them back', () => {
    const json = format({ ...BASE, occurredAt: { body: 'timestamp', format: 'epoch-millis' } });
    const message = json.write(envelope());
    expect(message.body).toMatchObject({ timestamp: 1792936800123 });
    expect(json.read(message).occurredAt).toEqual(new Date('2026-10-25T14:00:00.123Z'));
  });

  it('writes epoch seconds, truncated, and reads them back', () => {
    const json = format({ ...BASE, occurredAt: { body: 'meta.at', format: 'epoch-seconds' } });
    const message = json.write(envelope());
    expect(message.body).toMatchObject({ meta: { at: 1792936800 } });
    expect(json.read(message).occurredAt).toEqual(new Date('2026-10-25T14:00:00.000Z'));
  });

  it('writes an epoch in a header as decimal text and reads the text back', () => {
    const json = format({ ...BASE, occurredAt: { header: 'x-time', format: 'epoch-millis' } });
    const message = json.write(envelope());
    expect(message.headers).toMatchObject({ 'x-time': '1792936800123' });
    expect(json.read(message).occurredAt).toEqual(new Date('2026-10-25T14:00:00.123Z'));
  });

  it("keeps RFC 3339 as the default and as 'rfc3339'", () => {
    for (const occurredAt of [{ body: 'at' }, { body: 'at', format: 'rfc3339' as const }]) {
      const json = format({ ...BASE, occurredAt });
      expect(json.write(envelope()).body).toMatchObject({ at: '2026-10-25T14:00:00.123Z' });
    }
  });

  it('refuses an epoch that is not a whole number', () => {
    const json = format({ ...BASE, occurredAt: { body: 'timestamp', format: 'epoch-millis' } });
    for (const timestamp of ['soon', 1.5, '12a', -1]) {
      expect(() => json.read({ body: { type: 'a.b', data: {}, timestamp }, headers: {} })).toThrow(
        Invalid,
      );
    }
  });

  it('refuses an unknown format', () => {
    expect(() =>
      format({ ...BASE, occurredAt: { body: 'at', format: 'unix' as 'epoch-millis' } }),
    ).toThrow(ConfigurationError);
  });
});

describe('jsonEnvelope extra fields', () => {
  const EXTRA: Mapping = {
    ...BASE,
    extra: {
      body: {
        provider: 'billing-api',
        'meta.version': 2,
        createdDate: (event) => event.occurredAt.toISOString(),
      },
      headers: { 'x-origin': 'relaybus', 'x-key': (event) => event.key ?? '' },
    },
  };

  it('writes constant and derived values in the body and the headers', () => {
    const message = format(EXTRA).write(envelope({ headers: { traceparent: 'tp' } }));
    expect(message.body).toEqual({
      type: 'order_created',
      data: { orderId: 1 },
      provider: 'billing-api',
      meta: { version: 2 },
      createdDate: '2026-10-25T14:00:00.123Z',
    });
    expect(message.headers).toEqual({
      traceparent: 'tp',
      'x-origin': 'relaybus',
      'x-key': 'order-1',
    });
  });

  it('never reads extra values back, and keeps extra headers out of the event headers', () => {
    const json = format(EXTRA);
    const fields = json.read(json.write(envelope({ headers: { traceparent: 'tp' } })));
    expect(fields.headers).toEqual({ traceparent: 'tp' });
    expect(fields).not.toHaveProperty('provider');
  });

  it('refuses an event header that an extra header writes', () => {
    expect(() => format(EXTRA).write(envelope({ headers: { 'x-origin': 'mine' } }))).toThrow(
      Unencodable,
    );
  });

  it('refuses extra values that overlap mapped locations', () => {
    expect(() => format({ ...BASE, extra: { body: { 'data.x': 1 } } })).toThrow(ConfigurationError);
    expect(() =>
      format({ ...BASE, id: { header: 'x-id' }, extra: { headers: { 'x-id': 'a' } } }),
    ).toThrow(ConfigurationError);
    expect(() => format({ ...BASE, extra: { body: { '': 1 } } })).toThrow(ConfigurationError);
  });

  it('allows extra headers but no extra body values on a payload-only mapping', () => {
    const json = format({
      type: { header: 'event-type' },
      id: 'transport-id',
      payload: 'whole-body',
      extra: { headers: { 'x-origin': 'relaybus' } },
    });
    expect(json.write(envelope()).headers).toMatchObject({ 'x-origin': 'relaybus' });
    expectTypeOf({
      type: { header: 'event-type' },
      id: 'transport-id',
      payload: 'whole-body',
      extra: { body: { a: 1 } },
    } as const).not.toExtend<Mapping>();
    expect(() =>
      format({
        type: { header: 'event-type' },
        id: 'transport-id',
        payload: 'whole-body',
        extra: { body: { a: 1 } },
      } as unknown as Mapping),
    ).toThrow(ConfigurationError);
  });
});

describe('jsonEnvelope body order', () => {
  it('writes the listed top-level fields first, in the listed order, then the others', () => {
    const json = format({
      ...BASE,
      correlationId: { body: 'requestId' },
      occurredAt: { body: 'timestamp', format: 'epoch-millis' },
      extra: { body: { provider: 'billing-api' } },
      bodyOrder: ['requestId', 'provider', 'timestamp', 'missing', 'type'],
    });
    expect(JSON.stringify(json.write(envelope()).body)).toBe(
      '{"requestId":"req-1","provider":"billing-api","timestamp":1792936800123,"type":"order_created","data":{"orderId":1}}',
    );
  });

  it('refuses a body order on a payload-only mapping', () => {
    expect(() =>
      format({
        type: { header: 'event-type' },
        id: 'transport-id',
        payload: 'whole-body',
        bodyOrder: ['a'],
      } as unknown as Mapping),
    ).toThrow(ConfigurationError);
  });
});

describe('jsonEnvelope id when missing', () => {
  const json = format({
    ...BASE,
    id: { body: 'id', whenMissing: 'transport-id' },
  });

  it('writes the id at its location and reads it back', () => {
    const message = json.write(envelope());
    expect(message.body).toMatchObject({ id: 'evt-1' });
    expect(json.read(message).id).toBe('evt-1');
  });

  it("leaves the id to the transport's own id when the message has none", () => {
    expect(json.read({ body: { type: 'a.b', data: {} }, headers: {} })).not.toHaveProperty('id');
  });

  it('refuses a fallback that is not a transport id', () => {
    expect(() =>
      format({ ...BASE, id: { body: 'id', whenMissing: 'other' as 'transport-id' } }),
    ).toThrow(ConfigurationError);
  });
});

describe("jsonEnvelope headers as 'top-level-fields'", () => {
  const json = format({
    ...BASE,
    correlationId: { body: 'requestId' },
    headers: 'top-level-fields',
    extra: { body: { provider: 'billing-api' } },
  });

  it('spreads the event headers over the top level of the body', () => {
    const message = json.write(envelope({ headers: { tenant: 'tr', channel: 'web' } }));
    expect(message.body).toEqual({
      type: 'order_created',
      requestId: 'req-1',
      data: { orderId: 1 },
      tenant: 'tr',
      channel: 'web',
      provider: 'billing-api',
    });
    expect(message.headers).toEqual({});
  });

  it('reads every unmapped top-level text field back as an event header', () => {
    const fields = json.read({
      body: {
        type: 'order_created',
        data: {},
        provider: 'billing-api',
        requestId: 'req-1',
        tenant: 'tr',
        retries: 3,
        flags: { a: true },
      },
      headers: { 'x-ignored': '1' },
    });
    expect(fields.headers).toEqual({ tenant: 'tr' });
  });

  it('refuses an event header named like a mapped or extra field', () => {
    expect(() => json.write(envelope({ headers: { requestId: 'x' } }))).toThrow(Unencodable);
    expect(() => json.write(envelope({ headers: { provider: 'x' } }))).toThrow(Unencodable);
    expect(() => json.write(envelope({ headers: { data: 'x' } }))).toThrow(Unencodable);
  });

  it('is not allowed on a payload-only mapping', () => {
    expect(() =>
      format({
        type: { header: 'event-type' },
        id: 'transport-id',
        payload: 'whole-body',
        headers: 'top-level-fields',
      } as unknown as Mapping),
    ).toThrow(ConfigurationError);
  });
});
