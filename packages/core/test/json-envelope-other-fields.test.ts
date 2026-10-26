import { describe, expect, it } from 'vitest';
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
    correlationId: null,
    causationId: null,
    occurredAt: new Date('2026-10-25T14:00:00.123Z'),
    ...overrides,
  };
}

const FLAT: Mapping = {
  type: { body: 'type' },
  id: 'transport-id',
  payload: { body: 'data' },
  headers: 'top-level-fields',
  extra: { body: { provider: 'billing-api' } },
  otherFields: { payloadField: 'wire' },
};

const OTHER_FIELDS = {
  attempt: 2,
  ratio: 1e21,
  active: false,
  cleared: null,
  meta: { zz: 1, a: [{ y: 2, b: 1 }] },
  tags: ['x', 1, true, null, {}],
  empty: {},
};

describe('jsonEnvelope otherFields', () => {
  it('keeps unused top-level fields of every JSON type, in body order, under the payload field', () => {
    const fields = format(FLAT).read({
      body: { type: 'order_created', data: { orderId: 1 }, tenant: 'tr', ...OTHER_FIELDS },
      headers: {},
    });
    expect(fields.headers).toEqual({ tenant: 'tr' });
    expect(JSON.stringify(fields.payload)).toBe(JSON.stringify({ orderId: 1, wire: OTHER_FIELDS }));
  });

  it('writes them back over the top level, so the round trip is byte for byte', () => {
    const json = format(FLAT);
    const body = {
      type: 'order_created',
      data: { orderId: 1 },
      provider: 'billing-api',
      tenant: 'tr',
      ...OTHER_FIELDS,
    };
    const read = json.read({ body, headers: {} });
    const written = json.write(envelope({ payload: read.payload, headers: read.headers }));
    expect(JSON.stringify(written.body)).toBe(JSON.stringify(body));
    expect(json.read(written)).toEqual(read);
  });

  it('keeps text fields too when event headers do not live at the top level', () => {
    const json = format({ ...FLAT, headers: 'message-headers' });
    const read = json.read({
      body: { type: 'order_created', data: { orderId: 1 }, note: 'kept', attempt: 2 },
      headers: { tenant: 'tr' },
    });
    expect(read.payload).toEqual({ orderId: 1, wire: { note: 'kept', attempt: 2 } });
    expect(json.write(envelope({ payload: read.payload })).body).toEqual({
      type: 'order_created',
      data: { orderId: 1 },
      provider: 'billing-api',
      note: 'kept',
      attempt: 2,
    });
  });

  it('leaves the payload as it is when there is nothing to keep, and writes one without the field as it is', () => {
    const json = format(FLAT);
    const read = json.read({ body: { type: 'order_created', data: [1, 2] }, headers: {} });
    expect(read.payload).toEqual([1, 2]);
    expect(json.write(envelope({ payload: 'text' })).body).toEqual({
      type: 'order_created',
      data: 'text',
      provider: 'billing-api',
    });
  });

  it('gives an object payload the field even when there is nothing to keep', () => {
    const json = format(FLAT);
    const read = json.read({ body: { type: 'order_created', data: { orderId: 1 } }, headers: {} });
    expect(read.payload).toEqual({ orderId: 1, wire: {} });
    expect(json.write(envelope({ payload: read.payload })).body).toEqual({
      type: 'order_created',
      data: { orderId: 1 },
      provider: 'billing-api',
    });
  });

  it('refuses a message whose payload cannot hold the fields', () => {
    const json = format(FLAT);
    expect(() =>
      json.read({ body: { type: 'order_created', data: [1], n: 1 }, headers: {} }),
    ).toThrow(Invalid);
    expect(() =>
      json.read({ body: { type: 'order_created', data: { wire: 1 }, n: 1 }, headers: {} }),
    ).toThrow(Invalid);
    expect(() =>
      json.read({ body: { type: 'order_created', data: { wire: { a: 1 } } }, headers: {} }),
    ).toThrow(Invalid);
  });

  it('refuses to write kept fields that are not an object, that a mapping uses, or that would read back as headers', () => {
    const json = format(FLAT);
    const write =
      (wire: unknown, headers: Record<string, string> = {}) =>
      () =>
        json.write(envelope({ payload: { orderId: 1, wire }, headers }));
    expect(write([1])).toThrow(Unencodable);
    expect(write({ provider: 1 })).toThrow(Unencodable);
    expect(write({ data: 1 })).toThrow(Unencodable);
    expect(write({ note: 'text' })).toThrow(Unencodable);
    expect(write({ tenant: 1 }, { tenant: 'tr' })).toThrow(Unencodable);
  });

  it('needs a body payload and a payload field name', () => {
    expect(() =>
      format({
        type: { header: 'event-type' },
        id: 'transport-id',
        payload: 'whole-body',
        // @ts-expect-error a whole-body payload has no top-level fields to keep
        otherFields: { payloadField: 'wire' },
      }),
    ).toThrow(ConfigurationError);
    expect(() => format({ ...FLAT, otherFields: { payloadField: '' } })).toThrow(
      ConfigurationError,
    );
    expect(() => format({ ...FLAT, otherFields: 'wire' } as unknown as Mapping)).toThrow(
      ConfigurationError,
    );
  });
});
