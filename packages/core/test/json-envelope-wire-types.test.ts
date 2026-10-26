import { describe, expect, it } from 'vitest';
import { ConfigurationError, assertEventType, type EventEnvelope } from '@relaybus/core';
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

function envelope(type: string): EventEnvelope {
  return {
    id: 'evt-1',
    type,
    source: 'dispatch-api',
    key: null,
    payload: { transferId: 1 },
    headers: {},
    correlationId: null,
    causationId: null,
    occurredAt: new Date('2026-10-25T14:00:00.123Z'),
  };
}

const MAPPING: Mapping = {
  type: { body: 'type' },
  id: 'transport-id',
  payload: { body: 'data' },
  wireTypes: {
    'transfer.auto-dispatched.v1': 'Transfer Auto Dispatched',
    'transfer.cancelled.v1': 'transfer_cancelled',
  },
};

describe('jsonEnvelope wireTypes', () => {
  it('accepts snake_case event types as they are', () => {
    expect(() => assertEventType('transfer_auto_dispatched')).not.toThrow();
    const json = format({ type: { body: 'type' }, id: 'transport-id', payload: { body: 'data' } });
    const message = json.write(envelope('transfer_auto_dispatched'));
    expect(message.body).toEqual({ type: 'transfer_auto_dispatched', data: { transferId: 1 } });
    expect(json.read(message).type).toBe('transfer_auto_dispatched');
  });

  it('writes listed types under their wire names and reads them back, on one route', () => {
    const json = format(MAPPING);
    for (const [type, wire] of Object.entries(MAPPING.wireTypes!)) {
      const message = json.write(envelope(type));
      expect(message.body).toEqual({ type: wire, data: { transferId: 1 } });
      expect(json.read(message).type).toBe(type);
    }
  });

  it('lets other types travel as themselves', () => {
    const json = format(MAPPING);
    const message = json.write(envelope('transfer.created.v1'));
    expect(message.body).toMatchObject({ type: 'transfer.created.v1' });
    expect(json.read(message).type).toBe('transfer.created.v1');
  });

  it('works with the type in a header', () => {
    const json = format({ ...MAPPING, type: { header: 'event-type' } });
    const message = json.write(envelope('transfer.auto-dispatched.v1'));
    expect(message.headers['event-type']).toBe('Transfer Auto Dispatched');
    expect(json.read(message).type).toBe('transfer.auto-dispatched.v1');
  });

  it('refuses a wire value that is neither listed nor an event type', () => {
    expect(() =>
      format(MAPPING).read({ body: { type: 'Transfer Created', data: {} }, headers: {} }),
    ).toThrow(Invalid);
  });

  it('refuses to write an unlisted type that is the wire name of another type', () => {
    expect(() => format(MAPPING).write(envelope('transfer_cancelled'))).toThrow(Unencodable);
  });

  it('refuses invalid wire type maps', () => {
    const withTypes = (wireTypes: unknown) => () =>
      format({ ...MAPPING, wireTypes } as unknown as Mapping);
    expect(withTypes('x')).toThrow(ConfigurationError);
    expect(withTypes({ 'not valid!': 'x' })).toThrow(ConfigurationError);
    expect(withTypes({ 'a.v1': '' })).toThrow(ConfigurationError);
    expect(withTypes({ 'a.v1': 1 })).toThrow(ConfigurationError);
    expect(withTypes({ 'a.v1': 'same', 'b.v1': 'same' })).toThrow(ConfigurationError);
    expect(() => format({ ...MAPPING, type: { fixed: 'transfer.cancelled.v1' } })).toThrow(
      ConfigurationError,
    );
  });
});
