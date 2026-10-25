import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import {
  assertPositiveInteger,
  createCloudEventReaders,
  fromHeader,
  fromPayloadField,
  isJsonObject,
  mediaTypeOf,
} from '@relaybus/core/internal';
import * as kafka from '@relaybus/kafka';
import * as rabbitmq from '@relaybus/rabbitmq';

describe('assertPositiveInteger', () => {
  it('returns a positive integer unchanged', () => {
    expect(assertPositiveInteger(1, 'limit')).toBe(1);
    expect(assertPositiveInteger(250, 'limit')).toBe(250);
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects %s with the shared message',
    (value) => {
      expect(() => assertPositiveInteger(value, 'purgeBatchSize')).toThrow(
        new ConfigurationError(`purgeBatchSize must be a positive integer, received ${value}`),
      );
      expect(() => assertPositiveInteger(value, 'purgeBatchSize')).toThrow(ConfigurationError);
    },
  );
});

describe('field extractors', () => {
  it('are one implementation shared by the Kafka and RabbitMQ packages', () => {
    expect(kafka.fromHeader).toBe(fromHeader);
    expect(rabbitmq.fromHeader).toBe(fromHeader);
    expect(kafka.fromPayloadField).toBe(fromPayloadField);
    expect(rabbitmq.fromPayloadField).toBe(fromPayloadField);
  });
});

describe('CloudEvent readers', () => {
  class Invalid extends Error {}
  const readers = createCloudEventReaders((reason) => new Invalid(reason));

  it('reads optional and required string attributes', () => {
    expect(readers.optionalAttribute({}, 'subject')).toBeUndefined();
    expect(readers.optionalAttribute({ subject: 's' }, 'subject')).toBe('s');
    expect(readers.requiredAttribute({ id: 'e-1' }, 'id')).toBe('e-1');
  });

  it('raises the error of the calling package for bad attributes', () => {
    expect(() => readers.optionalAttribute({ id: 1 }, 'id')).toThrow(
      new Invalid("Attribute 'id' must be a string"),
    );
    expect(() => readers.requiredAttribute({ id: '' }, 'id')).toThrow(
      new Invalid("Attribute 'id' is missing or empty"),
    );
    expect(() => readers.requiredAttribute({}, 'id')).toThrow(Invalid);
    expect(() => readers.assertValidType('Not A Type!', 'type')).toThrow(
      /^type is not a valid event type: /,
    );
    expect(() => readers.assertValidType('Not A Type!', 'type')).toThrow(Invalid);
    expect(() => readers.assertJsonDataContentType('text/plain')).toThrow(
      new Invalid("datacontenttype must be JSON, received 'text/plain'"),
    );
  });

  it('accepts JSON content types and valid types', () => {
    for (const type of [
      undefined,
      'application/json',
      'text/json; charset=utf-8',
      'application/cloudevents+json',
    ]) {
      expect(() => readers.assertJsonDataContentType(type)).not.toThrow();
    }
    expect(() => readers.assertValidType('order.placed.v1', 'type')).not.toThrow();
  });

  it('parses media types and JSON objects', () => {
    expect(mediaTypeOf('Application/JSON; charset=utf-8')).toBe('application/json');
    expect(mediaTypeOf('')).toBe('');
    expect(mediaTypeOf(undefined)).toBeUndefined();
    expect(mediaTypeOf(42)).toBeUndefined();
    expect(isJsonObject({})).toBe(true);
    expect(isJsonObject([])).toBe(false);
    expect(isJsonObject(null)).toBe(false);
    expect(isJsonObject('x')).toBe(false);
  });
});
