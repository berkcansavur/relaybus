import { ConfigurationError, type EventHeaders } from '@relaybus/core';

/**
 * Reads one header of an incoming message, for the `type`, `id`, `key` or
 * `source` option of {@link plainJsonDecoder}. Header names are compared
 * exactly (AMQP header names are case-sensitive). Returns undefined when the
 * header is absent, so the decoder falls back to its default.
 *
 * ```ts
 * plainJsonDecoder({ type: fromHeader('event-type'), id: fromHeader('message-id') })
 * ```
 */
export function fromHeader(
  name: string,
): (message: { readonly headers: EventHeaders }) => string | undefined {
  if (name.length === 0) {
    throw new ConfigurationError('fromHeader needs a header name');
  }
  return (message) => (Object.hasOwn(message.headers, name) ? message.headers[name] : undefined);
}

/**
 * Reads a field of the JSON payload by a dotted path (`'meta.eventType'`), for
 * the `type`, `id`, `key` or `source` option of {@link plainJsonDecoder}. A
 * string is returned as is and a safe integer as its decimal text (numeric ids
 * are common). Anything else returns undefined so the decoder falls back to its
 * default: a missing field, and also a fraction or an integer beyond 2^53, which
 * JSON parsing may already have rounded, so two different ids could collide.
 *
 * ```ts
 * plainJsonDecoder({ type: fromPayloadField('eventType'), key: fromPayloadField('order.id') })
 * ```
 */
export function fromPayloadField(
  path: string,
): (message: { readonly payload: unknown }) => string | undefined {
  const segments = path.split('.');
  if (segments.some((segment) => segment.length === 0)) {
    throw new ConfigurationError(
      `fromPayloadField needs a dotted path of non-empty names, received '${path}'`,
    );
  }
  return (message) => {
    let value = message.payload;
    for (const segment of segments) {
      if (typeof value !== 'object' || value === null || !Object.hasOwn(value, segment)) {
        return undefined;
      }
      value = (value as Record<string, unknown>)[segment];
    }
    if (typeof value === 'string') {
      return value;
    }
    return Number.isSafeInteger(value) ? String(value) : undefined;
  };
}
