import { InvalidEnvelopeError, toErrorInfo } from './errors.js';
import { assertEventType, type EventEnvelope } from './event.js';

/**
 * Validates an event received from a broker and fills fields that JSON-based
 * wire formats commonly drop (`undefined` key, headers, correlation ids).
 */
export function normalizeEnvelope(envelope: EventEnvelope): EventEnvelope {
  if (typeof envelope.id !== 'string' || envelope.id.length === 0) {
    throw new InvalidEnvelopeError('Received event has no id');
  }
  if (typeof envelope.type !== 'string') {
    throw new InvalidEnvelopeError(`Received event ${envelope.id} has no type`);
  }
  try {
    assertEventType(envelope.type);
  } catch (error) {
    throw new InvalidEnvelopeError(toErrorInfo(error).message);
  }
  const occurredAt = toDate(envelope.occurredAt);
  if (occurredAt === undefined) {
    throw new InvalidEnvelopeError(`Received event ${envelope.id} has an invalid occurredAt`);
  }
  return {
    id: envelope.id,
    type: envelope.type,
    source: typeof envelope.source === 'string' ? envelope.source : 'unknown',
    key: optionalText(envelope.key),
    payload: envelope.payload,
    headers: { ...envelope.headers },
    correlationId: optionalText(envelope.correlationId),
    causationId: optionalText(envelope.causationId),
    occurredAt,
  };
}

function toDate(value: Date | string): Date | undefined {
  const date = typeof value === 'string' ? new Date(value) : value;
  return date instanceof Date && !Number.isNaN(date.getTime()) ? date : undefined;
}

function optionalText(value: string | null | undefined): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}
