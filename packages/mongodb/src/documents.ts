import {
  EventValidationError,
  type DeliveryRecord,
  type DeliveryStatus,
  type ErrorInfo,
  type EventEnvelope,
  type NewDelivery,
} from '@relaybus/core';

/** Where a delivery sits relative to the claim index. See {@link DeliveryDocument.lane}. */
export type DeliveryLane = 'ready' | 'parked';

/** Shape of a document in the events collection. `_id` is the event id. */
export interface EventDocument {
  readonly _id: string;
  readonly type: string;
  readonly source: string;
  readonly key: string | null;
  /** Position among the events of the same key, in commit order. Null for unkeyed events. */
  readonly sequence: number | null;
  /** JSON-normalized payload. Absent when the published payload was `undefined`. */
  readonly payload?: unknown;
  readonly headers: Readonly<Record<string, string>>;
  readonly correlationId: string | null;
  readonly causationId: string | null;
  readonly occurredAt: Date;
  /** Incremented when deliveries are added to an already stored event, so a concurrent purge conflicts with it. */
  readonly revision: number;
}

/**
 * Shape of a document in the deliveries collection.
 *
 * `dueAt` exists exactly while the delivery is `pending` (its `availableAt`) or
 * `processing` (its `leaseExpiresAt`). The ordering index is partial on it, so
 * it only ever contains unfinished work.
 *
 * `lane` exists on the same deliveries. A `ready` delivery is in the claim
 * index. A `parked` delivery is a pending, keyed delivery that waited behind an
 * unfinished earlier delivery of its handler and key; the claim index leaves it
 * out so a long queue behind one slow delivery costs nothing per claim. Claims
 * move the lowest parked delivery of a key back to `ready` once nothing
 * unfinished precedes it.
 */
export interface DeliveryDocument {
  readonly _id: string;
  readonly eventId: string;
  readonly eventType: string;
  readonly handler: string;
  /** Ordering key. Absent for unkeyed events. */
  readonly key?: string;
  /** Sequence of the event within its key. Absent for unkeyed events. */
  readonly sequence?: number;
  readonly status: DeliveryStatus;
  readonly lane?: DeliveryLane;
  readonly attempts: number;
  readonly availableAt: Date;
  readonly dueAt?: Date;
  readonly leaseOwner?: string;
  readonly leaseToken?: string;
  readonly leaseExpiresAt?: Date;
  readonly lastError?: ErrorInfo;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly completedAt?: Date;
}

/** Shape of a document in the key sequences collection. `_id` is the ordering key. */
export interface KeySequenceDocument {
  readonly _id: string;
  readonly value: number;
}

/**
 * Builds the stored form of an event. Payload and headers go through JSON so
 * that values JSON cannot represent (Dates, `undefined`, `NaN`) end up exactly
 * as in every other store.
 *
 * @throws EventValidationError when payload or headers contain a lone UTF-16
 * surrogate. BSON strings are UTF-8, so MongoDB would silently store U+FFFD
 * instead and the consumer would read different data than was published.
 */
export function toEventDocument(event: EventEnvelope, sequence: number | null): EventDocument {
  const payloadJson = serializeWellFormed(event.payload, 'payload');
  const headersJson = serializeWellFormed(event.headers, 'headers')!;
  return {
    _id: event.id,
    type: event.type,
    source: event.source,
    key: event.key,
    sequence,
    ...(payloadJson === undefined ? {} : { payload: JSON.parse(payloadJson) as unknown }),
    headers: JSON.parse(headersJson) as Record<string, string>,
    correlationId: event.correlationId,
    causationId: event.causationId,
    occurredAt: event.occurredAt,
    revision: 0,
  };
}

/**
 * JSON.stringify writes a lone surrogate as a `\udXXX` escape and a valid pair as
 * the character itself, so an escape in that range is exactly a lone surrogate.
 * The lookbehind and the pair group skip escaped backslashes (`\\ud800` is text).
 */
const LONE_SURROGATE_ESCAPE = /(?<!\\)(?:\\\\)*\\ud[89a-f][0-9a-f]{2}/i;

function serializeWellFormed(value: unknown, field: string): string | undefined {
  const json = JSON.stringify(value);
  if (json !== undefined && LONE_SURROGATE_ESCAPE.test(json)) {
    throw new EventValidationError(`The event ${field} contains a lone UTF-16 surrogate`, [
      {
        message:
          'A lone surrogate cannot be stored as UTF-8 and would be replaced by U+FFFD. Remove it or encode the text first (for example with String.prototype.toWellFormed)',
        path: [field],
      },
    ]);
  }
  return json;
}

export function toEnvelope(document: EventDocument): EventEnvelope {
  return {
    id: document._id,
    type: document.type,
    source: document.source,
    key: document.key,
    payload: document.payload,
    headers: document.headers,
    correlationId: document.correlationId,
    causationId: document.causationId,
    occurredAt: document.occurredAt,
  };
}

export function toDeliveryDocument(
  delivery: NewDelivery,
  event: Pick<EventEnvelope, 'id' | 'type'>,
  ordering: { readonly key: string | null; readonly sequence: number | null },
  now: Date,
): DeliveryDocument {
  return {
    _id: delivery.id,
    eventId: event.id,
    eventType: event.type,
    handler: delivery.handler,
    ...(ordering.key === null || ordering.sequence === null
      ? {}
      : { key: ordering.key, sequence: ordering.sequence }),
    status: 'pending',
    lane: 'ready',
    attempts: 0,
    availableAt: delivery.availableAt,
    dueAt: delivery.availableAt,
    createdAt: now,
    updatedAt: now,
  };
}

export function toDeliveryRecord(document: DeliveryDocument, event: EventEnvelope): DeliveryRecord {
  return {
    id: document._id,
    handler: document.handler,
    status: document.status,
    attempts: document.attempts,
    availableAt: document.availableAt,
    leaseOwner: document.leaseOwner ?? null,
    leaseExpiresAt: document.leaseExpiresAt ?? null,
    lastError: document.lastError === undefined ? null : toErrorInfo(document.lastError),
    createdAt: document.createdAt,
    updatedAt: document.updatedAt,
    completedAt: document.completedAt ?? null,
    event,
  };
}

/** Copies an error without `undefined` members, which BSON would store as null. */
export function toErrorInfo(error: ErrorInfo): ErrorInfo {
  return error.stack === undefined
    ? { name: error.name, message: error.message }
    : { name: error.name, message: error.message, stack: error.stack };
}
