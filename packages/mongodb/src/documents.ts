import {
  EventValidationError,
  type DeliveryRecord,
  type ErrorInfo,
  type EventEnvelope,
  type NewDelivery,
} from '@relaybus/core';
import { laneOf } from './lanes.js';

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
  /**
   * Handlers that ever got a delivery of this event, completed ones included.
   * Completed deliveries are deleted, so this list is what keeps a redelivered
   * or republished event from running a completed handler again.
   */
  readonly handlers: readonly string[];
  /** When the event was appended (the caller's clock). Orphaned events older than a purge cutoff are deleted. */
  readonly createdAt: Date;
  /**
   * Latest completion of one of its deliveries. An event nothing references
   * any more is kept as deduplication record until `completedBefore` passes it.
   */
  readonly completedAt?: Date;
}

/**
 * Status a delivery document can have. Running deliveries are stored as
 * `pending`: whether one runs is derived from its lane (see
 * {@link LaneDocument.inFlight}). `completed` documents only exist when they
 * were left by version 0.1.0, which kept completed deliveries.
 */
export type StoredDeliveryStatus = 'pending' | 'dead' | 'completed';

/**
 * Shape of a document in the deliveries collection.
 *
 * `dueAt` exists exactly while the delivery is `pending`, running or not, and
 * equals its `availableAt`. The ordering index is partial on it, so it only
 * ever contains unfinished work. A claim never writes the document.
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
  /** Lane the delivery belongs to, see {@link laneOf}. */
  readonly laneId: number;
  readonly status: StoredDeliveryStatus;
  readonly lane?: DeliveryLane;
  /** Attempts that ended: failed, or lost with their lane. A running attempt is not counted yet. */
  readonly attempts: number;
  /**
   * Claim generation: grows when a claim of the delivery ends (retry, release,
   * burial, lost lease). Absent means 0 (documents written before it existed).
   */
  readonly claims?: number;
  /**
   * Lane epoch of the last lost claim charged to the delivery. It only grows, so
   * a repeated or delayed charge of the same or an older lost claim is skipped.
   */
  readonly lostAt?: number;
  readonly availableAt: Date;
  readonly dueAt?: Date;
  readonly lastError?: ErrorInfo;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly completedAt?: Date;
}

/**
 * Shape of a document in the lanes collection, one per lane (`_id` is the lane
 * id). Whoever holds the lane under a live lease is the only claimer that sees
 * its pending deliveries.
 */
export interface LaneDocument {
  readonly _id: number;
  /** Fencing token. Grows every time the lane is taken, never when it is freed. */
  readonly epoch: number;
  readonly owner: string | null;
  /**
   * The store instance that holds the lane for `owner`. A process restarted with
   * the same owner name is another instance and does not adopt the lane.
   */
  readonly holder: string | null;
  readonly leaseExpiresAt: Date | null;
  /**
   * Ids of the deliveries the owner claimed. A delivery runs while its
   * document exists as `pending` and its id is listed here. Completed
   * deliveries stay listed (their documents are gone) until the next claim of
   * the lane prunes them, so a completion never writes the lane.
   */
  readonly inFlight: readonly string[];
  /**
   * Deliveries that were in flight when the lane was last taken over. Each of
   * them is charged one attempt before the new holder reads the lane, then the
   * list is cleared.
   */
  readonly lost: readonly LostDelivery[];
  /**
   * Grows with every claim recorded in the lane and every reconciliation of it.
   * A claim records only under the revision it read, so a record that reaches
   * the server after its claim failed and was reconciled changes nothing.
   * Absent means 0.
   */
  readonly revision?: number;
}

/** A delivery that was in flight when its lane was taken over, and the epoch it was claimed under. */
export interface LostDelivery {
  readonly id: string;
  readonly epoch: number;
}

/** Shape of a document in the completion history (`completionHistory: true`). `_id` is the delivery id. */
export interface CompletedDeliveryDocument {
  readonly _id: string;
  readonly eventId: string;
  readonly eventType: string;
  readonly handler: string;
  /** Attempts including the one that completed. */
  readonly attempts: number;
  readonly availableAt: Date;
  readonly createdAt: Date;
  readonly completedAt: Date;
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
export function toEventDocument(
  event: EventEnvelope,
  sequence: number | null,
  handlers: readonly string[],
  createdAt: Date,
): EventDocument {
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
    handlers,
    createdAt,
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

/** The envelope stored in an event document. */
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

/** A new pending delivery of `event`, placed in the lane of the event's key (or id). */
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
    laneId: laneOf(ordering.key, event.id),
    status: 'pending',
    lane: 'ready',
    attempts: 0,
    availableAt: delivery.availableAt,
    dueAt: delivery.availableAt,
    createdAt: now,
    updatedAt: now,
  };
}

/**
 * The record of a stored delivery. `runningIn` is the lane lease the delivery
 * runs under, when it runs: it is then reported as `processing`, with its
 * running attempt counted and the lease of its lane.
 */
export function toDeliveryRecord(
  document: DeliveryDocument,
  event: EventEnvelope,
  runningIn: LaneDocument | undefined,
): DeliveryRecord {
  return {
    id: document._id,
    handler: document.handler,
    status: runningIn === undefined ? document.status : 'processing',
    attempts: runningIn === undefined ? document.attempts : document.attempts + 1,
    availableAt: document.availableAt,
    leaseOwner: runningIn === undefined ? null : runningIn.owner,
    leaseExpiresAt: runningIn === undefined ? null : runningIn.leaseExpiresAt,
    lastError: document.lastError === undefined ? null : toErrorInfo(document.lastError),
    createdAt: document.createdAt,
    updatedAt: document.updatedAt,
    completedAt: document.completedAt ?? null,
    event,
  };
}

/** The record of a completed delivery kept in the completion history. */
export function toCompletedDeliveryRecord(
  document: CompletedDeliveryDocument,
  event: EventEnvelope,
): DeliveryRecord {
  return {
    id: document._id,
    handler: document.handler,
    status: 'completed',
    attempts: document.attempts,
    availableAt: document.availableAt,
    leaseOwner: null,
    leaseExpiresAt: null,
    lastError: null,
    createdAt: document.createdAt,
    updatedAt: document.completedAt,
    completedAt: document.completedAt,
    event,
  };
}

/** The completion history entry of a delivery completed at `completedAt`. */
export function toCompletedDeliveryDocument(
  document: DeliveryDocument,
  completedAt: Date,
): CompletedDeliveryDocument {
  return {
    _id: document._id,
    eventId: document.eventId,
    eventType: document.eventType,
    handler: document.handler,
    attempts: document.attempts + 1,
    availableAt: document.availableAt,
    createdAt: document.createdAt,
    completedAt,
  };
}

/** Copies an error without `undefined` members, which BSON would store as null. */
export function toErrorInfo(error: ErrorInfo): ErrorInfo {
  return error.stack === undefined
    ? { name: error.name, message: error.message }
    : { name: error.name, message: error.message, stack: error.stack };
}
