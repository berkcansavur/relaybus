import type { ErrorInfo } from '../errors.js';
import type { DeliveryQuery, DeliveryStatus, PurgeRequest } from '../event-store.js';
import type { EventEnvelope } from '../event.js';
import type { SettlementChange } from '../store-kit/lease-settlement.js';

// The rows of the in-memory store and the rules that read them, as pure functions.

export interface SerializedEnvelope extends Omit<EventEnvelope, 'occurredAt'> {
  readonly occurredAt: string;
}

export interface StoredEvent {
  readonly envelope: SerializedEnvelope;
  readonly sequence: number | null;
  readonly createdAt: Date;
  /**
   * Handlers that ever got a delivery of this event, completed ones included.
   * A repeated append adds deliveries only for handlers not listed, so a
   * completed (deleted) delivery is never created again.
   */
  readonly handlers: Set<string>;
  /** When a delivery of this event last completed. Retention of completed deliveries is measured from here, not from creation. */
  completedAt: Date | null;
}

export interface StoredDelivery {
  readonly id: string;
  readonly eventId: string;
  readonly handler: string;
  readonly key: string | null;
  readonly sequence: number | null;
  status: DeliveryStatus;
  attempts: number;
  availableAt: Date;
  leaseOwner: string | null;
  leaseToken: string | null;
  leaseExpiresAt: Date | null;
  lastError: ErrorInfo | null;
  readonly createdAt: Date;
  updatedAt: Date;
  completedAt: Date | null;
}

/** Applies a retry, a release or a burial to a running delivery and clears its lease. */
export function applyTransition(delivery: StoredDelivery, change: SettlementChange): void {
  if (change.kind === 'reschedule') {
    delivery.status = 'pending';
    delivery.availableAt = change.availableAt;
    delivery.lastError = change.error;
  } else if (change.kind === 'release') {
    delivery.status = 'pending';
    delivery.attempts -= 1;
    delivery.availableAt = change.now;
  } else {
    delivery.status = 'dead';
    delivery.lastError = change.error;
  }
  delivery.updatedAt = change.now;
  clearLease(delivery);
}

/** Whether `other`, another delivery of the same handler and key, holds `delivery` back. */
export function blocksKey(
  other: StoredDelivery,
  { delivery, now }: { readonly delivery: StoredDelivery; readonly now: Date },
): boolean {
  if (other === delivery || other.handler !== delivery.handler || other.key !== delivery.key) {
    return false;
  }
  const unfinishedPredecessor =
    other.sequence! < delivery.sequence! &&
    (other.status === 'pending' || other.status === 'processing');
  const runningUnderLiveLease = other.status === 'processing' && other.leaseExpiresAt! > now;
  return unfinishedPredecessor || runningUnderLiveLease;
}

/**
 * The cutoffs of events no delivery references. An event that had a delivery complete is measured
 * from that completion and only by the completed cutoff; any other (dead-lettered or never
 * delivered) from its creation and by the earliest cutoff.
 */
export function eventCutoffs(request: PurgeRequest): {
  orphanCutoff: number;
  completedCutoff: number;
} {
  const cutoffs = [request.completedBefore, request.deadBefore]
    .filter((cutoff) => cutoff !== undefined)
    .map((cutoff) => cutoff.getTime());
  return {
    orphanCutoff: cutoffs.length === 0 ? Number.NEGATIVE_INFINITY : Math.min(...cutoffs),
    completedCutoff: request.completedBefore?.getTime() ?? Number.NEGATIVE_INFINITY,
  };
}

export function serializeEnvelope(envelope: EventEnvelope): SerializedEnvelope {
  return JSON.parse(JSON.stringify(envelope)) as SerializedEnvelope;
}

export function isDue(delivery: StoredDelivery, now: Date): boolean {
  if (delivery.status === 'pending') {
    return delivery.availableAt <= now;
  }
  return delivery.status === 'processing' && delivery.leaseExpiresAt! <= now;
}

export function dueAtOf(delivery: StoredDelivery): Date {
  return delivery.status === 'pending' ? delivery.availableAt : delivery.leaseExpiresAt!;
}

export function compareDue(left: StoredDelivery, right: StoredDelivery): number {
  const byDueTime = dueAtOf(left).getTime() - dueAtOf(right).getTime();
  return byDueTime === 0 ? compareIds(left.id, right.id) : byDueTime;
}

export function compareIds(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}

export function clearLease(delivery: StoredDelivery): void {
  delivery.leaseOwner = null;
  delivery.leaseToken = null;
  delivery.leaseExpiresAt = null;
}

export function matchesQuery(
  delivery: StoredDelivery,
  { event, query }: { readonly event: StoredEvent; readonly query: DeliveryQuery },
): boolean {
  return (
    (query.status === undefined || delivery.status === query.status) &&
    (query.handler === undefined || delivery.handler === query.handler) &&
    (query.eventType === undefined || event.envelope.type === query.eventType) &&
    (query.eventId === undefined || delivery.eventId === query.eventId)
  );
}

export function isPurgeable(delivery: StoredDelivery, request: PurgeRequest): boolean {
  if (delivery.status === 'completed' && request.completedBefore !== undefined) {
    return delivery.completedAt! < request.completedBefore;
  }
  if (delivery.status === 'dead' && request.deadBefore !== undefined) {
    return delivery.updatedAt < request.deadBefore;
  }
  return false;
}
