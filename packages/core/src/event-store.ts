import type { ErrorInfo } from './errors.js';
import type { EventEnvelope } from './event.js';

/**
 * Lifecycle of one handler's delivery of one event.
 * `pending` (waiting or scheduled for retry) -> `processing` (leased by a worker)
 * -> `completed` | `pending` (retry) | `dead` (exhausted or non-retryable).
 * `dead` -> `pending` only through an explicit requeue.
 */
export type DeliveryStatus = 'pending' | 'processing' | 'completed' | 'dead';

export const DELIVERY_STATUSES: readonly DeliveryStatus[] = [
  'pending',
  'processing',
  'completed',
  'dead',
];

export interface NewDelivery {
  readonly id: string;
  /** Stable handler id. Together with the event id it is unique. */
  readonly handler: string;
  readonly availableAt: Date;
}

export interface AppendEntry {
  readonly event: EventEnvelope;
  readonly deliveries: readonly NewDelivery[];
}

export interface AppendResult {
  /** Events written by this call. */
  readonly appendedEventIds: readonly string[];
  /** Events that already existed. Their missing deliveries were still added. */
  readonly duplicateEventIds: readonly string[];
  /** Deliveries written by this call. Existing (event, handler) pairs are skipped. */
  readonly appendedDeliveryCount: number;
}

export interface ClaimRequest {
  /** Only deliveries of these handlers are claimed. */
  readonly handlers: readonly string[];
  readonly limit: number;
  readonly leaseOwner: string;
  readonly leaseExpiresAt: Date;
  readonly now: Date;
}

/** Proof of ownership. Every state change after a claim must present it (fencing). */
export interface LeaseReference {
  readonly deliveryId: string;
  readonly leaseToken: string;
}

export interface ClaimedDelivery extends LeaseReference {
  readonly handler: string;
  /** Attempt number of this claim, starting at 1. */
  readonly attempt: number;
  readonly leaseExpiresAt: Date;
  readonly event: EventEnvelope;
}

export interface DeliveryRecord {
  readonly id: string;
  readonly handler: string;
  readonly status: DeliveryStatus;
  readonly attempts: number;
  readonly availableAt: Date;
  readonly leaseOwner: string | null;
  readonly leaseExpiresAt: Date | null;
  readonly lastError: ErrorInfo | null;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly completedAt: Date | null;
  readonly event: EventEnvelope;
}

export interface DeliveryQuery {
  readonly status?: DeliveryStatus;
  readonly handler?: string;
  readonly eventType?: string;
  readonly eventId?: string;
  /** Default 100. */
  readonly limit?: number;
}

export interface PurgeRequest {
  /** Delete completed deliveries completed before this instant. */
  readonly completedBefore?: Date;
  /** Delete dead deliveries last updated before this instant. */
  readonly deadBefore?: Date;
}

export interface PurgeResult {
  readonly deletedDeliveries: number;
  /** Events deleted because no delivery referenced them any more. */
  readonly deletedEvents: number;
}

export interface EventStoreStatistics {
  readonly pending: number;
  readonly processing: number;
  readonly completed: number;
  readonly dead: number;
  /** `availableAt` of the oldest delivery that is due but not yet processed. */
  readonly oldestDueAt: Date | null;
}

/**
 * Storage port. Implementations persist events and deliveries in the same
 * database as the application's business data so that both commit atomically.
 *
 * Every implementation must pass the contract suite in `@relaybus/testing`.
 */
export interface EventStore<TTransaction = unknown> {
  /** Creates tables, collections and indexes when they do not exist. */
  setup?(): Promise<void>;
  /** Runs `work` inside a new database transaction and commits when it resolves. */
  transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T>;
  /**
   * Writes events and their deliveries inside `transaction`. Idempotent on
   * event id and on (event id, handler). Assigns a per-key sequence to keyed
   * events in commit order.
   */
  append(
    transaction: TTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult>;
  /**
   * Leases up to `limit` deliveries that are due: pending with `availableAt <= now`,
   * or processing with an expired lease. A keyed delivery is only claimable when
   * no earlier delivery of the same handler and key is pending or processing.
   * Increments the attempt count. Safe to call from many processes at once.
   */
  claim(request: ClaimRequest): Promise<ClaimedDelivery[]>;
  /** Extends a lease the caller still holds. Returns false when the lease was lost. */
  extendLease(lease: LeaseReference, leaseExpiresAt: Date, now: Date): Promise<boolean>;
  /**
   * Marks a leased delivery completed. With `transaction` the change commits
   * together with the handler's own writes. Returns false when the lease was lost.
   */
  complete(lease: LeaseReference, now: Date, transaction?: TTransaction): Promise<boolean>;
  /** Returns a leased delivery to pending, due at `availableAt`. */
  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean>;
  /** Moves a leased delivery to dead. */
  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean>;
  listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]>;
  /** Moves dead deliveries back to pending with a fresh attempt budget. Returns how many moved. */
  requeue(deliveryIds: readonly string[], now: Date): Promise<number>;
  purge(request: PurgeRequest): Promise<PurgeResult>;
  statistics(now: Date): Promise<EventStoreStatistics>;
  /** Releases connections the store opened itself. */
  close?(): Promise<void>;
}
