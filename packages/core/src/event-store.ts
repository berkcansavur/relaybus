import type { ErrorInfo } from './errors.js';
import type { EventEnvelope } from './event.js';

/**
 * Lifecycle of one handler's delivery of one event.
 * `pending` (waiting or scheduled for retry) -> `processing` (leased by a worker)
 * -> `completed` | `pending` (retry) | `dead` (exhausted or non-retryable).
 * `dead` -> `pending` only through an explicit requeue.
 *
 * A completed delivery is deleted. `completed` records exist only in stores
 * configured to keep a completion history (`retainCompleted` /
 * `completionHistory`), where they are listed, counted and purged like before.
 */
export type DeliveryStatus = 'pending' | 'processing' | 'completed' | 'dead';

export const DELIVERY_STATUSES: readonly DeliveryStatus[] = [
  'pending',
  'processing',
  'completed',
  'dead',
];

/**
 * Isolation level of a `bus.transaction`, in the vocabulary of SQL. `repeatable read` and
 * `serializable` can fail with a serialization failure that succeeds when the transaction runs
 * again; the bus then reruns `work` (see {@link TransactionOptions}).
 */
export type TransactionIsolationLevel = 'read committed' | 'repeatable read' | 'serializable';

export const TRANSACTION_ISOLATION_LEVELS: readonly TransactionIsolationLevel[] = [
  'read committed',
  'repeatable read',
  'serializable',
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

/**
 * Lease expiry is compared with the `now` the caller passes, taken from the
 * bus clock. Keep worker clocks synchronized (NTP): a skew larger than a third
 * of the lease duration lets another worker reclaim a delivery that is still running.
 *
 * Stores may grant the lease on a group of deliveries (a lane) instead of on
 * each delivery. The group is exclusive to `leaseOwner` while the lease is
 * live, so other owners do not see its pending deliveries until it expires or
 * is relinquished; every delivery of the group carries the group's fencing token.
 */
export interface ClaimRequest {
  /** Only deliveries of these handlers are claimed. */
  readonly handlers: readonly string[];
  readonly limit: number;
  readonly leaseOwner: string;
  readonly leaseExpiresAt: Date;
  readonly now: Date;
}

/**
 * Proof of ownership. Every state change after a claim must present it
 * (fencing). A lease stays valid until another claim replaces it, even after
 * its expiry time; expiry only makes the delivery claimable again.
 */
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

/** How `stop` closes a transport or a store. */
export interface CloseOptions {
  /**
   * Aborted when the shutdown deadline passes (`bus.stop({ timeout })`), possibly before `close` is
   * called. From then on, close what you opened yourself at once (destroy connections, cancel
   * running queries) instead of waiting for it to finish; the bus no longer waits for `close`.
   * Leave alone what the application passed in.
   */
  readonly signal?: AbortSignal;
}

/** How a store runs one transaction of `bus.transaction`. */
export interface EventStoreTransactionOptions {
  /**
   * Time budget of the whole transaction, in milliseconds. A store makes the database enforce it
   * where it can (for example a statement or idle-in-transaction timeout), so a transaction that
   * waits on something slow is ended by the server and its locks are released. The bus rolls back
   * a transaction that still ran past the budget, so a store may also ignore it.
   */
  readonly timeoutMs: number;
  /**
   * Isolation level the transaction runs at, set before its first statement. Only passed when
   * `bus.transaction` was given one, and only to a store that lists it in
   * {@link EventStore.isolationLevels}.
   */
  readonly isolationLevel?: TransactionIsolationLevel;
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
  /**
   * Runs `work` inside a new database transaction and commits when it resolves. `options` come
   * from `bus.transaction`; the store's own transactions and the dispatcher's pass none.
   */
  transaction<T>(
    work: (transaction: TTransaction) => Promise<T>,
    options?: EventStoreTransactionOptions,
  ): Promise<T>;
  /**
   * Optional. The isolation levels `transaction` can run at. `bus.transaction` with another
   * `isolationLevel` is rejected with a {@link ConfigurationError} before it opens a transaction;
   * a store without the list supports none. A store lists a level only when the database gives
   * the guarantee the name promises.
   */
  readonly isolationLevels?: readonly TransactionIsolationLevel[];
  /**
   * Optional. True when `error`, raised by `transaction` (by `work`, or by the commit), means the
   * database rolled the transaction back because of a conflict with a concurrent one and running
   * it again can succeed (PostgreSQL SQLSTATE `40001`). Without it `bus.transaction` never reruns.
   */
  isSerializationFailure?(error: unknown): boolean;
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
   * Counts the attempt (`attempt` of the result; a store may persist the count
   * only when the attempt fails or its lease is lost). Safe to call from many
   * processes at once.
   */
  claim(request: ClaimRequest): Promise<ClaimedDelivery[]>;
  /** Extends a lease the caller still holds. Returns false when the lease was lost. */
  extendLease(lease: LeaseReference, leaseExpiresAt: Date, now: Date): Promise<boolean>;
  /**
   * Marks a leased delivery completed by deleting it (or moving it to the
   * completion history, when the store keeps one). With `transaction` the change
   * commits together with the handler's own writes. Returns false when the lease
   * was lost.
   */
  complete(lease: LeaseReference, now: Date, transaction?: TTransaction): Promise<boolean>;
  /** Returns a leased delivery to pending, due at `availableAt`. */
  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean>;
  /**
   * Gives a leased delivery back without counting the attempt: pending, due
   * now, attempts decremented. Used when a handler is aborted at shutdown.
   */
  release(lease: LeaseReference, now: Date): Promise<boolean>;
  /**
   * Marks many leased deliveries completed with one round trip. Returns, for
   * every lease in the same order, whether it was completed (false: lease lost
   * or already finished). Each lease is fenced individually, exactly as in
   * {@link EventStore.complete}. Optional: the dispatcher falls back to
   * `complete` when a store does not implement it.
   */
  completeMany?(leases: readonly LeaseReference[], now: Date): Promise<boolean[]>;
  /** Moves a leased delivery to dead. */
  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean>;
  listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]>;
  /** Moves dead deliveries back to pending with a fresh attempt budget. Returns how many moved. */
  requeue(deliveryIds: readonly string[], now: Date): Promise<number>;
  purge(request: PurgeRequest): Promise<PurgeResult>;
  statistics(now: Date): Promise<EventStoreStatistics>;
  /**
   * Hands back every lease group that `leaseOwner` holds without running work,
   * so other workers can take them at once instead of after lease expiry.
   * Called at graceful shutdown after the owner's deliveries settled. Groups
   * with running deliveries stay leased. Optional.
   */
  relinquish?(leaseOwner: string, now: Date): Promise<void>;
  /**
   * Calls `listener` soon after a transaction that added deliveries due at once has committed, in this
   * process or in any other one, so an idle dispatcher claims them without waiting for its next poll.
   * A hint, never a guarantee: a report may be late, merged with others, or lost (while a connection
   * is re-established, for instance), and polling still finds every delivery. Resolves with a function
   * that stops listening, or with `undefined` when this store cannot listen. Optional.
   */
  watchNewDeliveries?(listener: () => void): Promise<(() => Promise<void>) | undefined>;
  /** Releases connections the store opened itself; forced once `options.signal` aborts. */
  close?(options?: CloseOptions): Promise<void>;
}
