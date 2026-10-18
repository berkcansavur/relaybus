import { ConfigurationError, type ErrorInfo } from './errors.js';
import type { EventEnvelope } from './event.js';
import type {
  AppendEntry,
  AppendResult,
  ClaimRequest,
  ClaimedDelivery,
  DeliveryQuery,
  DeliveryRecord,
  DeliveryStatus,
  EventStore,
  EventStoreStatistics,
  LeaseReference,
  PurgeRequest,
  PurgeResult,
} from './event-store.js';
import { createUuidV7, type IdentifierGenerator } from './identifier.js';

export interface InMemoryEventStoreOptions {
  /** Allow use when `NODE_ENV` is `production`. Data is lost on restart. Default false. */
  readonly allowInProduction?: boolean;
  readonly generateIdentifier?: IdentifierGenerator;
}

/** Transaction handle of {@link InMemoryEventStore}. Writes apply on commit only. */
export class InMemoryTransaction {
  readonly #operations: Array<() => void> = [];
  readonly #finishers: Array<() => void> = [];
  #state: 'open' | 'committed' | 'rolled-back' = 'open';

  get state(): 'open' | 'committed' | 'rolled-back' {
    return this.#state;
  }

  enqueue(operation: () => void): void {
    if (this.#state !== 'open') {
      throw new ConfigurationError(`Cannot write through a ${this.#state} in-memory transaction`);
    }
    this.#operations.push(operation);
  }

  /** Runs `finisher` once the transaction commits or rolls back, like a lock release. */
  onFinish(finisher: () => void): void {
    this.#finishers.push(finisher);
  }

  commit(): void {
    this.#state = 'committed';
    for (const operation of this.#operations) {
      operation();
    }
    this.#finish();
  }

  rollback(): void {
    this.#state = 'rolled-back';
    this.#finish();
  }

  #finish(): void {
    for (const finisher of this.#finishers.splice(0)) {
      finisher();
    }
  }
}

interface HeldLock {
  readonly owner: InMemoryTransaction;
  readonly released: Promise<void>;
}

interface StoredEvent {
  readonly envelope: SerializedEnvelope;
  readonly sequence: number | null;
}

interface SerializedEnvelope extends Omit<EventEnvelope, 'occurredAt'> {
  readonly occurredAt: string;
}

interface StoredDelivery {
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

/**
 * Event store kept in process memory, with the same semantics as the database
 * stores: transactional appends, leases, fencing and per-key ordering. Payloads
 * are round-tripped through JSON exactly like a database would store them.
 * Meant for tests and local development.
 */
export class InMemoryEventStore implements EventStore<InMemoryTransaction> {
  readonly #events = new Map<string, StoredEvent>();
  readonly #deliveries = new Map<string, StoredDelivery>();
  readonly #keySequences = new Map<string, number>();
  /** Row-lock emulation: event ids and ordering keys written by open transactions. */
  readonly #locks = new Map<string, HeldLock>();
  readonly #generateIdentifier: IdentifierGenerator;

  constructor(options: InMemoryEventStoreOptions = {}) {
    if (process.env['NODE_ENV'] === 'production' && options.allowInProduction !== true) {
      throw new ConfigurationError(
        'InMemoryEventStore loses every event on restart. Use a database store in production, or pass allowInProduction: true',
      );
    }
    this.#generateIdentifier = options.generateIdentifier ?? (() => createUuidV7());
  }

  async transaction<T>(work: (transaction: InMemoryTransaction) => Promise<T>): Promise<T> {
    const transaction = new InMemoryTransaction();
    try {
      const result = await work(transaction);
      transaction.commit();
      return result;
    } catch (error) {
      transaction.rollback();
      throw error;
    }
  }

  async append(
    transaction: InMemoryTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    await this.#lock(
      transaction,
      entries.flatMap((entry) => [
        `event:${entry.event.id}`,
        ...(entry.event.key === null ? [] : [`key:${entry.event.key}`]),
      ]),
    );
    const pendingEventIds = new Set<string>();
    const pendingDeliveryKeys = new Set<string>();
    const appendedEventIds: string[] = [];
    const duplicateEventIds: string[] = [];
    let appendedDeliveryCount = 0;
    for (const entry of entries) {
      const eventId = entry.event.id;
      const isDuplicate = this.#events.has(eventId) || pendingEventIds.has(eventId);
      if (isDuplicate) {
        duplicateEventIds.push(eventId);
      } else {
        appendedEventIds.push(eventId);
        pendingEventIds.add(eventId);
      }
      const newDeliveries = entry.deliveries.filter((delivery) => {
        const deliveryKey = `${eventId}\u0000${delivery.handler}`;
        if (pendingDeliveryKeys.has(deliveryKey) || this.#hasDelivery(eventId, delivery.handler)) {
          return false;
        }
        pendingDeliveryKeys.add(deliveryKey);
        return true;
      });
      appendedDeliveryCount += newDeliveries.length;
      const serialized = serializeEnvelope(entry.event);
      transaction.enqueue(() => {
        if (!isDuplicate) {
          this.#events.set(eventId, {
            envelope: serialized,
            sequence: this.#nextSequence(serialized.key),
          });
        }
        const stored = this.#events.get(eventId)!;
        for (const delivery of newDeliveries) {
          this.#deliveries.set(delivery.id, {
            id: delivery.id,
            eventId,
            handler: delivery.handler,
            key: stored.envelope.key,
            sequence: stored.sequence,
            status: 'pending',
            attempts: 0,
            availableAt: delivery.availableAt,
            leaseOwner: null,
            leaseToken: null,
            leaseExpiresAt: null,
            lastError: null,
            createdAt: now,
            updatedAt: now,
            completedAt: null,
          });
        }
      });
    }
    return { appendedEventIds, duplicateEventIds, appendedDeliveryCount };
  }

  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    const handlers = new Set(request.handlers);
    const due = [...this.#deliveries.values()]
      .filter((delivery) => handlers.has(delivery.handler) && isDue(delivery, request.now))
      .sort(compareDue);
    const claimed: ClaimedDelivery[] = [];
    for (const delivery of due) {
      if (claimed.length >= request.limit) {
        break;
      }
      if (this.#isKeyBlocked(delivery, request.now)) {
        continue;
      }
      delivery.status = 'processing';
      delivery.attempts += 1;
      delivery.leaseOwner = request.leaseOwner;
      delivery.leaseToken = this.#generateIdentifier();
      delivery.leaseExpiresAt = request.leaseExpiresAt;
      delivery.updatedAt = request.now;
      claimed.push({
        deliveryId: delivery.id,
        leaseToken: delivery.leaseToken,
        handler: delivery.handler,
        attempt: delivery.attempts,
        leaseExpiresAt: request.leaseExpiresAt,
        event: this.#readEnvelope(delivery.eventId),
      });
    }
    return claimed;
  }

  async extendLease(lease: LeaseReference, leaseExpiresAt: Date, now: Date): Promise<boolean> {
    return this.#updateLeased(lease, (delivery) => {
      delivery.leaseExpiresAt = leaseExpiresAt;
      delivery.updatedAt = now;
    });
  }

  async complete(
    lease: LeaseReference,
    now: Date,
    transaction?: InMemoryTransaction,
  ): Promise<boolean> {
    const finish = (delivery: StoredDelivery): void => {
      delivery.status = 'completed';
      delivery.completedAt = now;
      delivery.updatedAt = now;
      clearLease(delivery);
    };
    if (transaction === undefined) {
      return this.#updateLeased(lease, finish);
    }
    if (this.#findLeased(lease) === undefined) {
      return false;
    }
    transaction.enqueue(() => {
      const delivery = this.#findLeased(lease);
      if (delivery !== undefined) {
        finish(delivery);
      }
    });
    return true;
  }

  async reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#updateLeased(lease, (delivery) => {
      delivery.status = 'pending';
      delivery.availableAt = availableAt;
      delivery.lastError = error;
      delivery.updatedAt = now;
      clearLease(delivery);
    });
  }

  async release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#updateLeased(lease, (delivery) => {
      delivery.status = 'pending';
      delivery.attempts -= 1;
      delivery.availableAt = now;
      delivery.updatedAt = now;
      clearLease(delivery);
    });
  }

  async bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#updateLeased(lease, (delivery) => {
      delivery.status = 'dead';
      delivery.lastError = error;
      delivery.updatedAt = now;
      clearLease(delivery);
    });
  }

  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    const limit = query.limit ?? 100;
    return [...this.#deliveries.values()]
      .filter((delivery) => matchesQuery(delivery, this.#events.get(delivery.eventId)!, query))
      .sort(
        (left, right) =>
          left.createdAt.getTime() - right.createdAt.getTime() || compareIds(left.id, right.id),
      )
      .slice(0, limit)
      .map((delivery) => ({
        id: delivery.id,
        handler: delivery.handler,
        status: delivery.status,
        attempts: delivery.attempts,
        availableAt: delivery.availableAt,
        leaseOwner: delivery.leaseOwner,
        leaseExpiresAt: delivery.leaseExpiresAt,
        lastError: delivery.lastError,
        createdAt: delivery.createdAt,
        updatedAt: delivery.updatedAt,
        completedAt: delivery.completedAt,
        event: this.#readEnvelope(delivery.eventId),
      }));
  }

  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    let moved = 0;
    for (const deliveryId of new Set(deliveryIds)) {
      const delivery = this.#deliveries.get(deliveryId);
      if (delivery?.status !== 'dead') {
        continue;
      }
      delivery.status = 'pending';
      delivery.attempts = 0;
      delivery.availableAt = now;
      delivery.updatedAt = now;
      moved += 1;
    }
    return moved;
  }

  async purge(request: PurgeRequest): Promise<PurgeResult> {
    let deletedDeliveries = 0;
    const touchedEventIds = new Set<string>();
    for (const delivery of [...this.#deliveries.values()]) {
      if (isPurgeable(delivery, request)) {
        this.#deliveries.delete(delivery.id);
        touchedEventIds.add(delivery.eventId);
        deletedDeliveries += 1;
      }
    }
    const referencedEventIds = new Set(
      [...this.#deliveries.values()].map((delivery) => delivery.eventId),
    );
    let deletedEvents = 0;
    for (const eventId of touchedEventIds) {
      if (!referencedEventIds.has(eventId)) {
        this.#events.delete(eventId);
        deletedEvents += 1;
      }
    }
    return { deletedDeliveries, deletedEvents };
  }

  async statistics(now: Date): Promise<EventStoreStatistics> {
    const counts: Record<DeliveryStatus, number> = {
      pending: 0,
      processing: 0,
      completed: 0,
      dead: 0,
    };
    let oldestDueAt: Date | null = null;
    for (const delivery of this.#deliveries.values()) {
      counts[delivery.status] += 1;
      if (isDue(delivery, now)) {
        const dueAt = dueAtOf(delivery);
        if (oldestDueAt === null || dueAt < oldestDueAt) {
          oldestDueAt = dueAt;
        }
      }
    }
    return { ...counts, oldestDueAt };
  }

  /**
   * Waits for other open transactions holding any of `names`, then holds them
   * until `transaction` finishes, the way a database holds row locks. Sorted
   * acquisition keeps two transactions from waiting on each other.
   */
  async #lock(transaction: InMemoryTransaction, names: readonly string[]): Promise<void> {
    for (const name of [...new Set(names)].sort()) {
      let held = this.#locks.get(name);
      while (held !== undefined && held.owner !== transaction) {
        await held.released;
        held = this.#locks.get(name);
      }
      if (held !== undefined) {
        continue;
      }
      let release!: () => void;
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      this.#locks.set(name, { owner: transaction, released });
      transaction.onFinish(() => {
        this.#locks.delete(name);
        release();
      });
    }
  }

  #hasDelivery(eventId: string, handler: string): boolean {
    for (const delivery of this.#deliveries.values()) {
      if (delivery.eventId === eventId && delivery.handler === handler) {
        return true;
      }
    }
    return false;
  }

  #nextSequence(key: string | null): number | null {
    if (key === null) {
      return null;
    }
    const next = (this.#keySequences.get(key) ?? 0) + 1;
    this.#keySequences.set(key, next);
    return next;
  }

  /**
   * A keyed delivery waits while an earlier delivery of the same handler and
   * key is unfinished, or while any other delivery of that handler and key is
   * running under a live lease (a requeued dead letter must not run alongside
   * its successor).
   */
  #isKeyBlocked(delivery: StoredDelivery, now: Date): boolean {
    if (delivery.key === null) {
      return false;
    }
    for (const other of this.#deliveries.values()) {
      if (other === delivery || other.handler !== delivery.handler || other.key !== delivery.key) {
        continue;
      }
      const unfinishedPredecessor =
        other.sequence! < delivery.sequence! &&
        (other.status === 'pending' || other.status === 'processing');
      const runningUnderLiveLease = other.status === 'processing' && other.leaseExpiresAt! > now;
      if (unfinishedPredecessor || runningUnderLiveLease) {
        return true;
      }
    }
    return false;
  }

  #findLeased(lease: LeaseReference): StoredDelivery | undefined {
    const delivery = this.#deliveries.get(lease.deliveryId);
    if (delivery?.status === 'processing' && delivery.leaseToken === lease.leaseToken) {
      return delivery;
    }
    return undefined;
  }

  #updateLeased(lease: LeaseReference, update: (delivery: StoredDelivery) => void): boolean {
    const delivery = this.#findLeased(lease);
    if (delivery === undefined) {
      return false;
    }
    update(delivery);
    return true;
  }

  #readEnvelope(eventId: string): EventEnvelope {
    const serialized = this.#events.get(eventId)!.envelope;
    const copy = JSON.parse(JSON.stringify(serialized)) as SerializedEnvelope;
    return { ...copy, occurredAt: new Date(copy.occurredAt) };
  }
}

function serializeEnvelope(envelope: EventEnvelope): SerializedEnvelope {
  return JSON.parse(JSON.stringify(envelope)) as SerializedEnvelope;
}

function isDue(delivery: StoredDelivery, now: Date): boolean {
  if (delivery.status === 'pending') {
    return delivery.availableAt <= now;
  }
  return delivery.status === 'processing' && delivery.leaseExpiresAt! <= now;
}

function dueAtOf(delivery: StoredDelivery): Date {
  return delivery.status === 'pending' ? delivery.availableAt : delivery.leaseExpiresAt!;
}

function compareDue(left: StoredDelivery, right: StoredDelivery): number {
  const byDueTime = dueAtOf(left).getTime() - dueAtOf(right).getTime();
  return byDueTime === 0 ? compareIds(left.id, right.id) : byDueTime;
}

function compareIds(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}

function clearLease(delivery: StoredDelivery): void {
  delivery.leaseOwner = null;
  delivery.leaseToken = null;
  delivery.leaseExpiresAt = null;
}

function matchesQuery(delivery: StoredDelivery, event: StoredEvent, query: DeliveryQuery): boolean {
  return (
    (query.status === undefined || delivery.status === query.status) &&
    (query.handler === undefined || delivery.handler === query.handler) &&
    (query.eventType === undefined || event.envelope.type === query.eventType) &&
    (query.eventId === undefined || delivery.eventId === query.eventId)
  );
}

function isPurgeable(delivery: StoredDelivery, request: PurgeRequest): boolean {
  if (delivery.status === 'completed' && request.completedBefore !== undefined) {
    return delivery.completedAt! < request.completedBefore;
  }
  if (delivery.status === 'dead' && request.deadBefore !== undefined) {
    return delivery.updatedAt < request.deadBefore;
  }
  return false;
}
