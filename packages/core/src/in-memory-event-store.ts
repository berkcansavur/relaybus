import { ConfigurationError, type ErrorInfo } from './errors.js';
import type {
  AppendEntry,
  AppendResult,
  ClaimRequest,
  ClaimedDelivery,
  DeliveryQuery,
  DeliveryRecord,
  EventStore,
  EventStoreStatistics,
  LeaseReference,
  NewDelivery,
  PurgeRequest,
  PurgeResult,
} from './event-store.js';
import { createUuidV7, type IdentifierGenerator } from './identifier.js';
import { InMemoryMaintenance } from './in-memory/in-memory-maintenance.js';
import { InMemoryRowLocks } from './in-memory/in-memory-row-locks.js';
import { InMemorySettlement } from './in-memory/in-memory-settlement.js';
import { InMemoryTables } from './in-memory/in-memory-tables.js';
import {
  blocksKey,
  compareDue,
  isDue,
  serializeEnvelope,
  type SerializedEnvelope,
  type StoredDelivery,
} from './in-memory/stored-records.js';
import { InMemoryTransaction } from './in-memory-transaction.js';
import { CompositeKey } from './values/composite-key.js';

export interface InMemoryEventStoreOptions {
  /** Allow use when `NODE_ENV` is `production`. Data is lost on restart. Default false. */
  readonly allowInProduction?: boolean;
  readonly generateIdentifier?: IdentifierGenerator;
  /**
   * Keep a record of completed deliveries (status `completed`, listed, counted
   * and purged by `retention.completed`). Default false: a completed delivery
   * is deleted, like in the database stores.
   */
  readonly retainCompleted?: boolean;
}

/** One entry of an append, applied when its transaction commits. */
type ApplyAppendInput = {
  readonly eventId: string;
  readonly serialized: SerializedEnvelope;
  readonly isDuplicate: boolean;
  readonly newDeliveries: readonly NewDelivery[];
  readonly now: Date;
};

/**
 * Event store kept in process memory, with the same semantics as the database
 * stores: transactional appends, leases, fencing and per-key ordering. Payloads
 * are round-tripped through JSON exactly like a database would store them.
 * Meant for tests and local development.
 */
export class InMemoryEventStore implements EventStore<InMemoryTransaction> {
  readonly #tables = new InMemoryTables();
  readonly #locks = new InMemoryRowLocks();
  readonly #settlement: InMemorySettlement;
  readonly #maintenance = new InMemoryMaintenance(this.#tables, this.#locks);
  readonly #generateIdentifier: IdentifierGenerator;

  constructor(options: InMemoryEventStoreOptions = {}) {
    if (process.env['NODE_ENV'] === 'production' && options.allowInProduction !== true) {
      throw new ConfigurationError(
        'InMemoryEventStore loses every event on restart. Use a database store in production, or pass allowInProduction: true',
      );
    }
    this.#generateIdentifier = options.generateIdentifier ?? (() => createUuidV7());
    this.#settlement = new InMemorySettlement(this.#tables, options.retainCompleted ?? false);
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

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  async append(
    transaction: InMemoryTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    await this.#locks.lock(
      transaction,
      entries.flatMap((entry) => [
        `event:${entry.event.id}`,
        ...(entry.event.key === null ? [] : [`key:${entry.event.key}`]),
      ]),
    );
    // What earlier appends of this transaction wrote is not applied yet, but it is visible to this one.
    const written = this.#locks.written(transaction);
    const pendingEventIds = written.eventIds;
    const pendingDeliveryKeys = written.deliveryKeys;
    const appendedEventIds: string[] = [];
    const duplicateEventIds: string[] = [];
    let appendedDeliveryCount = 0;
    for (const entry of entries) {
      const eventId = entry.event.id;
      // Before anything is remembered: an event that cannot be serialized leaves no trace behind.
      const serialized = serializeEnvelope(entry.event);
      const isDuplicate = this.#tables.events.has(eventId) || pendingEventIds.has(eventId);
      if (isDuplicate) {
        duplicateEventIds.push(eventId);
      } else {
        appendedEventIds.push(eventId);
        pendingEventIds.add(eventId);
      }
      const newDeliveries = this.#newDeliveries(entry, pendingDeliveryKeys);
      appendedDeliveryCount += newDeliveries.length;
      transaction.enqueue(() => {
        this.#applyAppend({ eventId, serialized, isDuplicate, newDeliveries, now });
      });
    }
    return { appendedEventIds, duplicateEventIds, appendedDeliveryCount };
  }

  /**
   * The deliveries of `entry` whose handler the event neither has nor gets earlier in the
   * transaction; remembers them in `pendingDeliveryKeys`.
   */
  #newDeliveries(entry: AppendEntry, pendingDeliveryKeys: Set<string>): NewDelivery[] {
    const eventId = entry.event.id;
    return entry.deliveries.filter((delivery) => {
      const deliveryKey = CompositeKey.of(eventId, delivery.handler);
      if (
        pendingDeliveryKeys.has(deliveryKey) ||
        this.#tables.events.get(eventId)?.handlers.has(delivery.handler) === true
      ) {
        return false;
      }
      pendingDeliveryKeys.add(deliveryKey);
      return true;
    });
  }

  /** Applies one entry of an append when its transaction commits. */
  #applyAppend(input: ApplyAppendInput): void {
    const { eventId, serialized, isDuplicate, newDeliveries, now } = input;
    const { events, deliveries } = this.#tables;
    if (!isDuplicate) {
      events.set(eventId, {
        envelope: serialized,
        sequence: this.#tables.nextSequence(serialized.key),
        createdAt: now,
        handlers: new Set(),
        completedAt: null,
      });
    }
    const stored = events.get(eventId)!;
    for (const delivery of newDeliveries) {
      stored.handlers.add(delivery.handler);
      deliveries.set(delivery.id, {
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
  }

  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    const handlers = new Set(request.handlers);
    const due = [...this.#tables.deliveries.values()]
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
        event: this.#tables.readEnvelope(delivery.eventId),
      });
    }
    return claimed;
  }

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  async extendLease(lease: LeaseReference, leaseExpiresAt: Date, now: Date): Promise<boolean> {
    return this.#settlement.extendLease({ lease, leaseExpiresAt, now });
  }

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  async complete(
    lease: LeaseReference,
    now: Date,
    transaction?: InMemoryTransaction,
  ): Promise<boolean> {
    return this.#settlement.complete(
      lease,
      transaction === undefined ? { now } : { now, transaction },
    );
  }

  async completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return Promise.all(leases.map((lease) => this.complete(lease, now)));
  }

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  async reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#settlement.changeClaim({
      lease,
      change: { kind: 'reschedule', availableAt, error, now },
    });
  }

  async release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#settlement.changeClaim({ lease, change: { kind: 'release', now } });
  }

  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  async bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#settlement.changeClaim({ lease, change: { kind: 'bury', error, now } });
  }

  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    return this.#maintenance.listDeliveries(query);
  }

  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    return this.#maintenance.requeue(deliveryIds, now);
  }

  async purge(request: PurgeRequest): Promise<PurgeResult> {
    return this.#maintenance.purge(request);
  }

  async statistics(now: Date): Promise<EventStoreStatistics> {
    return this.#maintenance.statistics(now);
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
    for (const other of this.#tables.deliveries.values()) {
      if (blocksKey(other, { delivery, now })) {
        return true;
      }
    }
    return false;
  }
}
