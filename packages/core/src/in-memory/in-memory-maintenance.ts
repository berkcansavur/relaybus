import type {
  DeliveryQuery,
  DeliveryRecord,
  DeliveryStatus,
  EventStoreStatistics,
  PurgeRequest,
  PurgeResult,
} from '../event-store.js';
import type { InMemoryRowLocks } from './in-memory-row-locks.js';
import type { InMemoryTables } from './in-memory-tables.js';
import {
  compareIds,
  dueAtOf,
  eventCutoffs,
  isDue,
  isPurgeable,
  matchesQuery,
} from './stored-records.js';

const DEFAULT_LIST_LIMIT = 100;

/** Listing, requeueing, purging and counting the deliveries of the in-memory store. */
export class InMemoryMaintenance {
  readonly #tables: InMemoryTables;
  readonly #locks: InMemoryRowLocks;

  constructor(tables: InMemoryTables, locks: InMemoryRowLocks) {
    this.#tables = tables;
    this.#locks = locks;
  }

  listDeliveries(query: DeliveryQuery): DeliveryRecord[] {
    const limit = query.limit ?? DEFAULT_LIST_LIMIT;
    const { events, deliveries } = this.#tables;
    return [...deliveries.values()]
      .filter((delivery) => matchesQuery(delivery, { event: events.get(delivery.eventId)!, query }))
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
        event: this.#tables.readEnvelope(delivery.eventId),
      }));
  }

  requeue(deliveryIds: readonly string[], now: Date): number {
    let moved = 0;
    for (const deliveryId of new Set(deliveryIds)) {
      const delivery = this.#tables.deliveries.get(deliveryId);
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

  purge(request: PurgeRequest): PurgeResult {
    let deletedDeliveries = 0;
    const touchedEventIds = new Set<string>();
    for (const delivery of [...this.#tables.deliveries.values()]) {
      if (isPurgeable(delivery, request)) {
        this.#tables.deliveries.delete(delivery.id);
        touchedEventIds.add(delivery.eventId);
        deletedDeliveries += 1;
      }
    }
    return { deletedDeliveries, deletedEvents: this.#purgeEvents(request, touchedEventIds) };
  }

  statistics(now: Date): EventStoreStatistics {
    const counts: Record<DeliveryStatus, number> = {
      pending: 0,
      processing: 0,
      completed: 0,
      dead: 0,
    };
    let oldestDueAt: Date | null = null;
    for (const delivery of this.#tables.deliveries.values()) {
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

  /** Deletes the events no delivery references whose retention ran out; returns how many. */
  #purgeEvents(request: PurgeRequest, touchedEventIds: ReadonlySet<string>): number {
    const { events, deliveries } = this.#tables;
    const referencedEventIds = new Set(
      [...deliveries.values()].map((delivery) => delivery.eventId),
    );
    const { orphanCutoff, completedCutoff } = eventCutoffs(request);
    let deletedEvents = 0;
    for (const [eventId, event] of [...events]) {
      // An open append transaction holds the event (like a row lock): it is about to attach a delivery.
      if (referencedEventIds.has(eventId) || this.#locks.isHeld(`event:${eventId}`)) {
        continue;
      }
      const expired =
        event.completedAt === null
          ? touchedEventIds.has(eventId) || event.createdAt.getTime() < orphanCutoff
          : event.completedAt.getTime() < completedCutoff;
      if (expired) {
        events.delete(eventId);
        deletedEvents += 1;
      }
    }
    return deletedEvents;
  }
}
