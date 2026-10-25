import type { LeaseReference } from '../event-store.js';
import type { InMemoryTransaction } from '../in-memory-transaction.js';
import {
  LeaseSettlement,
  type LeaseChange,
  type LeaseExtension,
} from '../store-kit/lease-settlement.js';
import type { InMemoryTables } from './in-memory-tables.js';
import { applyTransition, clearLease, type StoredDelivery } from './stored-records.js';

/**
 * Settles the claims of the in-memory store: a lease is held while its delivery is processing
 * under exactly that token. The store keeps no bookkeeping of handed-out leases.
 */
export class InMemorySettlement extends LeaseSettlement<StoredDelivery> {
  readonly #tables: InMemoryTables;
  readonly #retainCompleted: boolean;

  constructor(tables: InMemoryTables, retainCompleted: boolean) {
    super();
    this.#tables = tables;
    this.#retainCompleted = retainCompleted;
  }

  /** Completes now, or when `transaction` commits if the lease is still held then. */
  complete(
    lease: LeaseReference,
    { now, transaction }: { readonly now: Date; readonly transaction?: InMemoryTransaction },
  ): boolean {
    const delivery = this.findHeld(lease);
    if (delivery === undefined) {
      return false;
    }
    if (transaction === undefined) {
      this.#finish(delivery, now);
      return true;
    }
    transaction.enqueue(() => {
      const held = this.findHeld(lease);
      if (held !== undefined) {
        this.#finish(held, now);
      }
    });
    return true;
  }

  protected findHeld(lease: LeaseReference): StoredDelivery | undefined {
    const delivery = this.#tables.deliveries.get(lease.deliveryId);
    if (delivery?.status === 'processing' && delivery.leaseToken === lease.leaseToken) {
      return delivery;
    }
    return undefined;
  }

  protected async writeChange(held: StoredDelivery, { change }: LeaseChange): Promise<boolean> {
    applyTransition(held, change);
    return true;
  }

  protected async writeExtension(
    held: StoredDelivery,
    { leaseExpiresAt, now }: LeaseExtension,
  ): Promise<boolean> {
    held.leaseExpiresAt = leaseExpiresAt;
    held.updatedAt = now;
    return true;
  }

  protected forgetHandedOut(): void {
    // Nothing is handed out: the store answers from the delivery itself.
  }

  /** Deletes a completed delivery, or marks it completed when a history is kept. */
  #finish(delivery: StoredDelivery, now: Date): void {
    const event = this.#tables.events.get(delivery.eventId)!;
    if (event.completedAt === null || event.completedAt < now) {
      event.completedAt = now;
    }
    if (!this.#retainCompleted) {
      this.#tables.deliveries.delete(delivery.id);
      return;
    }
    delivery.status = 'completed';
    delivery.completedAt = now;
    delivery.updatedAt = now;
    clearLease(delivery);
  }
}
