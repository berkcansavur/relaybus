import type { LeaseReference } from '../event-store.js';
import { DeliveryLeaseIdentity } from '../values/delivery-lease-identity.js';

/** What a ledger remembers of a claim: at least who it was handed to and which delivery. */
export interface HandedOutEntry {
  readonly owner: string;
  readonly deliveryId: string;
}

/**
 * The claims a store instance handed out and has not seen settled, keyed by
 * {@link DeliveryLeaseIdentity} (delivery id and exact lease token). After a failed claim the
 * store keeps exactly these listed as running and gives the rest back.
 */
export class HandedOutLedger<TEntry extends HandedOutEntry> {
  readonly #entries = new Map<string, TEntry>();

  register(lease: LeaseReference, entry: TEntry): void {
    this.#entries.set(DeliveryLeaseIdentity.of(lease.deliveryId, lease.leaseToken), entry);
  }

  /** Whether the claim with this identity ({@link DeliveryLeaseIdentity.of}) is handed out. */
  has(identity: string): boolean {
    return this.#entries.has(identity);
  }

  forget(lease: LeaseReference): void {
    this.#entries.delete(DeliveryLeaseIdentity.of(lease.deliveryId, lease.leaseToken));
  }

  /** Forgets every entry `settled` says is settled. */
  forgetWhere(settled: (entry: TEntry, identity: string) => boolean): void {
    for (const [identity, entry] of this.#entries) {
      if (settled(entry, identity)) {
        this.#entries.delete(identity);
      }
    }
  }

  /** The entries handed out to `owner`. */
  entriesOf(owner: string): TEntry[] {
    return [...this.#entries.values()].filter((entry) => entry.owner === owner);
  }
}
