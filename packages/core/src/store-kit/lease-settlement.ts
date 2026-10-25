import type { ErrorInfo } from '../errors.js';
import type { LeaseReference } from '../event-store.js';

/** How a claim ends without completing: retried later, given back, or dead-lettered. */
export type SettlementChange =
  | {
      readonly kind: 'reschedule';
      readonly availableAt: Date;
      readonly error: ErrorInfo;
      readonly now: Date;
    }
  | { readonly kind: 'release'; readonly now: Date }
  | { readonly kind: 'bury'; readonly error: ErrorInfo; readonly now: Date };

/** A change of the claim a lease names. */
export type LeaseChange = { readonly lease: LeaseReference; readonly change: SettlementChange };

/** A lease renewal. */
export type LeaseExtension = {
  readonly lease: LeaseReference;
  readonly leaseExpiresAt: Date;
  readonly now: Date;
};

/**
 * The fenced settlement skeleton every event store shares: find the claim a lease names (a lease
 * this store cannot have issued, or whose claim ended, is not held and changes nothing), apply the
 * change fenced by the lease, then forget the lease as handed out once the store answered.
 * A store supplies only how it finds a held claim, how it writes a change and a renewal, and its
 * bookkeeping of handed-out leases.
 */
export abstract class LeaseSettlement<THeld> {
  /** Renews the lease when it is still held; resolves false otherwise. */
  extendLease(extension: LeaseExtension): Promise<boolean> {
    const held = this.findHeld(extension.lease);
    return held === undefined ? Promise.resolve(false) : this.writeExtension(held, extension);
  }

  /**
   * Applies `change` when the lease is still held. Once the store answered (either way) the lease
   * is forgotten as handed out; when the write throws it stays registered, since the caller may
   * retry with the same lease.
   */
  async changeClaim(request: LeaseChange): Promise<boolean> {
    const held = this.findHeld(request.lease);
    if (held === undefined) {
      return false;
    }
    const changed = await this.writeChange(held, request);
    this.forgetHandedOut(request.lease);
    return changed;
  }

  /** The claim `lease` names, when this store issued the lease; undefined otherwise. */
  protected abstract findHeld(lease: LeaseReference): THeld | undefined;

  protected abstract writeChange(held: THeld, request: LeaseChange): Promise<boolean>;

  protected abstract writeExtension(held: THeld, extension: LeaseExtension): Promise<boolean>;

  /** Drops `lease` from the store's handed-out bookkeeping; nothing for a store that keeps none. */
  protected abstract forgetHandedOut(lease: LeaseReference): void;
}
