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

/** A lease renewal; `now` for a store that records when it happened. */
export type LeaseExtension = {
  readonly lease: LeaseReference;
  readonly leaseExpiresAt: Date;
  readonly now?: Date;
};

/**
 * What a store answers about a lease: at once when it answers from memory, later when it asks a
 * database. Answering at once keeps the in-memory store as fast (in microtasks) as it always was.
 */
export type StoreAnswer = boolean | Promise<boolean>;

/**
 * The fenced settlement skeleton every event store shares: find the claim a lease names (a lease
 * this store cannot have issued, or whose claim ended, is not held and changes nothing), apply the
 * change fenced by the lease, then forget the lease as handed out once the store answered.
 * A store supplies only how it finds a held claim, how it writes a change and a renewal, and its
 * bookkeeping of handed-out leases.
 */
export abstract class LeaseSettlement<THeld> {
  /** Renews the lease when it is still held; false otherwise. */
  extendLease(extension: LeaseExtension): StoreAnswer {
    const held = this.findHeld(extension.lease);
    return held === undefined ? false : this.writeExtension(held, extension);
  }

  /**
   * Applies `change` when the lease is still held. Once the store answered (either way) the lease
   * is forgotten as handed out; when the write throws it stays registered, since the caller may
   * retry with the same lease.
   */
  changeClaim(request: LeaseChange): StoreAnswer {
    const held = this.findHeld(request.lease);
    if (held === undefined) {
      return false;
    }
    const changed = this.writeChange(held, request);
    if (typeof changed === 'boolean') {
      this.forgetHandedOut(request.lease);
      return changed;
    }
    return this.#forgetOnceAnswered(request.lease, changed);
  }

  /** The claim `lease` names, when this store issued the lease; undefined otherwise. */
  protected abstract findHeld(lease: LeaseReference): THeld | undefined;

  protected abstract writeChange(held: THeld, request: LeaseChange): StoreAnswer;

  protected abstract writeExtension(held: THeld, extension: LeaseExtension): StoreAnswer;

  /** Drops `lease` from the store's handed-out bookkeeping; nothing for a store that keeps none. */
  protected abstract forgetHandedOut(lease: LeaseReference): void;

  async #forgetOnceAnswered(lease: LeaseReference, answer: Promise<boolean>): Promise<boolean> {
    const changed = await answer;
    this.forgetHandedOut(lease);
    return changed;
  }
}
