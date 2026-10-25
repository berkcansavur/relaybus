import type { InMemoryTransaction } from '../in-memory-transaction.js';

interface HeldLock {
  readonly owner: InMemoryTransaction;
  readonly released: Promise<void>;
}

/** The events and deliveries a transaction appended so far, which are not applied before it commits. */
export type PendingWrites = { eventIds: Set<string>; deliveryKeys: Set<string> };

/** Row-lock emulation: event ids and ordering keys written by open transactions. */
export class InMemoryRowLocks {
  readonly #locks = new Map<string, HeldLock>();
  readonly #writes = new WeakMap<InMemoryTransaction, PendingWrites>();

  /** Whether an open transaction holds `name`. */
  isHeld(name: string): boolean {
    return this.#locks.has(name);
  }

  /** The events and deliveries the transaction appended so far, which are not applied before it commits. */
  written(transaction: InMemoryTransaction): PendingWrites {
    let written = this.#writes.get(transaction);
    if (written === undefined) {
      written = { eventIds: new Set(), deliveryKeys: new Set() };
      this.#writes.set(transaction, written);
    }
    return written;
  }

  /**
   * Waits for other open transactions holding any of `names`, then holds them
   * until `transaction` finishes, the way a database holds row locks. Sorted
   * acquisition keeps two transactions from waiting on each other.
   */
  async lock(transaction: InMemoryTransaction, names: readonly string[]): Promise<void> {
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
}
