import { type ErrorInfo, type LeaseReference } from '@relaybus/core';
import type { PostgresParameter } from './executor.js';
import { handedOutKey, type PostgresStoreState } from './postgres-store-state.js';
import { parseLeaseToken, readCount, type LaneLease } from './row-mapping.js';

/** completion */
export class PostgresCompletion<TTransaction> {
  readonly #state: PostgresStoreState<TTransaction>;

  constructor(state: PostgresStoreState<TTransaction>) {
    this.#state = state;
  }

  /** Implements {@link PostgresEventStore.extendLease}. */
  extendLease(lease: LeaseReference, leaseExpiresAt: Date): Promise<boolean> {
    return this.#whenIssued(lease, (held) =>
      this.#changed(this.#state.statements.completion.extendLease, [
        lease.deliveryId,
        held.lane,
        held.epoch,
        held.claims,
        leaseExpiresAt.toISOString(),
      ]),
    );
  }

  /** Implements {@link PostgresEventStore.complete}. */
  async complete(lease: LeaseReference, now: Date, transaction?: TTransaction): Promise<boolean> {
    return (await this.#completeLeases([lease], now, transaction))[0]!;
  }

  /** Implements {@link PostgresEventStore.completeMany}. */
  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completeLeases(leases, now, undefined);
  }

  /** Implements {@link PostgresEventStore.reschedule}. */
  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return this.#settled(
      lease,
      this.#whenIssued(lease, (held) =>
        this.#changed(this.#state.statements.completion.reschedule, [
          lease.deliveryId,
          held.lane,
          held.epoch,
          held.claims,
          availableAt.toISOString(),
          JSON.stringify(error),
          now.toISOString(),
        ]),
      ),
    );
  }

  /** Implements {@link PostgresEventStore.bury}. */
  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return this.#settled(
      lease,
      this.#whenIssued(lease, (held) =>
        this.#changed(this.#state.statements.completion.bury, [
          lease.deliveryId,
          held.lane,
          held.epoch,
          held.claims,
          JSON.stringify(error),
          now.toISOString(),
        ]),
      ),
    );
  }

  /** Implements {@link PostgresEventStore.release}. */
  release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#settled(
      lease,
      this.#whenIssued(lease, (held) =>
        this.#changed(this.#state.statements.completion.release, [
          lease.deliveryId,
          held.lane,
          held.epoch,
          held.claims,
          now.toISOString(),
        ]),
      ),
    );
  }

  /** Forgets a handed-out lease once the store answered about it (any answer: the claim is over or fenced out). */
  async #settled(lease: LeaseReference, work: Promise<boolean>): Promise<boolean> {
    const result = await work;
    this.#state.handedOut.delete(handedOutKey(lease.deliveryId, lease.leaseToken));
    return result;
  }

  async #changed(sql: string, parameters: readonly PostgresParameter[]): Promise<boolean> {
    const rows = await this.#state.query(sql, parameters);
    return readCount(rows[0]!, 'count') > 0;
  }

  /** Runs `operation` for a lease this store issued; any other token cannot be held. */
  async #whenIssued(
    lease: LeaseReference,
    operation: (held: LaneLease) => Promise<boolean>,
  ): Promise<boolean> {
    const held = parseLeaseToken(lease.leaseToken);
    return held !== undefined && (await operation(held));
  }

  /**
   * Deletes the delivery of every lease that is still the current claim of its
   * delivery. Returns one flag per lease, in order. Results are matched to leases
   * by position, so two leases of one delivery (a stale and a current one) get
   * their own answers; a lease listed twice completes once, at its first position.
   */
  async #completeLeases(
    leases: readonly LeaseReference[],
    now: Date,
    transaction: TTransaction | undefined,
  ): Promise<boolean[]> {
    const issued: Array<{ ord: number; id: string } & LaneLease> = [];
    const seen = new Set<string>();
    leases.forEach((lease, ord) => {
      const held = parseLeaseToken(lease.leaseToken);
      const identity = `${lease.deliveryId}\u0000${lease.leaseToken}`;
      if (held !== undefined && !seen.has(identity)) {
        seen.add(identity);
        issued.push({ ord, id: lease.deliveryId, ...held });
      }
    });
    const completed = new Set<number>();
    if (issued.length > 0) {
      const rows = await this.#state.query(
        this.#state.statements.completion.complete,
        [JSON.stringify(issued), now.toISOString()],
        transaction,
      );
      for (const row of rows) {
        completed.add(readCount(row, 'ord'));
      }
    }
    if (transaction === undefined) {
      // Inside the caller's transaction the completion is only provisional: it may roll back and retry.
      for (const lease of leases) {
        this.#state.handedOut.delete(handedOutKey(lease.deliveryId, lease.leaseToken));
      }
    }
    return leases.map((_, ord) => completed.has(ord));
  }
}
