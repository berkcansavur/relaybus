import { type LeaseReference } from '@relaybus/core';
import {
  DeliveryLeaseIdentity,
  LeaseSettlement,
  type LeaseChange,
  type LeaseExtension,
} from '@relaybus/core/internal';
import type { PostgresParameter } from './executor.js';
import { type PostgresStoreState } from './postgres-store-state.js';
import { parseLeaseToken, readCount, type LaneLease } from './row-mapping.js';

/**
 * Ends and renews claims for {@link PostgresEventStore}: every statement is fenced by the lane
 * lease the token names (lane, epoch and claim generation).
 */
export class PostgresCompletion<TTransaction> extends LeaseSettlement<LaneLease> {
  readonly #state: PostgresStoreState<TTransaction>;

  constructor(state: PostgresStoreState<TTransaction>) {
    super();
    this.#state = state;
  }

  /** Implements {@link PostgresEventStore.complete}. */
  async complete(
    lease: LeaseReference,
    { now, transaction }: { readonly now: Date; readonly transaction?: TTransaction | undefined },
  ): Promise<boolean> {
    return (await this.#completeLeases([lease], { now, transaction }))[0]!;
  }

  /** Implements {@link PostgresEventStore.completeMany}. */
  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completeLeases(leases, { now, transaction: undefined });
  }

  /** A lease this store issued; any other token cannot be held. */
  protected findHeld(lease: LeaseReference): LaneLease | undefined {
    return parseLeaseToken(lease.leaseToken);
  }

  protected writeExtension(
    held: LaneLease,
    { lease, leaseExpiresAt }: LeaseExtension,
  ): Promise<boolean> {
    return this.#changed(this.#state.statements.completion.extendLease, [
      ...fenceOf(lease, held),
      leaseExpiresAt.toISOString(),
    ]);
  }

  protected writeChange(held: LaneLease, { lease, change }: LeaseChange): Promise<boolean> {
    const { completion } = this.#state.statements;
    const fence = fenceOf(lease, held);
    if (change.kind === 'reschedule') {
      return this.#changed(completion.reschedule, [
        ...fence,
        change.availableAt.toISOString(),
        JSON.stringify(change.error),
        change.now.toISOString(),
      ]);
    }
    if (change.kind === 'bury') {
      return this.#changed(completion.bury, [
        ...fence,
        JSON.stringify(change.error),
        change.now.toISOString(),
      ]);
    }
    return this.#changed(completion.release, [...fence, change.now.toISOString()]);
  }

  /** Forgets a handed-out lease once the store answered about it (any answer: the claim is over or fenced out). */
  protected forgetHandedOut(lease: LeaseReference): void {
    this.#state.handedOut.forget(lease);
  }

  async #changed(sql: string, parameters: readonly PostgresParameter[]): Promise<boolean> {
    const rows = await this.#state.query({ sql, parameters });
    return readCount(rows[0]!, 'count') > 0;
  }

  /**
   * Deletes the delivery of every lease that is still the current claim of its
   * delivery. Returns one flag per lease, in order. Results are matched to leases
   * by position, so two leases of one delivery (a stale and a current one) get
   * their own answers; a lease listed twice completes once, at its first position.
   */
  async #completeLeases(
    leases: readonly LeaseReference[],
    { now, transaction }: { readonly now: Date; readonly transaction: TTransaction | undefined },
  ): Promise<boolean[]> {
    const issued = issuedLeases(leases);
    const completed = new Set<number>();
    if (issued.length > 0) {
      const rows = await this.#state.query({
        sql: this.#state.statements.completion.complete,
        parameters: [JSON.stringify(issued), now.toISOString()],
        transaction,
      });
      for (const row of rows) {
        completed.add(readCount(row, 'ord'));
      }
    }
    if (transaction === undefined) {
      // Inside the caller's transaction the completion is only provisional: it may roll back and retry.
      for (const lease of leases) {
        this.#state.handedOut.forget(lease);
      }
    }
    return leases.map((_, ord) => completed.has(ord));
  }
}

/** The first parameters of every fenced statement: the delivery and the lane lease of its claim. */
function fenceOf(lease: LeaseReference, held: LaneLease): PostgresParameter[] {
  return [lease.deliveryId, held.lane, held.epoch, held.claims];
}

/** The leases this store can have issued, each once, with their position in `leases`. */
function issuedLeases(
  leases: readonly LeaseReference[],
): Array<{ ord: number; id: string } & LaneLease> {
  const issued: Array<{ ord: number; id: string } & LaneLease> = [];
  const seen = new Set<string>();
  leases.forEach((lease, ord) => {
    const held = parseLeaseToken(lease.leaseToken);
    const identity = DeliveryLeaseIdentity.of(lease.deliveryId, lease.leaseToken);
    if (held !== undefined && !seen.has(identity)) {
      seen.add(identity);
      issued.push({ ord, id: lease.deliveryId, ...held });
    }
  });
  return issued;
}
