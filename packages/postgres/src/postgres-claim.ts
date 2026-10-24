import { type ClaimRequest, type ClaimedDelivery } from '@relaybus/core';
import { ReconcileFailedError } from './errors.js';
import type { PostgresParameter, PostgresRow } from './executor.js';
import { handedOutKey, type PostgresStoreState } from './postgres-store-state.js';
import { mapClaimedDelivery, parseLeaseToken, readCount } from './row-mapping.js';

/** Lanes a claim takes in its first round; every further round takes twice as many. */
const INITIAL_LANES_PER_ROUND = 4;

/** Attempts of the held-lane claim before a stale statement is given up for this call (a later claim finds the work). */
const MAXIMUM_STALE_CLAIM_ATTEMPTS = 3;

/** claims */
export class PostgresClaims<TTransaction> {
  readonly #state: PostgresStoreState<TTransaction>;

  constructor(state: PostgresStoreState<TTransaction>) {
    this.#state = state;
  }

  /** Implements {@link PostgresEventStore.claim}. */
  claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || !(request.limit > 0)) {
      return Promise.resolve([]);
    }
    return this.#serialized(request.leaseOwner, () => this.#claim(request));
  }

  /**
   * Runs `work` after the earlier claims of the same owner on this store settled. The
   * bookkeeping of what was handed out and the reconciliation of a failed claim need
   * one claim at a time per owner (the dispatcher claims one batch at a time anyway;
   * `runOnce` next to a running dispatcher is what this guards against).
   */
  #serialized<T>(owner: string, work: () => Promise<T>): Promise<T> {
    const previous = this.#state.ownerQueues.get(owner) ?? Promise.resolve();
    const run = previous.then(work, work);
    const tail = run.then(
      () => undefined,
      () => undefined,
    );
    this.#state.ownerQueues.set(owner, tail);
    void tail.then(() => {
      if (this.#state.ownerQueues.get(owner) === tail) {
        this.#state.ownerQueues.delete(owner);
      }
    });
    return run;
  }

  async #claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (
      this.#state.unreconciled.has(request.leaseOwner) &&
      !(await this.#reconcile(request.leaseOwner))
    ) {
      // The generation moved on and the lanes were not told: claiming now would take the owner's live lanes for foreign ones.
      throw new ReconcileFailedError(
        `[PostgresEventStore](claim) Giving back unreturned deliveries still fails, so no claim is made | owner=${request.leaseOwner}`,
      );
    }
    // An owner whose last claim was filled from the lanes it held is draining a backlog there: those
    // lanes are asked first.
    const heldFirst = this.#state.drainingOwners.has(request.leaseOwner);
    let rows: readonly PostgresRow[];
    try {
      rows = await this.#claimInOneStatement(
        request,
        heldFirst
          ? this.#state.statements.lanes.claimHeldLanes
          : this.#state.statements.lanes.claimLanes,
      );
    } catch (error) {
      await this.#reconcile(request.leaseOwner);
      throw error;
    }
    const heldLanes = new Set(JSON.parse(rows[0]!['examined_lanes'] as string) as number[]);
    const claimed = this.#takeClaimed(rows, request.leaseOwner);
    if (heldFirst && claimed.length < request.limit && rows[0]!['stale'] !== true) {
      try {
        rows = await this.#claimInOneStatement(
          { ...request, limit: request.limit - claimed.length },
          this.#state.statements.lanes.claimLanes,
        );
      } catch (error) {
        await this.#reconcile(request.leaseOwner);
        if (claimed.length === 0) {
          this.#state.drainingOwners.delete(request.leaseOwner);
          throw error;
        }
        this.#state.logger.error(
          `[PostgresEventStore](claim) Claiming from free lanes failed, returning the deliveries already claimed | claimed=${claimed.length} | error=${error instanceof Error ? error.message : String(error)}`,
        );
        return this.#handOutInDueOrder(claimed, request, heldLanes);
      }
      claimed.push(...this.#takeClaimed(rows, request.leaseOwner));
    }
    if (
      claimed.length < request.limit &&
      (rows[0]!['takeover_waiting'] === true ||
        rows[0]!['unexamined_free_lanes'] === true ||
        rows[0]!['stale'] === true)
    ) {
      try {
        const more = await this.#claimMore(request, claimed.length);
        this.#handOut(more, request.leaseOwner);
        claimed.push(...more);
      } catch (error) {
        await this.#reconcile(request.leaseOwner);
        if (claimed.length === 0) {
          throw error;
        }
        this.#state.logger.error(
          `[PostgresEventStore](claim) Taking more lanes failed, returning the deliveries already claimed | claimed=${claimed.length} | error=${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
    return this.#handOutInDueOrder(claimed, request, heldLanes);
  }

  /** The deliveries a claim statement picked, registered as handed out to `leaseOwner`. */
  #takeClaimed(rows: readonly PostgresRow[], leaseOwner: string): PostgresRow[] {
    const claimed = rows.filter((row) => row['delivery_id'] !== null);
    this.#forgetFinished(rows[0]!, leaseOwner, claimed);
    this.#handOut(claimed, leaseOwner);
    return claimed;
  }

  #handOutInDueOrder(
    claimed: PostgresRow[],
    request: ClaimRequest,
    heldLanes: ReadonlySet<number>,
  ): ClaimedDelivery[] {
    if (
      claimed.length >= request.limit &&
      claimed.every((row) => heldLanes.has(readCount(row, 'lane')))
    ) {
      this.#state.drainingOwners.add(request.leaseOwner);
    } else {
      this.#state.drainingOwners.delete(request.leaseOwner);
    }
    // The steps pick from different lanes; hand the batch out in due order, like a single query would.
    claimed.sort(compareDue);
    return claimed.map((row) => mapClaimedDelivery(row, request.leaseExpiresAt));
  }

  /**
   * Runs `statement` (`LaneStatements.claimLanes` or its held-lane form), again when it reports that it waited for a lane lock and
   * read deliveries with an older snapshot than the lane it got. A held lane is only locked by an
   * operation of its own owner (another claim, a reschedule), so a few attempts are enough.
   */
  async #claimInOneStatement(
    request: ClaimRequest,
    statement: string,
  ): Promise<readonly PostgresRow[]> {
    const parameters = [
      request.now.toISOString(),
      JSON.stringify(request.handlers),
      request.limit,
      request.leaseOwner,
      request.leaseExpiresAt.toISOString(),
      this.#generation(request.leaseOwner).toString(),
    ];
    let rows = await this.#state.query(statement, parameters);
    for (
      let attempt = 1;
      attempt < MAXIMUM_STALE_CLAIM_ATTEMPTS && rows[0]!['stale'] === true;
      attempt += 1
    ) {
      rows = await this.#state.query(statement, parameters);
    }
    return rows;
  }

  /**
   * Second step of {@link PostgresEventStore.claim}, in one transaction: takes free lanes a few at a
   * time, lanes to take over included (their lost attempts are charged first), and records the batch.
   * `alreadyClaimed` deliveries of the first step are in flight, so they are not picked again.
   */
  async #claimMore(request: ClaimRequest, alreadyClaimed: number): Promise<PostgresRow[]> {
    const now = request.now.toISOString();
    const leaseExpiresAt = request.leaseExpiresAt.toISOString();
    const handlers = JSON.stringify(request.handlers);
    // Taken before the transaction starts: a transaction that reaches the server late keeps its generation.
    const generation = this.#generation(request.leaseOwner).toString();
    return this.#state.executor.transaction(async (transaction) => {
      const claimed: PostgresRow[] = [];
      const remaining = (): number => request.limit - alreadyClaimed - claimed.length;
      const takenLanes = new Set<number>();
      for (let laneCount = INITIAL_LANES_PER_ROUND; remaining() > 0;) {
        const taken = await this.#state.query(
          this.#state.statements.lanes.takeFreeLanes,
          [now, handlers, laneCount, request.leaseOwner, leaseExpiresAt, generation],
          transaction,
        );
        if (taken.length === 0) {
          break;
        }
        const lanes = taken.map((row) => readCount(row, 'lane'));
        for (const lane of lanes) {
          takenLanes.add(lane);
        }
        claimed.push(
          ...(await this.#pickDeliveries(
            [now, handlers, remaining(), JSON.stringify(lanes)],
            transaction,
          )),
        );
        laneCount *= 2;
      }
      await this.#recordClaim(claimed, takenLanes, request, transaction);
      return claimed;
    });
  }

  /**
   * The generation of the owner's claim statements. It only grows, also across restarts (it starts at the
   * clock), and a reconciliation moves it up, which fences off every earlier statement still in flight.
   */
  #generation(owner: string, advance = false): bigint {
    const current = this.#state.generations.get(owner) ?? BigInt(Date.now());
    const next = advance
      ? current + 1n > BigInt(Date.now())
        ? current + 1n
        : BigInt(Date.now())
      : current;
    this.#state.generations.set(owner, next);
    return next;
  }

  /** Remembers the claims handed to the caller of `claim` until they are settled. */
  #handOut(rows: readonly PostgresRow[], leaseOwner: string): void {
    for (const row of rows) {
      const claim = mapClaimedDelivery(row, new Date(0));
      this.#state.handedOut.set(handedOutKey(claim.deliveryId, claim.leaseToken), {
        owner: leaseOwner,
        deliveryId: claim.deliveryId,
        ...parseLeaseToken(claim.leaseToken)!,
      });
    }
  }

  /**
   * Drops the registrations of the owner's held lanes whose delivery no longer runs there
   * (completed inside the caller's transaction, deleted, or listed no more). Registrations of
   * lanes this claim did not see stay: their tokens may still be valid.
   */
  #forgetFinished(row: PostgresRow, leaseOwner: string, claimed: readonly PostgresRow[]): void {
    // Lanes the statement freed are examined too: nothing runs there any more.
    const heldLanes = new Set(JSON.parse(row['examined_lanes'] as string) as number[]);
    const running = new Set([
      ...(JSON.parse(row['running_ids'] as string) as string[]),
      ...claimed.map((picked) => picked['delivery_id'] as string),
    ]);
    for (const [key, entry] of this.#state.handedOut) {
      if (
        entry.owner === leaseOwner &&
        heldLanes.has(entry.lane) &&
        !running.has(entry.deliveryId)
      ) {
        this.#state.handedOut.delete(key);
      }
    }
  }

  /**
   * After a claim failed, possibly after the database recorded its batch: gives back the recorded
   * deliveries that were never handed out, keeping exactly the claims handed out and not settled.
   * A failure is remembered and retried before the owner's next claim: other deliveries keep
   * renewing the lane, so an unreturned one would otherwise never be recovered.
   */
  async #reconcile(leaseOwner: string): Promise<boolean> {
    // Claim statements of the generation before are fenced off by the new one.
    const generation = this.#generation(leaseOwner, true);
    const handedOut = [...this.#state.handedOut.values()]
      .filter((entry) => entry.owner === leaseOwner)
      .map(({ deliveryId, lane, epoch, claims }) => ({ id: deliveryId, lane, epoch, claims }));
    try {
      await this.#state.query(this.#state.statements.claim.reconcileClaims, [
        leaseOwner,
        JSON.stringify(handedOut),
        generation.toString(),
      ]);
      this.#state.unreconciled.delete(leaseOwner);
      return true;
    } catch (error) {
      this.#state.unreconciled.add(leaseOwner);
      this.#state.logger.error(
        `[PostgresEventStore](reconcile) Giving back unreturned deliveries failed, it is retried before the next claim | owner=${leaseOwner} | error=${error instanceof Error ? error.message : String(error)}`,
      );
      return false;
    }
  }

  /** Fast claim, then the slow one when the fast window was full of blocked followers. */
  async #pickDeliveries(
    parameters: readonly PostgresParameter[],
    transaction: TTransaction,
  ): Promise<PostgresRow[]> {
    const limit = parameters[2] as number;
    const fastRows = await this.#state.query(
      this.#state.statements.claim.claim,
      parameters,
      transaction,
    );
    const picked = fastRows.filter((row) => row['delivery_id'] !== null);
    if (picked.length < limit && fastRows[0]!['window_saturated'] === true) {
      const slowRows = await this.#state.query(
        this.#state.statements.claim.claimBehindBlockedKeys,
        parameters,
        transaction,
      );
      // Nothing was written, so the slow statement sees the fast statement's picks again.
      // Both pick at most the head of a key, so the union is still one delivery per key.
      const pickedIds = new Set(picked.map((row) => row['delivery_id']));
      picked.push(
        ...slowRows.filter(
          (row) => row['delivery_id'] !== null && !pickedIds.has(row['delivery_id']),
        ),
      );
      // The two statements read at different moments: a dead letter requeued in between can put a key's
      // predecessor in one result and its successor in the other. Only the lowest of a key may run.
      const seenKeys = new Set<string>();
      const kept = [...picked]
        .sort((left, right) => compareSequence(left, right))
        .filter((row) => {
          if (row['delivery_key'] === null) {
            return true;
          }
          const group = `${row['handler'] as string}\u0000${row['delivery_key'] as string}`;
          return !seenKeys.has(group) && Boolean(seenKeys.add(group));
        });
      kept.sort(compareDue);
      return kept.slice(0, limit);
    }
    return picked;
  }

  /** Writes the picked ids into their lane rows and frees the lanes that picked nothing. */
  async #recordClaim(
    claimed: readonly PostgresRow[],
    lanes: ReadonlySet<number>,
    request: ClaimRequest,
    transaction: TTransaction,
  ): Promise<void> {
    const idsByLane = new Map<number, string[]>();
    for (const row of claimed) {
      const lane = readCount(row, 'lane');
      idsByLane.set(lane, [...(idsByLane.get(lane) ?? []), row['delivery_id'] as string]);
    }
    const idleLanes = [...lanes].filter((lane) => !idsByLane.has(lane));
    if (idsByLane.size === 0 && idleLanes.length === 0) {
      return;
    }
    await this.#state.query(
      this.#state.statements.claim.recordClaim,
      [
        JSON.stringify([...idsByLane].map(([lane, ids]) => ({ lane, ids }))),
        JSON.stringify(idleLanes),
        request.leaseOwner,
        request.leaseExpiresAt.toISOString(),
      ],
      transaction,
    );
  }
}

function compareSequence(left: PostgresRow, right: PostgresRow): number {
  const first = BigInt((left['sequence'] as string | null) ?? 0);
  const second = BigInt((right['sequence'] as string | null) ?? 0);
  return Number(first > second) - Number(first < second);
}

function compareDue(left: PostgresRow, right: PostgresRow): number {
  return (
    compareText(left['due_at'] as string, right['due_at'] as string) ||
    compareText(left['delivery_id'] as string, right['delivery_id'] as string)
  );
}

function compareText(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}
