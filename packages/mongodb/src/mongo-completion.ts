import type { ErrorInfo, LeaseReference } from '@relaybus/core';
import type { ClientSession, Filter, UpdateFilter } from 'mongodb';
import {
  toCompletedDeliveryDocument,
  toErrorInfo,
  type DeliveryDocument,
  type LaneDocument,
} from './documents.js';
import { formatLeaseToken, parseLeaseToken, type LaneLease } from './lanes.js';
import {
  assertInTransaction,
  atCount,
  leaseKey,
  runInTransaction,
  type MongoStoreState,
} from './mongo-store-state.js';

/** Fields that exist only while a delivery is unfinished. */
const FINISHED_FIELDS = { dueAt: '', lane: '' } as const;

/** A lease of this store, with the delivery it names. */
interface IssuedLease extends LaneLease {
  readonly deliveryId: string;
}

/** Ends claims: completion, retry, release, burial and lease renewal, all fenced by the lane lease. */
export class MongoCompletion {
  readonly #state: MongoStoreState;

  constructor(state: MongoStoreState) {
    this.#state = state;
  }

  /** Implements {@link MongoEventStore.extendLease}. */
  async extendLease(lease: LeaseReference, leaseExpiresAt: Date): Promise<boolean> {
    const held = parseLeaseToken(lease.leaseToken);
    if (held === undefined) {
      return false;
    }
    const deliveryId = String(lease.deliveryId);
    // One transaction: a stale lease must not extend the lane of a newer claim of the delivery.
    // A claim ending or starting meanwhile writes the delivery or the lane, so the two conflict.
    return runInTransaction(this.#state, async (session) => {
      const running = await this.#state.deliveries.findOne(ofClaim({ ...held, deliveryId }), {
        session,
        projection: { _id: 1 },
      });
      if (running === null) {
        return false;
      }
      const renewed = await this.#state.lanes.updateOne(
        { _id: held.laneId, epoch: held.epoch, inFlight: deliveryId },
        { $max: { leaseExpiresAt } },
        { session },
      );
      return renewed.matchedCount === 1;
    });
  }

  /** Implements {@link MongoEventStore.complete}. */
  async complete(
    lease: LeaseReference,
    { now, session }: { readonly now: Date; readonly session?: ClientSession | undefined },
  ): Promise<boolean> {
    if (session !== undefined) {
      assertInTransaction(session, 'complete');
    }
    return (await this.#completeLeases([lease], { now, session }))[0]!;
  }

  /** Implements {@link MongoEventStore.completeMany}. */
  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completeLeases(leases, { now, session: undefined });
  }

  /** Implements {@link MongoEventStore.reschedule}. */
  async reschedule(
    lease: LeaseReference,
    {
      availableAt,
      error,
      now,
    }: { readonly availableAt: Date; readonly error: ErrorInfo; readonly now: Date },
  ): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { availableAt, dueAt: availableAt, lastError: toErrorInfo(error), updatedAt: now },
      $inc: { attempts: 1, claims: 1 },
    });
  }

  /** Implements {@link MongoEventStore.release}. */
  async release(lease: LeaseReference, now: Date): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { availableAt: now, dueAt: now, updatedAt: now },
      $inc: { claims: 1 },
    });
  }

  /** Implements {@link MongoEventStore.bury}. */
  async bury(
    lease: LeaseReference,
    { error, now }: { readonly error: ErrorInfo; readonly now: Date },
  ): Promise<boolean> {
    return this.#leaveFlight(lease, {
      $set: { status: 'dead', lastError: toErrorInfo(error), updatedAt: now },
      $unset: FINISHED_FIELDS,
      $inc: { attempts: 1, claims: 1 },
    });
  }

  /**
   * Runs `operation`, which ends the claims of `leases` (completion, retry,
   * release, burial), and once it returned forgets exactly those leases as
   * handed out. When it throws, they stay registered: the caller may retry
   * with the same lease, which must still find its claim.
   */
  async #settle<T>(leases: readonly LeaseReference[], operation: () => Promise<T>): Promise<T> {
    const result = await operation();
    for (const lease of leases) {
      this.#state.handedOut.delete(leaseKey(String(lease.deliveryId), String(lease.leaseToken)));
    }
    return result;
  }

  /**
   * Deletes the delivery of every lease whose lane still has the lease's epoch
   * and lists the delivery, in one transaction (the caller's, when given).
   * Returns one flag per lease, in order; a lease listed twice completes once.
   */
  async #completeLeases(
    leases: readonly LeaseReference[],
    { now, session }: { readonly now: Date; readonly session: ClientSession | undefined },
  ): Promise<boolean[]> {
    // Inside the caller's transaction the completion may still be retried or rolled back, so it
    // stays registered until a later claim finds the delivery gone.
    return session === undefined
      ? this.#settle(leases, () => this.#completeIssued(leases, { now, session: undefined }))
      : this.#completeIssued(leases, { now, session });
  }

  async #completeIssued(
    leases: readonly LeaseReference[],
    { now, session }: { readonly now: Date; readonly session: ClientSession | undefined },
  ): Promise<boolean[]> {
    const parsed = leases.map((lease) => parseLeaseToken(lease.leaseToken));
    const issued = leases.flatMap((lease, index) =>
      parsed[index] === undefined
        ? []
        : [{ deliveryId: String(lease.deliveryId), ...parsed[index] }],
    );
    let completed = new Set<string>();
    if (issued.length > 0) {
      if (session !== undefined) {
        completed = await this.#deleteCompleted(issued, { now, session });
      } else if (this.#state.completionHistory) {
        completed = await runInTransaction(this.#state, (own) =>
          this.#deleteCompleted(issued, { now, session: own }),
        );
      } else {
        completed = await this.#deleteFenced(issued, now);
      }
    }
    return leases.map(
      (lease, index) =>
        parsed[index] !== undefined &&
        completed.delete(leaseIdentity({ deliveryId: String(lease.deliveryId), ...parsed[index] })),
    );
  }

  /**
   * Completion without a transaction, used when no history record has to be
   * written with the deletion. The deliveries' claim generations are read
   * first, then the lanes, and a delivery is deleted only if it still has the
   * lease's generation. A takeover moves the generation of every delivery it
   * takes from the previous owner on before it reads anything, and so do a
   * retry, a release and a burial, so:
   * - a takeover before the delivery read shows up in the lane read (new epoch);
   * - a takeover between the two reads shows up in the lane read as well;
   * - a takeover, retry, release or burial after the lane read changed the
   *   generation, so the delete matches nothing.
   * A completion and a takeover (or the end of the claim) therefore never both succeed. Two concurrent
   * completions of the same lease may both report true (the dispatcher never
   * issues them); one call with a lease listed twice reports it once.
   */
  async #deleteFenced(issued: readonly IssuedLease[], now: Date): Promise<Set<string>> {
    const ids = [...new Set(issued.map((lease) => lease.deliveryId))];
    const documents = await this.#state.deliveries
      .find({ _id: { $in: ids }, status: 'pending' }, { projection: { claims: 1, eventId: 1 } })
      .toArray();
    const lanes = new Map(
      (
        await this.#state.lanes
          .find(
            { _id: { $in: [...new Set(issued.map((lease) => lease.laneId))] } },
            { projection: { epoch: 1, inFlight: 1 } },
          )
          .toArray()
      ).map((lane) => [lane._id, lane]),
    );
    const fenced = fencedLeases(issued, { lanes, documents });
    if (fenced.length === 0) {
      return new Set();
    }
    await this.#markEventsCompleted(fenced, { documents, now });
    const deleted = await this.#state.deliveries.deleteMany({ $or: fenced.map(ofClaim) });
    // Each branch of the delete matches at most one document of exactly that claim, so all of
    // them deleting proves every completion. When fewer did, a claim ended or changed hands after
    // it was read, and nothing tells which of them this call deleted: a missing document may as
    // well have been completed by the claim that took it over. Report none, never a guess.
    return deleted.deletedCount === fenced.length
      ? new Set(fenced.map((lease) => leaseIdentity(lease)))
      : new Set();
  }

  /**
   * The transactional part of a completion. The lanes are only read: the
   * conflict with a takeover comes from the deleted delivery documents, which a
   * takeover writes before it reads them (see `MongoLaneLeases.takeLanes`).
   * So whichever of the two commits second either fails with a write conflict
   * and is retried against the new state, or waits for the first.
   */
  async #deleteCompleted(
    issued: readonly IssuedLease[],
    { now, session }: { readonly now: Date; readonly session: ClientSession },
  ): Promise<Set<string>> {
    const lanes = new Map(
      (
        await this.#state.lanes
          .find(
            { _id: { $in: [...new Set(issued.map((lease) => lease.laneId))] } },
            { session, projection: { epoch: 1, inFlight: 1 } },
          )
          .toArray()
      ).map((lane) => [lane._id, lane]),
    );
    const documents = await this.#state.deliveries
      .find(
        { _id: { $in: [...new Set(issued.map((lease) => lease.deliveryId))] }, status: 'pending' },
        { session },
      )
      .toArray();
    const fenced = fencedLeases(issued, { lanes, documents });
    if (fenced.length === 0) {
      return new Set();
    }
    await this.#markEventsCompleted(fenced, { documents, now, session });
    await this.#state.deliveries.deleteMany({ $or: fenced.map(ofClaim) }, { session });
    if (this.#state.completionHistory) {
      const completedIds = new Set(fenced.map((lease) => lease.deliveryId));
      await this.#state.completions.insertMany(
        documents
          .filter((document) => completedIds.has(document._id))
          .map((document) => toCompletedDeliveryDocument(document, now)),
        { session },
      );
    }
    return new Set(fenced.map((lease) => leaseIdentity(lease)));
  }

  /**
   * Records `now` as the latest completion of the events of the fenced
   * deliveries (`$max`, so an earlier clock never moves it back). Written
   * before the deletion, it can only be later than the real last completion
   * (when the deletion then fails), which keeps the event longer, never shorter.
   * Inside a transaction it is part of it: the transaction may hold the event
   * already (a transactional handler that appended to its own event), and a
   * write from outside would wait for it forever. Deliveries of one event that
   * complete in concurrent transactions (fan-out to transactional handlers)
   * therefore conflict on the event document, and one of them retries.
   */
  async #markEventsCompleted(
    fenced: readonly IssuedLease[],
    {
      documents,
      now,
      session,
    }: {
      readonly documents: readonly Pick<DeliveryDocument, '_id' | 'eventId'>[];
      readonly now: Date;
      readonly session?: ClientSession | undefined;
    },
  ): Promise<void> {
    const completedIds = new Set(fenced.map((lease) => lease.deliveryId));
    const eventIds = [
      ...new Set(
        documents
          .filter((document) => completedIds.has(document._id))
          .map((document) => document.eventId),
      ),
    ];
    await this.#state.events.updateMany(
      { _id: { $in: eventIds } },
      { $max: { completedAt: now } },
      session === undefined ? {} : { session },
    );
  }

  /**
   * Applies `update` to a running delivery and takes it out of its lane's
   * in-flight list, if the lane still has the lease's epoch. One transaction:
   * the lane write fences the change against a takeover.
   */
  async #leaveFlight(
    lease: LeaseReference,
    update: UpdateFilter<DeliveryDocument>,
  ): Promise<boolean> {
    const held = parseLeaseToken(lease.leaseToken);
    if (held === undefined) {
      return false;
    }
    const deliveryId = String(lease.deliveryId);
    const ofThisClaim = ofClaim({ ...held, deliveryId });
    return this.#settle([lease], () =>
      this.#leaveFlightNow(held, { deliveryId, ofThisClaim, update }),
    );
  }

  async #leaveFlightNow(
    held: LaneLease,
    {
      deliveryId,
      ofThisClaim,
      update,
    }: {
      readonly deliveryId: string;
      readonly ofThisClaim: Filter<DeliveryDocument>;
      readonly update: UpdateFilter<DeliveryDocument>;
    },
  ): Promise<boolean> {
    return runInTransaction(this.#state, async (session) => {
      // The claim generation is checked before the lane is touched: a later claim of the same
      // delivery in the same lane epoch is listed under the same id and must stay listed.
      if (
        (await this.#state.deliveries.findOne(ofThisClaim, { session, projection: { _id: 1 } })) ===
        null
      ) {
        return false;
      }
      const fence = await this.#state.lanes.updateOne(
        { _id: held.laneId, epoch: held.epoch, inFlight: deliveryId },
        { $pull: { inFlight: deliveryId } },
        { session },
      );
      if (fence.matchedCount === 0) {
        return false;
      }
      const changed = await this.#state.deliveries.updateOne(ofThisClaim, update, { session });
      return changed.matchedCount === 1;
    });
  }
}

/** The leases whose lane still has their epoch and lists the delivery, and whose delivery is still in their claim. */
function fencedLeases(
  issued: readonly IssuedLease[],
  {
    lanes,
    documents,
  }: {
    readonly lanes: ReadonlyMap<number, Pick<LaneDocument, 'epoch' | 'inFlight'>>;
    readonly documents: readonly Pick<DeliveryDocument, '_id' | 'claims'>[];
  },
): IssuedLease[] {
  const claimsById = new Map(documents.map((document) => [document._id, document.claims ?? 0]));
  const fenced = new Map<string, IssuedLease>();
  for (const lease of issued) {
    const lane = lanes.get(lease.laneId)!;
    if (
      claimsById.get(lease.deliveryId) === lease.claims &&
      lane.epoch === lease.epoch &&
      lane.inFlight.includes(lease.deliveryId)
    ) {
      // A lease listed twice completes once.
      fenced.set(leaseIdentity(lease), lease);
    }
  }
  return [...fenced.values()];
}

/** The delivery document of exactly this claim, still pending. */
function ofClaim(lease: IssuedLease): Filter<DeliveryDocument> {
  return { _id: lease.deliveryId, status: 'pending', claims: atCount(lease.claims) };
}

function leaseIdentity(lease: IssuedLease): string {
  return `${lease.deliveryId}\u0000${formatLeaseToken(lease.laneId, { epoch: lease.epoch, claims: lease.claims })}`;
}
