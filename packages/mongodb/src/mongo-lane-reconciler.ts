import { DeliveryLeaseIdentity } from '@relaybus/core/internal';
import type { ClientSession } from 'mongodb';
import type { LaneDocument } from './documents.js';
import { formatLeaseToken } from './lanes.js';
import { runInTransaction, type MongoStoreState } from './mongo-store-state.js';

/** One held lane with the listed deliveries this instance did not hand out. */
type UnclaimedLane = { readonly lane: LaneDocument; readonly ids: string[] };

/** Takes the deliveries nobody runs out of the lanes an owner holds after a failed claim. */
export class MongoLaneReconciler {
  readonly #state: MongoStoreState;

  constructor(state: MongoStoreState) {
    this.#state = state;
  }

  /**
   * Takes every delivery this instance does not run out of the lanes `owner`
   * holds on it: every listed delivery whose current claim (lane epoch and
   * claim generation now) was not handed out by this instance, or was settled.
   * One transaction, and the lanes are written first under their epoch and
   * holder: a worker taking a lane over meanwhile conflicts with it, so its
   * claim is never touched. The claim generation of every delivery taken out
   * moves on, so no lease of those claims (a completion that failed and is
   * retried later, say) can ever match a later claim of the same delivery.
   * Registrations of this owner that are not the current claim of a listed
   * delivery are dropped.
   */
  async reconcile(owner: string): Promise<void> {
    await runInTransaction(this.#state, async (session) => {
      const held = await this.#state.lanes
        .find({ owner, holder: this.#state.instanceId }, { session })
        .toArray();
      const claimsById = await this.#claimGenerations(
        held.flatMap((lane) => lane.inFlight),
        session,
      );
      const { unclaimed, current } = this.#splitUnclaimed(held, claimsById);
      this.#state.handedOut.forgetWhere(
        (entry, identity) => entry.owner === owner && !current.has(identity),
      );
      if (held.length === 0) {
        return;
      }
      await this.#takeOut(unclaimed, session);
    });
  }

  /** The claim generation now of every listed delivery that is still pending. */
  async #claimGenerations(
    listed: readonly string[],
    session: ClientSession,
  ): Promise<Map<string, number>> {
    return new Map(
      (
        await this.#state.deliveries
          .find({ _id: { $in: listed }, status: 'pending' }, { session, projection: { claims: 1 } })
          .toArray()
      ).map((document) => [document._id, document.claims ?? 0]),
    );
  }

  /**
   * Per held lane, the listed deliveries this instance did not hand out under their current claim,
   * and the identities of all current claims.
   */
  #splitUnclaimed(
    held: readonly LaneDocument[],
    claimsById: ReadonlyMap<string, number>,
  ): { readonly unclaimed: UnclaimedLane[]; readonly current: Set<string> } {
    const current = new Set<string>();
    const unclaimed = held.map((lane) => ({
      lane,
      ids: lane.inFlight.filter((id) => {
        const key = DeliveryLeaseIdentity.of(
          id,
          formatLeaseToken(lane._id, { epoch: lane.epoch, claims: claimsById.get(id) ?? 0 }),
        );
        current.add(key);
        return !this.#state.handedOut.has(key);
      }),
    }));
    return { unclaimed, current };
  }

  /** Takes the unclaimed deliveries out of their lanes, then moves their claim generation on. */
  async #takeOut(unclaimed: readonly UnclaimedLane[], session: ClientSession): Promise<void> {
    // Every lane gets a new revision, also one with nothing to take out: a record of the failed
    // claim that is still on its way to the server then matches nothing when it arrives.
    await this.#state.lanes.bulkWrite(
      unclaimed.map(({ lane, ids }) => ({
        updateOne: {
          filter: { _id: lane._id, epoch: lane.epoch, holder: this.#state.instanceId },
          update: { $pull: { inFlight: { $in: ids } }, $inc: { revision: 1 } },
        },
      })),
      { session, ordered: false },
    );
    await this.#state.deliveries.updateMany(
      { _id: { $in: unclaimed.flatMap((entry) => entry.ids) }, status: 'pending' },
      { $inc: { claims: 1 } },
      { session },
    );
  }
}
