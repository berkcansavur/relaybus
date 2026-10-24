import type { ClaimRequest } from '@relaybus/core';
import type { AnyBulkWriteOperation } from 'mongodb';
import type { DeliveryDocument, LaneDocument } from './documents.js';
import type { MongoQueries } from './mongo-queries.js';
import { atCount, type MongoStoreState, type ScopeLane } from './mongo-store-state.js';

/** Takes, records and hands back the lanes a claim works in. */
export class MongoLaneLeases {
  readonly #state: MongoStoreState;
  readonly #queries: MongoQueries;

  constructor(state: MongoStoreState, collaborators: { readonly queries: MongoQueries }) {
    this.#state = state;
    this.#queries = collaborators.queries;
  }

  /**
   * Up to `laneCount` lanes nobody holds under a live lease that have a due,
   * ready delivery of the requested handlers, the lane with the oldest such
   * delivery first, leaving out `passed`. One round trip: the oldest delivery
   * of each lane is one index probe on the server. A lane whose due deliveries
   * all wait behind an unfinished predecessor is taken at most once: the claim
   * that hands it back parks them (see `ClaimScan.waitingBacklogs`).
   */
  async findFreeLanes(
    request: ClaimRequest,
    laneCount: number,
    passed: readonly number[],
  ): Promise<number[]> {
    const lanes = await this.#state.lanes
      .aggregate<{ _id: number }>([
        {
          $match: {
            _id: { $nin: passed },
            $or: [{ leaseExpiresAt: null }, { leaseExpiresAt: { $lte: request.now } }],
          },
        },
        {
          $lookup: {
            from: this.#state.deliveries.collectionName,
            localField: '_id',
            foreignField: 'laneId',
            pipeline: [
              {
                $match: {
                  handler: { $in: [...request.handlers] },
                  lane: 'ready',
                  dueAt: { $lte: request.now },
                },
              },
              { $sort: { dueAt: 1, _id: 1 } },
              { $limit: 1 },
              { $project: { dueAt: 1 } },
            ],
            as: 'oldest',
          },
        },
        { $unwind: '$oldest' },
        { $sort: { 'oldest.dueAt': 1, 'oldest._id': 1 } },
        { $limit: laneCount },
        { $project: { _id: 1 } },
      ])
      .toArray();
    return lanes.map((lane) => lane._id);
  }

  /**
   * Takes the lanes with one conditional write each, run in parallel; a lane
   * taken by someone else meanwhile is skipped. The same write moves the
   * previous owner's in-flight list to `lost`, which is charged before any
   * delivery of the lanes is read (see {@link MongoLaneLeases.chargeLost}).
   */
  async takeLanes(laneIds: readonly number[], request: ClaimRequest): Promise<ScopeLane[]> {
    const after = await Promise.all(
      laneIds.map((laneId) =>
        this.#state.lanes.findOneAndUpdate(
          {
            _id: laneId,
            $or: [{ leaseExpiresAt: null }, { leaseExpiresAt: { $lte: request.now } }],
          },
          [
            {
              $set: {
                epoch: { $add: ['$epoch', 1] },
                owner: { $literal: request.leaseOwner },
                holder: { $literal: this.#state.instanceId },
                leaseExpiresAt: { $literal: request.leaseExpiresAt },
                // Each lost delivery keeps the epoch it was claimed under: the key of its charge.
                lost: {
                  $setUnion: [
                    { $ifNull: ['$lost', []] },
                    { $map: { input: '$inFlight', in: { id: '$$this', epoch: '$epoch' } } },
                  ],
                },
                inFlight: { $literal: [] },
              },
            },
          ],
          { returnDocument: 'after' },
        ),
      ),
    );
    const taken = after.filter((lane) => lane !== null);
    await this.chargeLost(taken);
    return taken.map((lane) => ({
      laneId: lane._id,
      epoch: lane.epoch,
      listed: lane.inFlight,
      revision: lane.revision ?? 0,
    }));
  }

  /**
   * Charges one attempt to every pending delivery listed in the `lost` lists of
   * `lanes` (deliveries whose lane was taken while they ran), then clears those
   * lists under the lanes' current epochs. The charge is a write on each lost
   * delivery that comes before any read of the lane's deliveries: a completion
   * of the previous owner deletes the same documents, so the two conflict and
   * cannot both succeed. The charge also moves the claim generation on, which
   * fences every lease of the lost claim, and records the lost claim
   * (`lostAt` = lane and epoch it was claimed under): a holder that charges the
   * list again after a failure before the clear skips deliveries already
   * charged for that claim.
   */
  async chargeLost(lanes: readonly LaneDocument[]): Promise<void> {
    const losing = lanes.filter((lane) => lane.lost.length > 0);
    if (losing.length === 0) {
      return;
    }
    // Oldest lost claims first: a delivery's lane never changes and its epochs only grow, so
    // `lostAt` (the epoch of the last lost claim charged) only moves forward, and a charge of a
    // claim at or before it, repeated or delayed, matches nothing.
    const lost = losing
      .flatMap((lane) => lane.lost)
      .sort((left, right) => left.epoch - right.epoch);
    await this.#state.deliveries.bulkWrite(
      lost.map(({ id, epoch }) => ({
        updateOne: {
          filter: { _id: id, status: 'pending', lostAt: { $not: { $gte: epoch } } },
          update: { $inc: { attempts: 1, claims: 1 }, $set: { lostAt: epoch } },
        },
      })),
      { ordered: true },
    );
    await this.#state.lanes.bulkWrite(
      losing.map((lane) => ({
        updateOne: { filter: { _id: lane._id, epoch: lane.epoch }, update: { $set: { lost: [] } } },
      })),
      { ordered: false },
    );
  }

  /**
   * Records the picks in their lanes, one conditional write per lane and one
   * round trip for all of them, and frees the lanes that yielded nothing and run
   * nothing. A picked lane lists its running deliveries plus the new ones (ids
   * whose documents are gone are pruned) and its lease reaches
   * `request.leaseExpiresAt`. Returns the lanes whose picks were recorded: every
   * write is conditional on the claim's epoch, owner and store instance, so a
   * lane that changed hands, or was handed back by `relinquish`, in the meantime
   * keeps its deliveries for whoever takes it next.
   */
  async recordClaim(
    scope: ReadonlyMap<number, ScopeLane>,
    picked: readonly DeliveryDocument[],
    runningIds: ReadonlySet<string>,
    request: ClaimRequest,
  ): Promise<{ readonly recorded: Set<number>; readonly handedBack: Set<number> }> {
    const picksByLane = new Map<number, string[]>();
    const handedBack = new Set<number>();
    for (const document of picked) {
      picksByLane.set(document.laneId, [...(picksByLane.get(document.laneId) ?? []), document._id]);
    }
    const operations: Array<AnyBulkWriteOperation<LaneDocument>> = [];
    for (const lane of scope.values()) {
      const gone = lane.listed.filter((id) => !runningIds.has(id));
      const ids = picksByLane.get(lane.laneId);
      if (ids !== undefined) {
        operations.push({
          updateOne: {
            filter: {
              _id: lane.laneId,
              epoch: lane.epoch,
              owner: request.leaseOwner,
              holder: this.#state.instanceId,
              // A record that reaches the server late (after its claim failed and reconciled)
              // finds a newer revision and changes nothing.
              revision: atCount(lane.revision),
            },
            update: [
              {
                $set: {
                  inFlight: {
                    $concatArrays: [
                      {
                        $filter: {
                          input: '$inFlight',
                          cond: { $not: [{ $in: ['$$this', { $literal: gone }] }] },
                        },
                      },
                      { $literal: ids },
                    ],
                  },
                  leaseExpiresAt: { $max: ['$leaseExpiresAt', request.leaseExpiresAt] },
                  revision: { $add: [{ $ifNull: ['$revision', 0] }, 1] },
                },
              },
            ],
          },
        });
      } else if (gone.length === lane.listed.length) {
        operations.push(freeLane(lane.laneId, lane.epoch, lane.revision, gone));
        handedBack.add(lane.laneId);
      }
    }
    if (operations.length === 0) {
      return { recorded: new Set(), handedBack };
    }
    const result = await this.#state.lanes.bulkWrite(operations, { ordered: false });
    if (result.matchedCount === operations.length) {
      return { recorded: new Set(picksByLane.keys()), handedBack };
    }
    const current = await this.#state.lanes
      .find(
        { _id: { $in: [...picksByLane.keys()] } },
        { projection: { epoch: 1, owner: 1, holder: 1, revision: 1 } },
      )
      .toArray();
    const recorded = new Set(
      current
        .filter(
          (lane) =>
            lane.epoch === scope.get(lane._id)!.epoch &&
            lane.revision === scope.get(lane._id)!.revision + 1 &&
            lane.owner === request.leaseOwner &&
            lane.holder === this.#state.instanceId,
        )
        .map((lane) => lane._id),
    );
    return { recorded, handedBack };
  }

  /** Implements {@link MongoEventStore.relinquish}. */
  async relinquish(leaseOwner: string): Promise<void> {
    const held = await this.#state.lanes
      .find({ owner: String(leaseOwner), holder: this.#state.instanceId })
      .toArray();
    const runningIds = new Set(
      (await this.#queries.findRunning(held.flatMap((lane) => lane.inFlight))).map(
        (document) => document._id,
      ),
    );
    const idle = held.filter((lane) => lane.inFlight.every((id) => !runningIds.has(id)));
    if (idle.length > 0) {
      await this.#state.lanes.bulkWrite(
        idle.map((lane) => freeLane(lane._id, lane.epoch, lane.revision ?? 0, lane.inFlight)),
        { ordered: false },
      );
    }
  }
}

/**
 * Frees a lane, provided it still has `epoch` and lists nothing beyond
 * `finished` (ids of deliveries known not to run): a delivery recorded
 * meanwhile keeps the lane leased.
 */
function freeLane(
  laneId: number,
  epoch: number,
  revision: number,
  finished: readonly string[],
): AnyBulkWriteOperation<LaneDocument> {
  return {
    updateOne: {
      filter: {
        _id: laneId,
        epoch,
        revision: atCount(revision),
        inFlight: { $not: { $elemMatch: { $nin: [...finished] } } },
      },
      update: {
        $set: { owner: null, holder: null, leaseExpiresAt: null, inFlight: [] },
        $inc: { revision: 1 },
      },
    },
  };
}
