import type { ClaimRequest } from '@relaybus/core';
import type { Filter } from 'mongodb';
import { type ClaimScan, orderingIdentity } from './claim-scan.js';
import type { DeliveryDocument } from './documents.js';
import type { MongoStoreState, ScopeLane } from './mongo-store-state.js';

const MINIMUM_CANDIDATE_BATCH_SIZE = 32;

const PARK_UPDATE_CHUNK_SIZE = 1_000;

/** One round of candidate picking over some lanes of a claim. */
type PickCandidatesInput = {
  readonly lanes: readonly ScopeLane[];
  readonly request: ClaimRequest;
  readonly scan: ClaimScan;
  readonly running: Promise<{ readonly keys: Set<string> }>;
  /** The claim's picks so far; this round appends to it. */
  readonly picked: DeliveryDocument[];
};

/** Picks claimable candidates in key order and parks deliveries stuck behind an unfinished predecessor. */
export class MongoOrdering {
  readonly #state: MongoStoreState;

  constructor(state: MongoStoreState) {
    this.#state = state;
  }

  /**
   * Adds the claimable candidates of `lanes` to `picked`, in due order, until
   * `request.limit` is reached. A delivery is a candidate when it is ready, due
   * and not in flight in its lane. A keyed candidate is claimable only when
   * (a) no earlier delivery of its handler and key is pending (a running one is
   * still a pending document) and (b) no other delivery of them runs. Running
   * deliveries of a key live in the key's lane, which this owner holds, so the
   * running keys (loaded once from the held lanes) and this claim's own picks
   * are all of them.
   *
   * Candidates are read in pages with a limit, so no cursor is left open, and
   * the predecessors of the keys of a page are looked up in parallel, one
   * lookup per key.
   */
  async pickCandidates(input: PickCandidatesInput): Promise<void> {
    const { lanes, request, scan, running, picked } = input;
    const pageSize = Math.max((request.limit - picked.length) * 2, MINIMUM_CANDIDATE_BATCH_SIZE);
    const filter: Filter<DeliveryDocument> = {
      laneId: { $in: lanes.map((lane) => lane.laneId) },
      handler: { $in: [...request.handlers] },
      lane: 'ready',
      dueAt: { $lte: request.now },
      _id: { $nin: lanes.flatMap((lane) => lane.listed) },
    };
    let after: DeliveryDocument | undefined;
    for (;;) {
      const [page, { keys }] = await Promise.all([
        this.#state.deliveries
          .find(
            after === undefined
              ? filter
              : {
                  $and: [
                    filter,
                    {
                      $or: [
                        { dueAt: { $gt: after.dueAt! } },
                        { dueAt: after.dueAt!, _id: { $gt: after._id } },
                      ],
                    },
                  ],
                },
            { sort: { dueAt: 1, _id: 1 }, limit: pageSize },
          )
          .toArray(),
        running,
      ]);
      if (await this.#pickFromPage(page, { request, scan, picked, keys })) {
        return;
      }
      if (page.length < pageSize) {
        return;
      }
      after = page.at(-1);
    }
  }

  /**
   * Adds the claimable candidates of `page` to `picked`. Looks up predecessors only for as many
   * candidates as the claim still needs, then more. True once the claim has `request.limit` picks.
   */
  async #pickFromPage(
    page: readonly DeliveryDocument[],
    claim: {
      readonly request: ClaimRequest;
      readonly scan: ClaimScan;
      readonly picked: DeliveryDocument[];
      readonly keys: Set<string>;
    },
  ): Promise<boolean> {
    const { request, scan, picked, keys } = claim;
    for (let offset = 0; offset < page.length;) {
      const slice = page.slice(offset, offset + request.limit - picked.length);
      offset += slice.length;
      await this.#notePredecessors(slice, scan);
      for (const candidate of slice) {
        if (isClaimable(candidate, scan, keys)) {
          picked.push(candidate);
        }
      }
      if (picked.length >= request.limit) {
        return true;
      }
    }
    return false;
  }

  /**
   * Looks up, in parallel and once per key, whether an unfinished delivery
   * precedes the earliest candidate of each keyed (handler, key) of `page` that
   * this claim knows nothing about yet, and tells `scan`. The earliest candidate
   * is itself unfinished, so every later candidate of its key waits at least for it.
   */
  async #notePredecessors(page: readonly DeliveryDocument[], scan: ClaimScan): Promise<void> {
    const earliest = new Map<string, DeliveryDocument>();
    for (const candidate of page) {
      if (candidate.key !== undefined) {
        const orderingId = orderingIdentity(candidate.handler, candidate.key);
        // Pages come in due order, not in sequence order, so keep the lowest sequence.
        const known = earliest.get(orderingId);
        if (!scan.knows(orderingId) && (known?.sequence ?? Infinity) > candidate.sequence!) {
          earliest.set(orderingId, candidate);
        }
      }
    }
    await Promise.all(
      [...earliest].map(async ([orderingId, candidate]) => {
        const predecessor = await this.#findUnfinishedPredecessor(
          candidate.handler,
          candidate.key!,
          candidate.sequence!,
        );
        scan.noteOpen(orderingId, predecessor ?? candidate.sequence!);
      }),
    );
  }

  /**
   * Parks the waiting backlogs a claim found. Best effort: parking only saves
   * later claims work, so a failure is logged and the claim still returns its
   * batch; the next claim that meets the backlog parks it.
   */
  async parkWaiting(deliveryIds: readonly string[]): Promise<void> {
    try {
      await this.#parkDeliveries(deliveryIds);
    } catch (error) {
      this.#state.logger.warn(
        `[MongoEventStore](parkWaiting) Parking waiting deliveries failed, a later claim parks them | count=${deliveryIds.length} | error=${String(error)}`,
      );
    }
  }

  /** Sequence of the lowest unfinished delivery of the handler and key before `sequence`, if any. */
  async #findUnfinishedPredecessor(
    handler: string,
    key: string,
    sequence: number,
  ): Promise<number | undefined> {
    const predecessor = await this.#state.deliveries.findOne(
      { handler, key, sequence: { $lt: sequence }, dueAt: { $exists: true } },
      { sort: { sequence: 1 }, projection: { sequence: 1 } },
    );
    return predecessor?.sequence;
  }

  /**
   * Parks pending deliveries that waited behind an unfinished predecessor, which
   * removes them from the claim index until their turn. Parking is a hint, not a
   * guarantee: a predecessor that finishes meanwhile is found by the next
   * {@link MongoEventStore.claim}, which moves the lowest parked delivery back.
   */
  async #parkDeliveries(deliveryIds: readonly string[]): Promise<void> {
    for (let offset = 0; offset < deliveryIds.length; offset += PARK_UPDATE_CHUNK_SIZE) {
      await this.#state.deliveries.updateMany(
        {
          _id: { $in: deliveryIds.slice(offset, offset + PARK_UPDATE_CHUNK_SIZE) },
          status: 'pending',
          lane: 'ready',
        },
        { $set: { lane: 'parked' } },
      );
    }
  }

  /**
   * Moves the lowest parked delivery of every parked key of `handlers` back into
   * the claim index when nothing unfinished precedes it. Level-triggered on
   * purpose: it looks at the current state only, so a crash anywhere, or a
   * predecessor finishing while its followers were being parked, can delay a key
   * until the next claim but never strand it.
   */
  async unparkNextDeliveries(handlers: readonly string[]): Promise<void> {
    const anyParked = await this.#state.deliveries.findOne(
      { handler: { $in: [...handlers] }, lane: 'parked' },
      // The sort lets the planner pick the small partial index over the handler's whole history.
      { sort: { handler: 1, key: 1, sequence: 1 }, projection: { _id: 1 } },
    );
    if (anyParked === null) {
      return;
    }
    // The lowest parked delivery of every key, one aggregation per handler in parallel. With the
    // handler fixed and one group field, the server answers it with a distinct scan of the small
    // partial index: one index probe per parked key, not one per parked delivery.
    const lowest = (
      await Promise.all(
        [...new Set(handlers)].map((handler) =>
          this.#state.deliveries
            .aggregate<{ _id: string; id: string; sequence: number }>([
              { $match: { handler, lane: 'parked' } },
              { $sort: { handler: 1, key: 1, sequence: 1 } },
              {
                $group: { _id: '$key', id: { $first: '$_id' }, sequence: { $first: '$sequence' } },
              },
            ])
            .toArray()
            .then((groups) => groups.map((group) => ({ ...group, handler }))),
        ),
      )
    ).flat();
    const next = await Promise.all(
      lowest.map(async (parked) =>
        (await this.#findUnfinishedPredecessor(parked.handler, parked._id, parked.sequence)) ===
        undefined
          ? [parked.id]
          : [],
      ),
    );
    const ready = next.flat();
    if (ready.length > 0) {
      await this.#state.deliveries.updateMany(
        { _id: { $in: ready }, lane: 'parked' },
        { $set: { lane: 'ready' } },
      );
    }
  }
}

/**
 * Whether a candidate may be claimed, given what `scan` knows about its key
 * (see `MongoOrdering.#notePredecessors`) and `blockedKeys`: the keys that
 * run, to which every key this claim takes is added, so a claim takes at most
 * one delivery per key.
 */
function isClaimable(
  candidate: DeliveryDocument,
  scan: ClaimScan,
  blockedKeys: Set<string>,
): boolean {
  const { key, sequence } = candidate;
  if (key === undefined) {
    return true;
  }
  const orderingId = orderingIdentity(candidate.handler, key);
  if (scan.waits(orderingId, sequence!)) {
    scan.noteWaiting(orderingId, candidate._id, candidate.laneId);
    return false;
  }
  if (blockedKeys.has(orderingId)) {
    return false;
  }
  blockedKeys.add(orderingId);
  return true;
}
