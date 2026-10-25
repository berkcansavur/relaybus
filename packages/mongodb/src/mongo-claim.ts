import type { ClaimRequest, ClaimedDelivery } from '@relaybus/core';
import { CompositeKey, DeliveryLeaseIdentity } from '@relaybus/core/internal';
import { ClaimScan, orderingIdentity } from './claim-scan.js';
import type { DeliveryDocument, LaneDocument } from './documents.js';
import { formatLeaseToken, parseLeaseToken } from './lanes.js';
import type { MongoLaneLeases } from './mongo-lane-leases.js';
import type { MongoOrdering } from './mongo-ordering.js';
import type { MongoQueries } from './mongo-queries.js';
import { runInTransaction, type MongoStoreState, type ScopeLane } from './mongo-store-state.js';

/** Deliveries that waited behind one unfinished predecessor before they are parked. */
const PARK_BACKLOG_THRESHOLD = 32;

/** Lanes a claim takes in its first round; every further round takes twice as many. */
const INITIAL_LANES_PER_ROUND = 4;

/**
 * Lanes one candidate query covers. A query over more lanes than the planner
 * explodes into merged index scans (200 by default) would sort every due
 * delivery of those lanes in memory instead of reading them in due order.
 */
const LANES_PER_CANDIDATE_QUERY = 128;

/** The deliveries the held lanes still run: their ids and (handler, key) identities. */
type RunningDeliveries = { readonly ids: Set<string>; readonly keys: Set<string> };

/** One claim's lanes and picks while it reads candidates. */
type PickAcrossLanesInput = {
  readonly request: ClaimRequest;
  /** Grows by every lane the claim takes. */
  readonly scope: Map<number, ScopeLane>;
  readonly scan: ClaimScan;
  readonly running: Promise<RunningDeliveries>;
  readonly picked: DeliveryDocument[];
};

/** The held lanes as the claim read them. */
function scopeOf(held: readonly LaneDocument[]): Map<number, ScopeLane> {
  return new Map<number, ScopeLane>(
    held.map((lane) => [
      lane._id,
      {
        laneId: lane._id,
        epoch: lane.epoch,
        listed: lane.inFlight,
        revision: lane.revision ?? 0,
      },
    ]),
  );
}

/** Claims due deliveries for one owner at a time and reconciles the lanes after a failed claim. */
export class MongoClaims {
  readonly #state: MongoStoreState;
  readonly #laneLeases: MongoLaneLeases;
  readonly #ordering: MongoOrdering;
  readonly #queries: MongoQueries;

  constructor(
    state: MongoStoreState,
    collaborators: {
      readonly laneLeases: MongoLaneLeases;
      readonly ordering: MongoOrdering;
      readonly queries: MongoQueries;
    },
  ) {
    this.#state = state;
    this.#laneLeases = collaborators.laneLeases;
    this.#ordering = collaborators.ordering;
    this.#queries = collaborators.queries;
  }

  /** Implements {@link MongoEventStore.claim}. */
  async claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (request.handlers.length === 0 || request.limit <= 0) {
      return [];
    }
    // Claims of one owner read the same held lanes and in-flight lists, so they must not overlap
    // (bus.runOnce() next to a running dispatcher, for instance): they run one after the other.
    const owner = request.leaseOwner;
    const previous = this.#state.claimsInProgress.get(owner) ?? Promise.resolve();
    const claimed = previous.then(() => this.#claimReconciled(request));
    const settled = Promise.allSettled([claimed]);
    this.#state.claimsInProgress.set(owner, settled);
    void settled.then(() => {
      if (this.#state.claimsInProgress.get(owner) === settled) {
        this.#state.claimsInProgress.delete(owner);
      }
    });
    return claimed;
  }

  /**
   * Runs one claim and remembers what it hands out. When the claim fails at any
   * point, also after its record reached the database without the reply
   * reaching this process, the lanes this owner holds on this instance are
   * reconciled: every listed delivery this instance has not handed out, or has
   * handed out and seen settled, is taken out of them, so nothing stays listed
   * as running that nobody runs.
   */
  async #claimReconciled(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    const owner = request.leaseOwner;
    if (this.#state.unreconciledOwners.has(owner)) {
      // An earlier reconciliation failed: a delivery may still be listed that nobody runs, and the
      // owner's other deliveries may keep the lane alive, so it is done before claiming anything.
      await this.#reconcileLanes(owner);
      this.#state.unreconciledOwners.delete(owner);
    }
    try {
      const claimed = await this.#claimNow(request);
      for (const delivery of claimed) {
        const { laneId, epoch } = parseLeaseToken(delivery.leaseToken)!;
        this.#state.handedOut.set(
          DeliveryLeaseIdentity.of(delivery.deliveryId, delivery.leaseToken),
          {
            deliveryId: delivery.deliveryId,
            owner: request.leaseOwner,
            laneId,
            epoch,
          },
        );
      }
      return claimed;
    } catch (error) {
      const [reconciled] = await Promise.allSettled([this.#reconcileLanes(owner)]);
      if (reconciled.status === 'rejected') {
        this.#state.unreconciledOwners.add(owner);
      }
      throw error;
    }
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
  async #reconcileLanes(owner: string): Promise<void> {
    await runInTransaction(this.#state, async (session) => {
      const held = await this.#state.lanes
        .find({ owner, holder: this.#state.instanceId }, { session })
        .toArray();
      const listed = held.flatMap((lane) => lane.inFlight);
      const claimsById = new Map(
        (
          await this.#state.deliveries
            .find(
              { _id: { $in: listed }, status: 'pending' },
              { session, projection: { claims: 1 } },
            )
            .toArray()
        ).map((document) => [document._id, document.claims ?? 0]),
      );
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
      for (const [key, entry] of this.#state.handedOut) {
        if (entry.owner === owner && !current.has(key)) {
          this.#state.handedOut.delete(key);
        }
      }
      if (held.length === 0) {
        return;
      }
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
    });
  }

  async #claimNow(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    // Independent reads, so they share one round trip: unparking only has to finish before candidates are read.
    const [held] = await Promise.all([
      this.#state.lanes
        .find({
          owner: request.leaseOwner,
          holder: this.#state.instanceId,
          leaseExpiresAt: { $gt: request.now },
        })
        .toArray(),
      this.#ordering.unparkNextDeliveries(request.handlers),
    ]);
    await this.#laneLeases.chargeLost(held);
    const scope = scopeOf(held);
    // Loaded while the first candidates are read; only keyed candidates and the record need it.
    const running = this.#loadRunning(held);
    const scan = new ClaimScan();
    const picked: DeliveryDocument[] = [];
    await this.#pickAcrossLanes({ request, scope, scan, running, picked });
    // Read before recording: a failure from here on must not leave picks listed as running.
    const events = await this.#queries.readEnvelopes(picked.map((document) => document.eventId));
    const runningIds = (await running).ids;
    this.#forgetSettled(held, { runningIds, owner: request.leaseOwner });
    const { recorded, handedBack } = await this.#laneLeases.recordClaim(scope, {
      picked,
      runningIds,
      request,
    });
    const claimed = await this.#releaseOvertaken(
      picked.filter((document) => recorded.has(document.laneId)),
      scope,
    );
    await this.#ordering.parkWaiting(scan.waitingBacklogs(PARK_BACKLOG_THRESHOLD, handedBack));
    return claimed.map((document) => ({
      deliveryId: document._id,
      leaseToken: formatLeaseToken(document.laneId, {
        epoch: scope.get(document.laneId)!.epoch,
        claims: document.claims ?? 0,
      }),
      handler: document.handler,
      attempt: document.attempts + 1,
      leaseExpiresAt: request.leaseExpiresAt,
      event: events.get(document.eventId)!,
    }));
  }

  /** The deliveries still running in the held lanes, with their (handler, key) identities. */
  #loadRunning(held: readonly LaneDocument[]): Promise<RunningDeliveries> {
    return this.#queries.findRunning(held.flatMap((lane) => lane.inFlight)).then((documents) => ({
      ids: new Set(documents.map((document) => document._id)),
      keys: new Set(
        documents.flatMap((document) =>
          document.key === undefined ? [] : [orderingIdentity(document.handler, document.key)],
        ),
      ),
    }));
  }

  /**
   * Picks from the lanes in `scope`, then takes free lanes with due work, a few more each round
   * (4, 8, 16, ...), and picks from them, until the claim is full or no free lane is left.
   */
  async #pickAcrossLanes(input: PickAcrossLanesInput): Promise<void> {
    const { request, scope, scan, running, picked } = input;
    const pickFrom = async (lanes: readonly ScopeLane[]): Promise<void> => {
      for (let offset = 0; offset < lanes.length; offset += LANES_PER_CANDIDATE_QUERY) {
        if (picked.length < request.limit) {
          await this.#ordering.pickCandidates({
            lanes: lanes.slice(offset, offset + LANES_PER_CANDIDATE_QUERY),
            request,
            scan,
            running,
            picked,
          });
        }
      }
    };
    await pickFrom([...scope.values()]);
    const passed = new Set(scope.keys());
    for (let laneCount = INITIAL_LANES_PER_ROUND; picked.length < request.limit; laneCount *= 2) {
      const free = await this.#laneLeases.findFreeLanes(request, {
        laneCount,
        passed: [...passed],
      });
      if (free.length === 0) {
        break;
      }
      for (const laneId of free) {
        passed.add(laneId);
      }
      const taken = await this.#laneLeases.takeLanes(free, request);
      for (const lane of taken) {
        scope.set(lane.laneId, lane);
      }
      await pickFrom(taken);
    }
  }

  /**
   * Forgets the claims of `owner` that are listed in the held lanes but no longer pending:
   * completed (a burial or retry would have unlisted it). Such a claim is settled, also when it was
   * completed inside a caller's transaction that committed. Only this owner's registrations of the
   * lane epochs it read: the same delivery may meanwhile have been requeued and handed out again,
   * under another owner or lane epoch.
   */
  #forgetSettled(
    held: readonly LaneDocument[],
    { runningIds, owner }: { readonly runningIds: ReadonlySet<string>; readonly owner: string },
  ): void {
    const gone = new Set(
      held.flatMap((lane) =>
        lane.inFlight
          .filter((id) => !runningIds.has(id))
          .map((id) => CompositeKey.of(id, `${lane._id}:${lane.epoch}`)),
      ),
    );
    for (const [key, entry] of this.#state.handedOut) {
      if (
        entry.owner === owner &&
        gone.has(CompositeKey.of(entry.deliveryId, `${entry.laneId}:${entry.epoch}`))
      ) {
        this.#state.handedOut.delete(key);
      }
    }
  }

  /**
   * Gives back the recorded keyed picks that a delivery requeued during the
   * claim now precedes, without charging an attempt, and returns the rest. The
   * claim checked their predecessors before recording them; a dead letter
   * requeued in between would otherwise wait behind its own successor. Once a
   * pick is recorded, a later requeue waits for it like for any running delivery.
   */
  async #releaseOvertaken(
    claimed: readonly DeliveryDocument[],
    scope: ReadonlyMap<number, ScopeLane>,
  ): Promise<DeliveryDocument[]> {
    const keyed = claimed.filter((document) => document.key !== undefined);
    if (keyed.length === 0) {
      return [...claimed];
    }
    // One query for the whole batch: each branch is one probe of the ordering index.
    const blockers = await this.#state.deliveries
      .find(
        {
          $or: keyed.map((document) => ({
            handler: document.handler,
            key: document.key!,
            sequence: { $lt: document.sequence! },
            dueAt: { $exists: true },
          })),
        },
        { projection: { handler: 1, key: 1 } },
      )
      .toArray();
    const blocked = new Set(
      blockers.map((blocker) => orderingIdentity(blocker.handler, blocker.key!)),
    );
    const overtaken = new Set(
      keyed
        .filter((document) => blocked.has(orderingIdentity(document.handler, document.key!)))
        .map((document) => document._id),
    );
    if (overtaken.size === 0) {
      return [...claimed];
    }
    await this.#releasePicks(
      claimed.filter((document) => overtaken.has(document._id)),
      scope,
    );
    return claimed.filter((document) => !overtaken.has(document._id));
  }

  /** Takes recorded picks out of their lanes' in-flight lists again, under the claim's epochs and record revisions. */
  async #releasePicks(
    documents: readonly DeliveryDocument[],
    scope: ReadonlyMap<number, ScopeLane>,
  ): Promise<void> {
    const idsByLane = new Map<number, string[]>();
    for (const document of documents) {
      idsByLane.set(document.laneId, [...(idsByLane.get(document.laneId) ?? []), document._id]);
    }
    await Promise.all(
      [...idsByLane].map(([laneId, ids]) =>
        this.#state.lanes.updateOne(
          // Under the revision the record left: a pull that reaches the server late, after a
          // reconciliation and a newer claim of the same delivery, matches nothing.
          {
            _id: laneId,
            epoch: scope.get(laneId)!.epoch,
            revision: scope.get(laneId)!.revision + 1,
          },
          { $pull: { inFlight: { $in: ids } } },
        ),
      ),
    );
  }
}
