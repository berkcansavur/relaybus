import type { ClaimRequest, ClaimedDelivery } from '@relaybus/core';
import { CompositeKey, OwnerSerializedClaims } from '@relaybus/core/internal';
import { ClaimScan, orderingIdentity } from './claim-scan.js';
import type { DeliveryDocument, LaneDocument } from './documents.js';
import { formatLeaseToken, parseLeaseToken } from './lanes.js';
import { MongoLaneReconciler } from './mongo-lane-reconciler.js';
import type { MongoLaneLeases } from './mongo-lane-leases.js';
import type { MongoOrdering } from './mongo-ordering.js';
import type { MongoQueries } from './mongo-queries.js';
import { type MongoStoreState, type ScopeLane } from './mongo-store-state.js';

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

/** The claimed deliveries as the claim hands them to the caller. */
function toClaimedDeliveries(
  claimed: readonly DeliveryDocument[],
  context: {
    readonly scope: ReadonlyMap<number, ScopeLane>;
    readonly events: Awaited<ReturnType<MongoQueries['readEnvelopes']>>;
    readonly request: ClaimRequest;
  },
): ClaimedDelivery[] {
  const { scope, events, request } = context;
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
export class MongoClaims extends OwnerSerializedClaims {
  readonly #state: MongoStoreState;
  readonly #laneLeases: MongoLaneLeases;
  readonly #ordering: MongoOrdering;
  readonly #queries: MongoQueries;
  readonly #reconciler: MongoLaneReconciler;

  constructor(
    state: MongoStoreState,
    collaborators: {
      readonly laneLeases: MongoLaneLeases;
      readonly ordering: MongoOrdering;
      readonly queries: MongoQueries;
    },
  ) {
    super();
    this.#state = state;
    this.#laneLeases = collaborators.laneLeases;
    this.#ordering = collaborators.ordering;
    this.#queries = collaborators.queries;
    this.#reconciler = new MongoLaneReconciler(state);
  }

  /** Mongo claims nothing for a limit of 0 or below (a limit that is not a number is tried). */
  protected override claimsNothing(request: ClaimRequest): boolean {
    return request.handlers.length === 0 || request.limit <= 0;
  }

  /**
   * Runs one claim and remembers what it hands out. When the claim fails at any
   * point, also after its record reached the database without the reply
   * reaching this process, the lanes this owner holds on this instance are
   * reconciled: every listed delivery this instance has not handed out, or has
   * handed out and seen settled, is taken out of them, so nothing stays listed
   * as running that nobody runs.
   */
  protected async claimInTurn(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    const owner = request.leaseOwner;
    if (this.#state.unreconciledOwners.has(owner)) {
      // An earlier reconciliation failed: a delivery may still be listed that nobody runs, and the
      // owner's other deliveries may keep the lane alive, so it is done before claiming anything.
      await this.#reconciler.reconcile(owner);
      this.#state.unreconciledOwners.delete(owner);
    }
    try {
      const claimed = await this.#claimNow(request);
      for (const delivery of claimed) {
        const { laneId, epoch } = parseLeaseToken(delivery.leaseToken)!;
        this.#state.handedOut.register(delivery, {
          deliveryId: delivery.deliveryId,
          owner: request.leaseOwner,
          laneId,
          epoch,
        });
      }
      return claimed;
    } catch (error) {
      const [reconciled] = await Promise.allSettled([this.#reconciler.reconcile(owner)]);
      if (reconciled.status === 'rejected') {
        this.#state.unreconciledOwners.add(owner);
      }
      throw error;
    }
  }

  async #claimNow(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    const held = await this.#readHeldLanes(request);
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
    return toClaimedDeliveries(claimed, { scope, events, request });
  }

  /** The lanes the owner holds on this instance, read while the next deliveries are unparked. */
  async #readHeldLanes(request: ClaimRequest): Promise<LaneDocument[]> {
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
    return held;
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
    this.#state.handedOut.forgetWhere(
      (entry) =>
        entry.owner === owner &&
        gone.has(CompositeKey.of(entry.deliveryId, `${entry.laneId}:${entry.epoch}`)),
    );
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
