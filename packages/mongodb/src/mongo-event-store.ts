import {
  createUuidV7,
  type AppendEntry,
  type AppendResult,
  type ClaimRequest,
  type ClaimedDelivery,
  type DeliveryQuery,
  type DeliveryRecord,
  type ErrorInfo,
  type EventStore,
  type EventStoreStatistics,
  type EventStoreTransactionOptions,
  type IdentifierGenerator,
  type LeaseReference,
  type PurgeRequest,
  type PurgeResult,
  type RelayBusLogger,
} from '@relaybus/core';
import { HandedOutLedger, PositiveInteger } from '@relaybus/core/internal';
import type { ClientSession, MongoClient, TransactionOptions } from 'mongodb';
import { MongoAppend } from './mongo-append.js';
import { MongoClaims } from './mongo-claim.js';
import { MongoCompletion } from './mongo-completion.js';
import { MongoLaneLeases } from './mongo-lane-leases.js';
import { MongoOrdering } from './mongo-ordering.js';
import { MongoPurge } from './mongo-purge.js';
import { MongoQueries } from './mongo-queries.js';
import { MongoSchema } from './mongo-schema.js';
import {
  runInBudgetedTransaction,
  runInTransaction,
  type MongoStoreState,
} from './mongo-store-state.js';
import { assertCollectionPrefix, assertDatabaseName } from './naming.js';

export interface MongoEventStoreOptions {
  /** Connected client. The store never closes it. */
  readonly client: MongoClient;
  /** Database holding the relaybus collections. Default: the client's default database. */
  readonly database?: string;
  /** Prefix of the collection names. Default `relaybus_`. */
  readonly collectionPrefix?: string;
  /**
   * Options of the transactions the store opens. Default: snapshot read
   * concern, majority write concern, primary reads.
   */
  readonly transactionOptions?: TransactionOptions;
  /** Rows deleted per purge step. Default 500. */
  readonly purgeBatchSize?: number;
  /**
   * Keep an append-only record of every completed delivery in
   * `<prefix>completed_deliveries`, listed as `completed`, counted by
   * `statistics` and removed by `purge({ completedBefore })`. Default false: a
   * completed delivery is deleted and leaves nothing behind. Use the same value
   * in every process.
   */
  readonly completionHistory?: boolean;
  /** Receives warnings about best-effort steps that failed (parking). Default `console`. */
  readonly logger?: RelayBusLogger;
  /**
   * @deprecated Ignored. Lease tokens name the lane and its epoch
   * (`<laneId>:<epoch>:<claims>`) and are no longer generated.
   */
  readonly generateIdentifier?: IdentifierGenerator;
}

const DEFAULT_TRANSACTION_OPTIONS: TransactionOptions = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
};
const DEFAULT_PURGE_BATCH_SIZE = 500;

/**
 * Event store on MongoDB 6+ multi-document transactions (replica set or sharded
 * cluster). The transaction handle is the driver's `ClientSession`, so the same
 * session can carry your own writes (native driver or Mongoose).
 *
 * Guarantees:
 * - **Atomic, idempotent append.** `append` is atomic with the caller's writes
 *   and idempotent on event id and on (event id, handler). An event remembers
 *   every handler it ever had a delivery for, so a redelivered or republished
 *   event never runs a completed handler again although completed deliveries are
 *   deleted. Duplicates are detected by reading, never by catching a
 *   duplicate-key error, which would abort the surrounding transaction.
 * - **Per-key order.** Per-key sequences come from a counter document inside the
 *   append transaction. Two transactions appending the same key cannot both hold
 *   the counter, so the loser conflicts, restarts after the winner committed, and
 *   its sequence is higher: sequence order is commit order. A keyed delivery is
 *   claimed only when no earlier delivery of its handler and key is pending and
 *   no other delivery of them runs.
 * - **Lane leases.** Every delivery belongs to one of 256 lanes (a hash of its
 *   ordering key, or of its event id), so all deliveries of a key share a lane.
 *   Workers lease lanes with one conditional write on a lane document, and a
 *   claim records its whole batch in the lane documents: it writes no delivery
 *   document. While a lane lease is live, other workers do not see the pending
 *   deliveries of that lane, even idle ones. A lane changes hands only when its
 *   lease expired or its owner handed it back.
 * - **Fencing.** A lease token is `<laneId>:<epoch>:<claims>`. The epoch grows
 *   whenever the lane is taken; `claims`, the delivery's claim generation, grows
 *   whenever a claim of it ends (retry, release, burial, lost lease), so the
 *   lease of an earlier claim never matches a later claim of the same delivery
 *   in the same lane epoch. Completion, retry, burial, release and renewal apply
 *   only while the lane has that epoch, lists the delivery as in flight, and the
 *   delivery has that claim generation.
 * - **Effectively-once for transactional handlers.** A worker that takes a lane
 *   over first writes every delivery the previous owner had in flight (it
 *   charges the lost attempt) and only then reads what to claim. A completion
 *   inside a transaction (a transactional handler's, or any completion when a
 *   completion history is kept) deletes the delivery after checking the lane:
 *   both write the same delivery document, so the later of the two either
 *   conflicts and retries (and then sees the new epoch or the missing
 *   delivery), or waits for the earlier to commit. Other completions run
 *   without a transaction: they read the deliveries' claim generations, then
 *   the lanes, and delete only deliveries whose generation did not change,
 *   which a takeover, a retry or a release always changes. Either way a completion and a takeover
 *   cannot both succeed. Completions never write the lane document, so
 *   concurrent completions of one lane never conflict with each other.
 * - **Attempts survive crashes.** `attempts` counts attempts that ended. The
 *   claim result reports `attempts + 1`; a failure records it; a lane taken over
 *   charges one attempt to every delivery the previous owner still had in flight,
 *   so a poison message still reaches the retry limit after crashes.
 *
 * - **Deduplication record.** A completion records its time on the event
 *   (`completedAt`). An event nothing references any more stays until
 *   `purge({ completedBefore })` passes that time, however old the event is, and
 *   `deadBefore` alone never removes it. Once purged, the event takes its list
 *   of handlers with it: a broker redelivery or publish retry of that event id
 *   afterwards is appended as a new event and runs its handlers again, as in
 *   the PostgreSQL store, so keep completed retention longer than any
 *   redelivery window.
 *
 * `transaction` may run `work` more than once (see {@link MongoEventStore.transaction}).
 * Claims of one `leaseOwner` on one store instance run one after the other
 * (a claim that overlaps another of the same owner waits for it); different
 * owners claim concurrently, from any number of processes. Lanes are held per store
 * instance, so a process restarted with the same `leaseOwner` takes its
 * predecessor's lanes over only once their leases expired.
 */
export class MongoEventStore implements EventStore<ClientSession> {
  readonly #state: MongoStoreState;
  readonly #schema: MongoSchema;
  readonly #appender: MongoAppend;
  readonly #claims: MongoClaims;
  readonly #laneLeases: MongoLaneLeases;
  readonly #completion: MongoCompletion;
  readonly #queries: MongoQueries;
  readonly #purger: MongoPurge;

  constructor(options: MongoEventStoreOptions) {
    const prefix = options.collectionPrefix ?? 'relaybus_';
    assertCollectionPrefix(prefix);
    if (options.database !== undefined) {
      assertDatabaseName(options.database);
    }
    const purgeBatchSize = PositiveInteger.of(
      options.purgeBatchSize ?? DEFAULT_PURGE_BATCH_SIZE,
      'purgeBatchSize',
    );
    const database = options.client.db(options.database);
    // Majority writes make a lease durable; primary reads keep the ordering check fresh.
    const collectionOptions = {
      readPreference: 'primary',
      writeConcern: { w: 'majority' },
    } as const;
    const state: MongoStoreState = {
      client: options.client,
      database,
      events: database.collection(`${prefix}events`, collectionOptions),
      deliveries: database.collection(`${prefix}deliveries`, collectionOptions),
      keySequences: database.collection(`${prefix}key_sequences`, collectionOptions),
      lanes: database.collection(`${prefix}lanes`, collectionOptions),
      completions: database.collection(`${prefix}completed_deliveries`, collectionOptions),
      completionHistory: options.completionHistory ?? false,
      transactionOptions: options.transactionOptions ?? DEFAULT_TRANSACTION_OPTIONS,
      purgeBatchSize,
      logger: options.logger ?? console,
      instanceId: createUuidV7(),
      handedOut: new HandedOutLedger(),
      unreconciledOwners: new Set(),
    };
    this.#state = state;
    const queries = new MongoQueries(state);
    const ordering = new MongoOrdering(state);
    const laneLeases = new MongoLaneLeases(state, { queries });
    this.#schema = new MongoSchema(state);
    this.#appender = new MongoAppend(state);
    this.#claims = new MongoClaims(state, { laneLeases, ordering, queries });
    this.#laneLeases = laneLeases;
    this.#completion = new MongoCompletion(state);
    this.#queries = queries;
    this.#purger = new MongoPurge(state);
  }

  /**
   * Verifies the deployment supports transactions, creates the collections and
   * indexes and seeds the lane documents. Upgrades a 0.1.0 database in place
   * first (see `MongoSchema.#upgradeFromVersion010`); the seeded lanes
   * mark the upgrade as done, so later runs skip it. Safe to run repeatedly and
   * from several processes. Stop 0.1.0 workers before the first run.
   *
   * @throws ConfigurationError when the server is a standalone mongod.
   */
  setup(): Promise<void> {
    return this.#schema.setup();
  }

  /**
   * Runs `work` in a transaction on a new session and commits when it resolves.
   *
   * `work` can run more than once. The driver restarts the whole callback after
   * a transient failure such as a write conflict, and re-sends only the commit
   * after an unknown commit result. A restart is safe for everything written
   * through the session, because the aborted attempt left nothing behind. It is
   * not safe for side effects outside the session (HTTP calls, other databases),
   * so keep those out of `work`; relaybus handlers are already at-least-once.
   *
   * With `options.timeoutMs` (from `bus.transaction`) the server bounds the commit with
   * `maxCommitTimeMS`; the bus aborts a transaction that ran past the budget before its commit.
   *
   * @throws ConfigurationError when the deployment does not support transactions.
   */
  transaction<T>(
    work: (transaction: ClientSession) => Promise<T>,
    options?: EventStoreTransactionOptions,
  ): Promise<T> {
    return options === undefined
      ? runInTransaction(this.#state, work)
      : runInBudgetedTransaction(this.#state, { work, timeoutMs: options.timeoutMs });
  }

  /**
   * Writes the entries in `session`. An existing event gets a delivery only for
   * handlers it does not list yet; the added handlers are appended to its list
   * together with a `revision` increment, a real write that makes a concurrent
   * purge of the event conflict with this transaction.
   */
  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  append(
    session: ClientSession,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    return this.#appender.append(session, { entries, now });
  }

  /**
   * Claims up to `limit` due deliveries. It claims from the lanes this owner
   * (and this store instance) holds under a live lease first, then takes free
   * lanes with due work, a few at a time (4, 8, 16, ...) and the lane with the
   * oldest due delivery first, so a worker holds only as many lanes as it
   * needs. It then records the batch with one write per lane and hands back the
   * lanes that yielded nothing and run nothing. No delivery document is
   * written, except for the attempt charged to deliveries of a lane taken over
   * and for parking (see `MongoOrdering.#parkDeliveries`).
   *
   * Taking a lane is one conditional `findOneAndUpdate` that also moves the
   * previous owner's in-flight list to the lane's `lost` list; a lane another
   * claimer took first is skipped, never waited for. Lost deliveries are charged
   * one attempt before anything is read from the lane, and `lost` is cleared
   * only after that. Whoever holds the lane next charges a `lost` list it finds
   * first, so a claim that fails anywhere after the take loses no charge; the
   * charge of a lost claim is recorded on the delivery, so charging the list
   * again after a failure before the clear charges nothing twice.
   * Nothing a failed claim did leaves a delivery listed as running.
   */
  claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    return this.#claims.claim(request);
  }

  /**
   * Extends the lease of the lane a running delivery belongs to. The lane lease
   * never shrinks, so a repeated renewal with the same value still holds. An
   * expired lease nobody took over is still renewed, so `_now` is not needed.
   */
  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  extendLease(lease: LeaseReference, leaseExpiresAt: Date, _now?: Date): Promise<boolean> {
    // A database answers with a promise; Promise.resolve returns it as it is.
    return Promise.resolve(this.#completion.extendLease({ lease, leaseExpiresAt }));
  }

  /**
   * Deletes the delivery (and records it in the completion history, when kept).
   * With `session` the deletion commits with the caller's transaction; without,
   * it runs in a transaction of its own. Returns false when the lease was lost.
   */
  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  complete(lease: LeaseReference, now: Date, session?: ClientSession): Promise<boolean> {
    return this.#completion.complete(lease, { now, session });
  }

  /**
   * Completes many leases with a fixed number of round trips, whatever lanes
   * they belong to: three commands without a completion history, one
   * transaction with it (see `MongoCompletion.#deleteFenced`).
   * Returns one flag per lease in input order; a lease listed twice completes
   * once, and a token this store cannot have issued is refused.
   */
  completeMany(leases: readonly LeaseReference[], now: Date): Promise<boolean[]> {
    return this.#completion.completeMany(leases, now);
  }

  /** Returns a running delivery to pending, due at `availableAt`, and counts the attempt that ended. */
  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  reschedule(
    lease: LeaseReference,
    availableAt: Date,
    error: ErrorInfo,
    now: Date,
  ): Promise<boolean> {
    return Promise.resolve(
      this.#completion.changeClaim({
        lease,
        change: { kind: 'reschedule', availableAt, error, now },
      }),
    );
  }

  /** Gives a running delivery back without counting the attempt: pending, due `now`. */
  release(lease: LeaseReference, now: Date): Promise<boolean> {
    return Promise.resolve(
      this.#completion.changeClaim({ lease, change: { kind: 'release', now } }),
    );
  }

  /** Moves a running delivery to dead and counts the attempt that ended. */
  // eslint-disable-next-line relaybus/max-two-params -- EventStore port signature
  bury(lease: LeaseReference, error: ErrorInfo, now: Date): Promise<boolean> {
    return Promise.resolve(
      this.#completion.changeClaim({ lease, change: { kind: 'bury', error, now } }),
    );
  }

  /**
   * Frees every lane `leaseOwner` holds that runs no delivery, so other workers
   * need not wait for the lease to expire. Lanes with running deliveries stay leased.
   */
  relinquish(leaseOwner: string): Promise<void> {
    return this.#laneLeases.relinquish(leaseOwner);
  }

  /**
   * Lists deliveries in creation order. A delivery is `processing` while its
   * document exists as `pending` and its lane lists it; its attempt count then
   * includes the running attempt, and owner and expiry are those of the lane
   * lease. Completion history records (when kept) are listed as `completed`.
   */
  listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    return this.#queries.listDeliveries(query);
  }

  /** Moves dead deliveries back to pending with a fresh attempt budget. */
  requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    return this.#queries.requeue(deliveryIds, now);
  }

  /**
   * Deletes, in batches of `purgeBatchSize`, dead deliveries last updated before
   * `deadBefore`, and completion history records (and `completed` documents left
   * by 0.1.0) completed before `completedBefore`, each batch in one transaction
   * together with the events it left without any reference. Finally, events
   * nothing references are deleted: one with a completed delivery once its last
   * completion is before `completedBefore` (never by `deadBefore` alone), one
   * that never had a completion (no subscribers, or only dead letters) once it
   * was created before the earliest given cutoff. The batch step applies the
   * same rule to events whose last completion is recorded.
   */
  purge(request: PurgeRequest): Promise<PurgeResult> {
    return this.#purger.purge(request);
  }

  /**
   * Counts by derived status. `pending` excludes running deliveries; `completed`
   * counts history records and 0.1.0 `completed` documents. `oldestDueAt` is the
   * oldest instant at which a delivery became claimable: the due time of a
   * pending delivery nobody runs, or the lease expiry of a lane that still runs
   * deliveries.
   */
  statistics(now: Date): Promise<EventStoreStatistics> {
    return this.#queries.statistics(now);
  }
}
