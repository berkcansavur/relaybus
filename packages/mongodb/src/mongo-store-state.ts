import { ConfigurationError, type RelayBusLogger } from '@relaybus/core';
import type { ClientSession, Collection, Db, MongoClient, TransactionOptions } from 'mongodb';
import {
  type CompletedDeliveryDocument,
  type DeliveryDocument,
  type EventDocument,
  type KeySequenceDocument,
  type LaneDocument,
} from './documents.js';
import {
  createTransactionsUnsupportedError,
  isTransactionsUnsupportedError,
} from './mongo-errors.js';

/** A lane a claim holds: already held before it, or taken by it. */
export interface ScopeLane {
  readonly laneId: number;
  /** Epoch of this owner's lease. */
  readonly epoch: number;
  /** `inFlight` of the lane as the claim read it: deliveries this owner claimed earlier. */
  readonly listed: readonly string[];
  /** `revision` of the lane as the claim read it; the record only applies while it is unchanged. */
  readonly revision: number;
}

/** A claim this store instance handed out and has not seen settled. */
export interface HandedOutClaim {
  readonly deliveryId: string;
  readonly owner: string;
  readonly laneId: number;
  readonly epoch: number;
}

/** The collections, settings and per-instance bookkeeping every part of {@link MongoEventStore} shares. */
export type MongoStoreState = {
  readonly client: MongoClient;
  readonly database: Db;
  readonly events: Collection<EventDocument>;
  readonly deliveries: Collection<DeliveryDocument>;
  readonly keySequences: Collection<KeySequenceDocument>;
  readonly lanes: Collection<LaneDocument>;
  readonly completions: Collection<CompletedDeliveryDocument>;
  readonly completionHistory: boolean;
  readonly transactionOptions: TransactionOptions;
  readonly purgeBatchSize: number;
  readonly logger: RelayBusLogger;
  /**
   * Identifies this store instance in the lanes it holds. A process that restarts
   * with the same `leaseOwner` gets a new instance, so it never mistakes the
   * lanes of its dead predecessor for its own: it waits for their leases to
   * expire and takes them over like any other worker.
   */
  readonly instanceId: string;
  /** The last claim of each owner still running on this instance; the next one waits for it. */
  readonly claimsInProgress: Map<string, Promise<unknown>>;
  /**
   * Claims this instance handed out that are not settled yet, keyed by
   * {@link leaseKey} (delivery id and exact lease token), with the owner. A
   * claim is settled when a completion, retry, release or burial of exactly
   * that lease returned, or, for a completion inside the caller's transaction
   * (which may still retry or roll back), when a later claim of the owner finds
   * the delivery gone. After a failed claim, a listed delivery stays listed only
   * if one of these is its current claim (current lane epoch and claim generation).
   */
  readonly handedOut: Map<string, HandedOutClaim>;
  /** Owners whose lanes on this instance still need reconciling after a failed claim. */
  readonly unreconciledOwners: Set<string>;
};

/** Runs `work` in a transaction on a new session; see {@link MongoEventStore.transaction}. */
export async function runInTransaction<T>(
  state: MongoStoreState,
  work: (transaction: ClientSession) => Promise<T>,
): Promise<T> {
  return runInTransactionWith(state, { work, transactionOptions: state.transactionOptions });
}

/**
 * Runs `work` in a transaction held to `timeoutMs`: the driver's client-side operation timeout
 * (`timeoutMS`) bounds the whole `withTransaction`, including retries and every operation sent
 * through the session after the budget ran out, and `maxCommitTimeMS` bounds the commit on the server.
 */
export async function runInBudgetedTransaction<T>(
  state: MongoStoreState,
  {
    work,
    timeoutMs,
  }: { readonly work: (transaction: ClientSession) => Promise<T>; readonly timeoutMs: number },
): Promise<T> {
  const budget = Math.ceil(timeoutMs);
  return runInTransactionWith(state, {
    work,
    transactionOptions: { ...state.transactionOptions, maxCommitTimeMS: budget, timeoutMS: budget },
  });
}

async function runInTransactionWith<T>(
  state: MongoStoreState,
  {
    work,
    transactionOptions,
  }: {
    readonly work: (transaction: ClientSession) => Promise<T>;
    readonly transactionOptions: TransactionOptions & { readonly timeoutMS?: number };
  },
): Promise<T> {
  try {
    return await state.client.withSession(async (session) => {
      let result!: T;
      await session.withTransaction(async () => {
        result = await work(session);
      }, transactionOptions);
      return result;
    });
  } catch (error) {
    if (isTransactionsUnsupportedError(error)) {
      throw createTransactionsUnsupportedError(error);
    }
    throw error;
  }
}

/** Key of a handed-out claim: the delivery and the exact lease token it was handed out with. */
export function leaseKey(deliveryId: string, leaseToken: string): string {
  return `${deliveryId}\u0000${leaseToken}`;
}

export function assertInTransaction(session: ClientSession, operation: string): void {
  if (!session.inTransaction()) {
    throw new ConfigurationError(
      `MongoEventStore.${operation} needs a ClientSession inside a started transaction (session.withTransaction or startTransaction). Without one, MongoDB commits each write immediately and the write is no longer atomic with yours`,
    );
  }
}

/** Matches a counter (claim generation, lane revision) equal to `count`; a missing counter is 0. */
export function atCount(count: number): number | { $not: { $gt: number } } {
  // Counters are never negative, so "not above 0" is 0 or absent.
  return count === 0 ? { $not: { $gt: 0 } } : count;
}
