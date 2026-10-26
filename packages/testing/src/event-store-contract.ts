import type { AppendEntry, ClaimedDelivery, EventStore, LeaseReference } from '@relaybus/core';
import { afterEach, beforeEach, describe } from 'vitest';
import { appendAndClaimContract } from './contract/append-and-claim.js';
import { fencingContract } from './contract/fencing-and-retries.js';
import { maintenanceContract } from './contract/queries-and-purge.js';
import { orderingAndLeasesContract } from './contract/ordering-and-leases.js';
import { completionContract } from './contract/completion.js';
import { lostLeasesContract } from './contract/lost-leases.js';
import { payloadFidelityContract } from './contract/payload-fidelity.js';
import { LEASE_MS, at } from './contract/contract-fixtures.js';
import type { ContractSuite } from './contract/contract-suite.js';

export { at, createDelivery, createEnvelope } from './contract/contract-fixtures.js';

export interface EventStoreContractOptions<TTransaction> {
  /** Creates an empty, set-up store. Called before every test. */
  readonly createStore: () => Promise<EventStore<TTransaction>>;
  /** Releases the store. Called after every test. */
  readonly destroyStore?: (store: EventStore<TTransaction>) => Promise<void>;
  /**
   * Creates a second store instance on the same database with its own
   * connections, simulating another process. Enables cross-instance tests.
   * Stores without shared state across instances (in-memory) omit it.
   */
  readonly createPeerStore?: (
    primary: EventStore<TTransaction>,
  ) => Promise<EventStore<TTransaction>>;
  /**
   * Whether the stores `createStore` returns keep a record of completed
   * deliveries (completion history). Default false: a completed delivery is
   * deleted. Run the contract once for each mode a store supports.
   */
  readonly keepsCompletedRecords?: boolean;
}

/**
 * Behaviour every {@link EventStore} must have. Call it inside a test file of
 * the store package:
 *
 * ```ts
 * eventStoreContract('PostgresEventStore', { createStore: async () => ... });
 * ```
 */
export function eventStoreContract<TTransaction>(
  name: string,
  options: EventStoreContractOptions<TTransaction>,
): void {
  const keepsCompleted = options.keepsCompletedRecords ?? false;
  describe(`${name} satisfies the EventStore contract`, () => {
    let store: EventStore<TTransaction>;

    beforeEach(async () => {
      store = await options.createStore();
    });

    afterEach(async () => {
      await options.destroyStore?.(store);
    });

    async function append(entries: AppendEntry[], now: Date = at(0)) {
      return store.transaction((transaction) => store.append(transaction, entries, now));
    }

    /** `completeMany` is optional on the port: stores without it complete one by one, in order. */
    async function completeMany(
      target: EventStore<TTransaction>,
      leases: readonly LeaseReference[],
      now: Date,
    ): Promise<boolean[]> {
      if (target.completeMany !== undefined) {
        return target.completeMany(leases, now);
      }
      const outcomes: boolean[] = [];
      for (const lease of leases) {
        outcomes.push(await target.complete(lease, now));
      }
      return outcomes;
    }

    async function claim(
      handlers: string[],
      now: Date = at(0),
      limit = 100,
      owner = 'worker-1',
    ): Promise<ClaimedDelivery[]> {
      return store.claim({
        handlers,
        limit,
        leaseOwner: owner,
        leaseExpiresAt: new Date(now.getTime() + LEASE_MS),
        now,
      });
    }

    const suite: ContractSuite<TTransaction> = {
      options,
      keepsCompleted,
      store: () => store,
      append,
      claim,
      completeMany,
    };
    appendAndClaimContract(suite);
    fencingContract(suite);
    maintenanceContract(suite);
    orderingAndLeasesContract(suite);
    completionContract(suite);
    lostLeasesContract(suite);
    payloadFidelityContract(suite);
  });
}
