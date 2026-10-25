import type {
  AppendEntry,
  AppendResult,
  ClaimedDelivery,
  EventStore,
  LeaseReference,
} from '@relaybus/core';
import type { EventStoreContractOptions } from '../event-store-contract.js';

/** What every section of the store contract shares: the options, the store under test and its helpers. */
export interface ContractSuite<TTransaction> {
  readonly options: EventStoreContractOptions<TTransaction>;
  readonly keepsCompleted: boolean;
  /** The store of the running test, created before each test. */
  readonly store: () => EventStore<TTransaction>;
  readonly append: (entries: AppendEntry[], now?: Date) => Promise<AppendResult>;
  readonly claim: (
    handlers: string[],
    now?: Date,
    limit?: number,
    owner?: string,
  ) => Promise<ClaimedDelivery[]>;
  /** `completeMany` is optional on the port: stores without it complete one by one, in order. */
  readonly completeMany: (
    target: EventStore<TTransaction>,
    leases: readonly LeaseReference[],
    now: Date,
  ) => Promise<boolean[]>;
}
