import { type RelayBusLogger } from '@relaybus/core';
import { type Pool } from 'pg';
import type {
  PostgresExecutor,
  PostgresParameter,
  PostgresQueryOptions,
  PostgresRow,
} from './executor.js';
import { type LaneLease } from './row-mapping.js';
import { type PostgresStatements } from './statements.js';

interface HandedOutClaim extends LaneLease {
  readonly owner: string;
  readonly deliveryId: string;
}

/** The connection, statements, settings and per-instance bookkeeping every part of {@link PostgresEventStore} shares. */
export type PostgresStoreState<TTransaction> = {
  readonly executor: PostgresExecutor<TTransaction>;
  readonly statements: PostgresStatements;
  readonly schemaStatements: readonly string[];
  readonly setupLockKey: string;
  readonly schemaMarker: string;
  readonly purgeBatchSize: number;
  readonly completionHistory: boolean;
  readonly logger: RelayBusLogger;
  /** Passed with every statement whose text is fixed: asks the executor to prepare it (`preparedStatements`). */
  readonly queryOptions: PostgresQueryOptions | undefined;
  readonly notifies: boolean;
  /** Runs a statement of {@link PostgresStatements} whose text never changes, prepared when `preparedStatements` is on. */
  readonly query: (input: {
    readonly sql: string;
    readonly parameters: readonly PostgresParameter[];
    readonly transaction?: TTransaction | undefined;
  }) => Promise<readonly PostgresRow[]>;
  /** Every claim handed to a caller and not settled yet, by delivery id and exact lease token. */
  readonly handedOut: Map<string, HandedOutClaim>;
  readonly unreconciled: Set<string>;
  readonly generations: Map<string, bigint>;
  readonly ownerQueues: Map<string, Promise<void>>;
  /** Owners whose last claim was filled from the lanes they held. */
  readonly drainingOwners: Set<string>;
  ownedPool: Pool | undefined;
  /** The `pg` pool behind the store, when it has one: listening for notifications needs a connection of its own. */
  readonly pool: Pool | undefined;
  readonly notificationChannel: string;
  lastNotificationAt: number;
  /** Cancels the wake-up notification scheduled after a commit; set while one is pending. */
  trailingNotification: (() => void) | undefined;
  /** Set by `close()`: a transaction that commits afterwards schedules no wake-up. */
  closed: boolean;
  /** The transactions `transaction` runs right now, and whether one of their appends left its wake-up for after the commit. */
  readonly ownTransactions: Map<unknown, { wake: boolean }>;
};
