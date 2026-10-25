import { createUuidV7 } from '@relaybus/core';
import { HandedOutLedger, PositiveInteger } from '@relaybus/core/internal';
import type { TransactionOptions } from 'mongodb';
import type { MongoEventStoreOptions } from './mongo-event-store.js';
import type { MongoStoreState } from './mongo-store-state.js';
import { assertCollectionPrefix, assertDatabaseName } from './naming.js';

const DEFAULT_TRANSACTION_OPTIONS: TransactionOptions = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
};
const DEFAULT_PURGE_BATCH_SIZE = 500;

/** The collections, settings and bookkeeping of a new store, from its options (checked in order). */
export function createMongoStoreState(options: MongoEventStoreOptions): MongoStoreState {
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
  return state;
}
