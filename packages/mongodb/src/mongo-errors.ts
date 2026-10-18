import { ConfigurationError } from '@relaybus/core';

const ILLEGAL_OPERATION = 20;

/** True for the server error raised when a transaction runs on a standalone mongod. */
export function isTransactionsUnsupportedError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    error.code === ILLEGAL_OPERATION &&
    'message' in error &&
    typeof error.message === 'string' &&
    error.message.includes('replica set member or mongos')
  );
}

export function createTransactionsUnsupportedError(cause: unknown): ConfigurationError {
  return new ConfigurationError(
    'MongoEventStore needs multi-document transactions, which MongoDB only offers on a replica set or a sharded cluster. Run a single-node replica set for development (mongod --replSet rs0, then rs.initiate())',
    { cause },
  );
}
