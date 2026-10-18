import type { MongoClient } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { ConfigurationError } from '@relaybus/core';
import { MongoEventStore } from '../src/index.js';
import {
  createTransactionsUnsupportedError,
  isTransactionsUnsupportedError,
} from '../src/mongo-errors.js';

const unusedClient = {} as MongoClient;

describe('MongoEventStore options', () => {
  it.each(['', '1events', 'has space', 'dollar$', 'dotted.name', 'x'.repeat(41), 'ümlaut'])(
    'rejects the collection prefix %j',
    (collectionPrefix) => {
      expect(() => new MongoEventStore({ client: unusedClient, collectionPrefix })).toThrow(
        ConfigurationError,
      );
    },
  );

  it.each(['', 'has space', 'a/b', 'a.b', 'a$b', 'x'.repeat(64)])(
    'rejects the database name %j',
    (database) => {
      expect(() => new MongoEventStore({ client: unusedClient, database })).toThrow(
        ConfigurationError,
      );
    },
  );

  it.each([0, -1, 1.5, Number.NaN])('rejects the purge batch size %j', (purgeBatchSize) => {
    expect(() => new MongoEventStore({ client: unusedClient, purgeBatchSize })).toThrow(
      /purgeBatchSize/,
    );
  });
});

describe('transaction support detection', () => {
  const standaloneError = Object.assign(
    new Error('Transaction numbers are only allowed on a replica set member or mongos'),
    { code: 20 },
  );

  it('recognizes the standalone mongod error', () => {
    expect(isTransactionsUnsupportedError(standaloneError)).toBe(true);
  });

  it.each([
    null,
    'text',
    new Error('other'),
    Object.assign(new Error('replica set member or mongos'), { code: 11000 }),
    Object.assign(new Error('Transaction failed'), { code: 20 }),
    { code: 20 },
  ])('ignores %j', (error) => {
    expect(isTransactionsUnsupportedError(error)).toBe(false);
  });

  it('explains how to get a replica set and keeps the cause', () => {
    const error = createTransactionsUnsupportedError(standaloneError);
    expect(error).toBeInstanceOf(ConfigurationError);
    expect(error.message).toContain('replica set');
    expect(error.cause).toBe(standaloneError);
  });
});
