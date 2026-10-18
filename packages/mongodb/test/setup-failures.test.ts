import type { MongoClient } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { MongoEventStore } from '../src/index.js';

function createClientFailingToCreateCollections(failure: unknown): MongoClient {
  const database = {
    collection: (name: string) => ({ collectionName: name, createIndexes: async () => [] }),
    command: async () => ({ setName: 'rs0' }),
    createCollection: async () => {
      throw failure;
    },
  };
  return { db: () => database } as unknown as MongoClient;
}

describe('setup failures', () => {
  it.each([
    new Error('not authorized'),
    Object.assign(new Error('not authorized'), { code: 13 }),
    'plain string',
    null,
  ])('rethrows a collection creation error that is not "already exists": %j', async (failure) => {
    const store = new MongoEventStore({ client: createClientFailingToCreateCollections(failure) });
    await expect(store.setup()).rejects.toBe(failure);
  });

  it('accepts a collection that already exists, as MongoDB 6 reports it', async () => {
    const exists = Object.assign(new Error('collection already exists'), { code: 48 });
    const store = new MongoEventStore({ client: createClientFailingToCreateCollections(exists) });
    await expect(store.setup()).resolves.toBeUndefined();
  });
});
