import type { MongoClient } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { LANE_COUNT } from '../src/lanes.js';
import { MongoEventStore } from '../src/index.js';

interface FakeIndex {
  readonly name: string;
  readonly key: Record<string, number>;
}

function createClient(
  options: {
    createCollection?: () => Promise<unknown>;
    indexes?: readonly FakeIndex[];
    dropIndex?: (name: string) => Promise<unknown>;
  } = {},
): MongoClient {
  const database = {
    collection: (name: string) => ({
      collectionName: name,
      createIndexes: async () => [],
      // An upgraded database: every lane exists, so setup() skips the 0.1.0 upgrade.
      countDocuments: async () => LANE_COUNT,
      indexes: async () => options.indexes ?? [],
      dropIndex: options.dropIndex ?? (async () => ({})),
    }),
    command: async () => ({ setName: 'rs0' }),
    createCollection: options.createCollection ?? (async () => ({})),
  };
  return { db: () => database } as unknown as MongoClient;
}

function createClientFailingToCreateCollections(failure: unknown): MongoClient {
  return createClient({
    createCollection: async () => {
      throw failure;
    },
  });
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

describe('obsolete indexes', () => {
  const obsolete: FakeIndex[] = [
    { name: '_id_', key: { _id: 1 } },
    { name: 'running', key: { handler: 1, key: 1, dueAt: 1 } },
    { name: 'claim', key: { handler: 1, dueAt: 1, _id: 1 } },
    { name: 'ordering', key: { handler: 1, key: 1, sequence: 1 } },
  ];

  it('drops the 0.1.0 indexes and keeps the current ones', async () => {
    const dropIndex = vi.fn(async () => ({}));
    const store = new MongoEventStore({
      client: createClient({
        indexes: [...obsolete, { name: 'claim', key: { laneId: 1, dueAt: 1, _id: 1, handler: 1 } }],
        dropIndex,
      }),
    });
    await store.setup();
    expect(dropIndex.mock.calls).toEqual([['running'], ['claim']]);
  });

  it('tolerates an index that another process dropped first', async () => {
    const notFound = Object.assign(new Error('index not found with name [running]'), { code: 27 });
    const store = new MongoEventStore({
      client: createClient({
        indexes: obsolete,
        dropIndex: async () => {
          throw notFound;
        },
      }),
    });
    await expect(store.setup()).resolves.toBeUndefined();
  });

  it('rethrows any other failure to drop an index', async () => {
    const unauthorized = Object.assign(new Error('not authorized'), { code: 13 });
    const store = new MongoEventStore({
      client: createClient({
        indexes: obsolete,
        dropIndex: async () => {
          throw unauthorized;
        },
      }),
    });
    await expect(store.setup()).rejects.toBe(unauthorized);
  });
});
