import type { MongoClient } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { LANE_COUNT } from '../src/lanes.js';
import { MongoEventStore, type MongoEventStoreOptions } from '../src/index.js';

async function recordSetup(options: Omit<MongoEventStoreOptions, 'client'>): Promise<string> {
  const collections: unknown[] = [];
  const indexes: Record<string, unknown[]> = {};
  const database = {
    collection: (name: string) => ({
      collectionName: name,
      createIndexes: async (specs: unknown) => {
        (indexes[name] ??= []).push(specs);
        return [];
      },
      countDocuments: async () => LANE_COUNT,
      indexes: async () => [],
      dropIndex: async () => ({}),
    }),
    command: async () => ({ setName: 'rs0' }),
    createCollection: async (name: string, collectionOptions: unknown) => {
      collections.push(collectionOptions === undefined ? name : [name, collectionOptions]);
      return {};
    },
  };
  const client = { db: () => database } as unknown as MongoClient;
  await new MongoEventStore({ ...options, client }).setup();
  return `${JSON.stringify({ collections, indexes }, null, 2)}\n`;
}

describe('schema lock', () => {
  it('keeps the collections and indexes of the default options', async () => {
    await expect(await recordSetup({})).toMatchFileSnapshot('../etc/schema.json');
  });

  it('keeps the collections and indexes with every schema option set', async () => {
    const recorded = await recordSetup({ collectionPrefix: 'bus_', completionHistory: true });
    await expect(recorded).toMatchFileSnapshot('../etc/schema-all-options.json');
  });
});
