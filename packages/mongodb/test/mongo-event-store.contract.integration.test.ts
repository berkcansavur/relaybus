import type { ClientSession, MongoClient } from 'mongodb';
import { afterAll, beforeAll } from 'vitest';
import type { EventStore } from '@relaybus/core';
import { eventStoreContract } from '@relaybus/testing';
import { MongoEventStore } from '../src/index.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

let replicaSet: MongoReplicaSet;
let sharedClient: MongoClient;
const databaseOfStore = new WeakMap<EventStore<ClientSession>, string>();
const clientOfPeer = new WeakMap<EventStore<ClientSession>, MongoClient>();

beforeAll(async () => {
  replicaSet = await startReplicaSet();
  sharedClient = await replicaSet.connect();
});

afterAll(async () => {
  await sharedClient.close();
  await replicaSet.stop();
});

for (const completionHistory of [false, true]) {
  eventStoreContract<ClientSession>(
    `MongoEventStore${completionHistory ? ' (completion history)' : ''}`,
    {
      keepsCompletedRecords: completionHistory,
      createStore: async () => {
        const database = uniqueDatabaseName();
        const store = new MongoEventStore({
          client: sharedClient,
          database,
          purgeBatchSize: 2,
          completionHistory,
        });
        await store.setup();
        databaseOfStore.set(store, database);
        return store;
      },
      createPeerStore: async (primary) => {
        const client = await replicaSet.connect();
        const peer = new MongoEventStore({
          client,
          database: databaseOfStore.get(primary)!,
          completionHistory,
        });
        clientOfPeer.set(peer, client);
        return peer;
      },
      destroyStore: async (store) => {
        await clientOfPeer.get(store)?.close();
      },
    },
  );
}
