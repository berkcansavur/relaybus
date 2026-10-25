import type { MongoClient } from 'mongodb';
import { afterAll, beforeAll } from 'vitest';
import { describeRouteOrdering } from '../../testing/test/support/route-ordering.suite.js';
import { MongoEventStore } from '../src/index.js';
import { startReplicaSet, uniqueDatabaseName, type MongoReplicaSet } from './support.js';

let replicaSet: MongoReplicaSet;
let client: MongoClient;
const databases: string[] = [];

beforeAll(async () => {
  replicaSet = await startReplicaSet();
  client = await replicaSet.connect();
});

afterAll(async () => {
  await client.close();
  await replicaSet.stop();
});

describeRouteOrdering({
  name: 'MongoDB',
  async createStore() {
    const database = uniqueDatabaseName();
    databases.push(database);
    return new MongoEventStore({ client, database });
  },
  async cleanup() {
    await Promise.all(databases.splice(0).map((database) => client.db(database).dropDatabase()));
  },
});
