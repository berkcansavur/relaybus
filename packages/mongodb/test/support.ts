import { MongoDBContainer, type StartedMongoDBContainer } from '@testcontainers/mongodb';
import { MongoClient } from 'mongodb';
import { createUuidV7 } from '@relaybus/core';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';

const MONGO_IMAGE = 'mongo:7';

export interface MongoReplicaSet {
  readonly connectionString: string;
  connect(): Promise<MongoClient>;
  stop(): Promise<void>;
}

/** Starts a single-node replica set, the smallest deployment that supports transactions. */
export async function startReplicaSet(): Promise<MongoReplicaSet> {
  const container: StartedMongoDBContainer = await new MongoDBContainer(MONGO_IMAGE).start();
  const connectionString = container.getConnectionString();
  return {
    connectionString,
    connect: () =>
      new MongoClient(connectionString, {
        directConnection: true,
        monitorCommands: true,
      }).connect(),
    stop: async () => {
      await container.stop();
    },
  };
}

/** Starts a plain mongod without replication, which cannot run transactions. */
export async function startStandalone(): Promise<{
  connect(): Promise<MongoClient>;
  stop(): Promise<void>;
}> {
  const container: StartedTestContainer = await new GenericContainer(MONGO_IMAGE)
    .withExposedPorts(27017)
    .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
    .start();
  const connectionString = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}`;
  return {
    connect: () =>
      new MongoClient(connectionString, {
        directConnection: true,
        monitorCommands: true,
      }).connect(),
    stop: async () => {
      await container.stop();
    },
  };
}

/** A database name no other test uses, so tests never see each other's data. */
export function uniqueDatabaseName(): string {
  return `relaybus_${createUuidV7().replaceAll('-', '').slice(-24)}`;
}
