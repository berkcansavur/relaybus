import { inject } from 'vitest';
import { MongoClient } from 'mongodb';
import { createUuidV7 } from '@relaybus/core';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';

import { MONGO_IMAGE } from './mongo-server.js';

export interface MongoReplicaSet {
  readonly connectionString: string;
  connect(): Promise<MongoClient>;
  stop(): Promise<void>;
}

/**
 * Connects to the single-node replica set that the global setup started for this run,
 * the smallest deployment that supports transactions. Every test file shares it and
 * stays apart through `uniqueDatabaseName()`.
 */
export async function startReplicaSet(): Promise<MongoReplicaSet> {
  const connectionString = inject('mongoConnectionString');
  return {
    connectionString,
    connect: () =>
      new MongoClient(connectionString, {
        directConnection: true,
        monitorCommands: true,
      }).connect(),
    stop: async () => {},
  };
}

/** Starts a dedicated plain mongod without replication, which cannot run transactions. */
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
