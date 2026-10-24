import { MongoDBContainer } from '@testcontainers/mongodb';

export const MONGO_IMAGE = 'mongo:7';

export interface MongoServer {
  readonly connectionString: string;
  stop(): Promise<void>;
}

/**
 * Starts the single-node replica set that every Mongo test file shares for one run (the
 * smallest deployment that supports transactions). The data directory lives on tmpfs;
 * files stay apart through `uniqueDatabaseName()`.
 */
export async function startMongoServer(): Promise<MongoServer> {
  const container = await new MongoDBContainer(MONGO_IMAGE).withTmpFs({ '/data/db': 'rw' }).start();
  return {
    connectionString: container.getConnectionString(),
    stop: async () => {
      await container.stop();
    },
  };
}
