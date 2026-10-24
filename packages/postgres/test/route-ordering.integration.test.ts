import { afterAll, beforeAll } from 'vitest';
import { describeRouteOrdering } from '../../testing/test/support/route-ordering.suite.js';
import { PostgresEventStore } from '../src/index.js';
import {
  dropSchema,
  startDatabase,
  uniqueSchema,
  type TestDatabase,
} from './support/postgres-container.js';

let database: TestDatabase;
const schemas: string[] = [];

beforeAll(async () => {
  database = await startDatabase();
});

afterAll(async () => {
  await database.stop();
});

describeRouteOrdering({
  name: 'PostgreSQL',
  async createStore() {
    const schema = uniqueSchema();
    schemas.push(schema);
    return new PostgresEventStore({ pool: database.pool, schema });
  },
  async cleanup() {
    await Promise.all(schemas.splice(0).map((schema) => dropSchema(database.pool, schema)));
  },
});
