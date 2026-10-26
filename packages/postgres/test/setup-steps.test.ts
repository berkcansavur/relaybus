import { describe, expect, it } from 'vitest';
import {
  PostgresEventStore,
  createPostgresSchemaStatements,
  type PostgresExecutor,
} from '../src/index.js';
import { version040SchemaMarker } from '../src/schema.js';

const MARKER = /'(relaybus schema [0-9a-f]+)'/;

/** A schema as the setup check sees it: the lane table comment and whether the payload is `json`. */
type Schema = { comment: string | null; payloadJson: boolean };

/** A schema 0.4.0 created: its marker, and `jsonb` payload columns. */
const VERSION_040: Schema = { comment: version040SchemaMarker(), payloadJson: false };

/**
 * An executor that answers the schema check from a simulated lane table comment and payload column
 * type, and records the other statements of each transaction. `beforeTransaction` runs before the
 * n-th transaction starts, as another process holding the setup lock in between would.
 */
function scriptedDatabase(
  initial: Schema,
  beforeTransaction: (index: number) => Schema | null = () => null,
) {
  let schema = { ...initial };
  const transactions: string[][] = [];
  const executor: PostgresExecutor<string[]> = {
    async transaction(work) {
      schema = beforeTransaction(transactions.length) ?? schema;
      const statements: string[] = [];
      transactions.push(statements);
      return work(statements);
    },
    async query(sql, parameters, transaction) {
      if (sql.includes('AS payload_json')) {
        return [{ marked: parameters[0] === schema.comment, payload_json: schema.payloadJson }];
      }
      if (sql.startsWith('DO $relaybus_payload_json$')) {
        schema = { ...schema, payloadJson: true };
      }
      if (sql.startsWith('COMMENT ON TABLE')) {
        schema = { ...schema, comment: MARKER.exec(sql)![1]! };
      }
      if (!sql.includes('pg_advisory_xact_lock')) {
        transaction?.push(sql.split('\n')[0]!);
      }
      return [];
    },
  };
  return { executor, transactions, schema: () => schema };
}

describe('PostgresEventStore setup steps', () => {
  it('writes the marker 0.4.0 writes', () => {
    expect(MARKER.exec(createPostgresSchemaStatements().at(-1)!)![1]).toBe(
      version040SchemaMarker(),
    );
  });

  it('does nothing and takes no lock on a marked schema with json payload columns', async () => {
    const database = scriptedDatabase({ ...VERSION_040, payloadJson: true });
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions).toEqual([]);
  });

  it('on a 0.4.0 schema only converts the payload columns and keeps the marker', async () => {
    const database = scriptedDatabase(VERSION_040);
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions).toEqual([['DO $relaybus_payload_json$'], []]);
    expect(database.schema()).toEqual({ ...VERSION_040, payloadJson: true });
  });

  it('recognizes a 0.4.0 schema that another process created between the two steps', async () => {
    const database = scriptedDatabase({ comment: null, payloadJson: false }, (index) =>
      index === 1 ? VERSION_040 : null,
    );
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions).toEqual([
      ['DO $relaybus_payload_json$'],
      ['DO $relaybus_payload_json$'],
    ]);
    expect(database.schema()).toEqual({ ...VERSION_040, payloadJson: true });
  });

  it('runs every schema statement on a new or older schema', async () => {
    const database = scriptedDatabase({ comment: null, payloadJson: false });
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions[0]).toEqual(['DO $relaybus_payload_json$']);
    expect(database.transactions[1]).toHaveLength(createPostgresSchemaStatements().length);
    expect(database.schema()).toEqual({ ...VERSION_040, payloadJson: true });
  });

  it('runs every schema statement when an older schema has json payload columns', async () => {
    const database = scriptedDatabase({ comment: 'relaybus schema older', payloadJson: true });
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions[1]).toHaveLength(createPostgresSchemaStatements().length);
  });
});
