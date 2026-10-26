import { describe, expect, it } from 'vitest';
import {
  PostgresEventStore,
  createPostgresSchemaStatements,
  type PostgresExecutor,
} from '../src/index.js';
import { version040SchemaMarker } from '../src/schema.js';

const MARKER = /'(relaybus schema [0-9a-f]+)'/;
const CURRENT_MARKER = MARKER.exec(createPostgresSchemaStatements().at(-1)!)![1]!;

/**
 * An executor that answers the schema check from a simulated lane table comment and records the
 * other statements of each transaction. `beforeTransaction` runs before the n-th transaction starts,
 * as another process holding the setup lock in between would.
 */
function scriptedDatabase(beforeTransaction: (index: number) => string | null) {
  let comment: string | null = null;
  const transactions: string[][] = [];
  const executor: PostgresExecutor<string[]> = {
    async transaction(work) {
      comment = beforeTransaction(transactions.length) ?? comment;
      const statements: string[] = [];
      transactions.push(statements);
      return work(statements);
    },
    async query(sql, parameters, transaction) {
      if (sql.includes('AS current')) {
        return [{ current: parameters[0] === comment }];
      }
      if (sql.startsWith('COMMENT ON TABLE')) {
        comment = MARKER.exec(sql)![1]!;
      }
      if (!sql.includes('pg_advisory_xact_lock')) {
        transaction?.push(sql.split('\n')[0]!);
      }
      return [];
    },
  };
  return { executor, transactions, comment: () => comment };
}

describe('PostgresEventStore setup steps', () => {
  it('on a 0.4.0 schema only converts the payload columns and moves the marker on', async () => {
    const database = scriptedDatabase((index) => (index === 0 ? version040SchemaMarker() : null));
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions).toEqual([
      ['DO $relaybus_payload_json$', expect.stringMatching(/^COMMENT ON TABLE/)],
      [],
    ]);
    expect(database.comment()).toBe(CURRENT_MARKER);
  });

  it('recognizes a 0.4.0 schema that another process created between the two steps', async () => {
    const database = scriptedDatabase((index) => (index === 1 ? version040SchemaMarker() : null));
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions).toEqual([
      ['DO $relaybus_payload_json$'],
      ['DO $relaybus_payload_json$', expect.stringMatching(/^COMMENT ON TABLE/)],
    ]);
    expect(database.comment()).toBe(CURRENT_MARKER);
  });

  it('runs every schema statement on a new or older schema', async () => {
    const database = scriptedDatabase(() => null);
    await new PostgresEventStore({ executor: database.executor }).setup();
    expect(database.transactions[0]).toEqual(['DO $relaybus_payload_json$']);
    expect(database.transactions[1]).toHaveLength(createPostgresSchemaStatements().length);
    expect(database.comment()).toBe(CURRENT_MARKER);
  });
});
