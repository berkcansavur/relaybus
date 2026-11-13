import { describe, expect, it } from 'vitest';
import { createPostgresSchemaSql } from '../src/index.js';

describe('schema lock', () => {
  it('keeps the DDL of the default options', async () => {
    await expect(createPostgresSchemaSql()).toMatchFileSnapshot('../etc/schema.sql');
  });

  it('keeps the schema marker of 0.5.0', () => {
    expect(createPostgresSchemaSql()).toContain(
      `COMMENT ON TABLE "public"."relaybus_lanes" IS 'relaybus schema ae44fe2cd55d06183921bc5e2bb0db92'`,
    );
  });

  it('keeps the DDL with every option set', async () => {
    const sql = createPostgresSchemaSql({
      schema: 'events',
      tablePrefix: 'bus_',
      completionHistory: true,
    });
    await expect(sql).toMatchFileSnapshot('../etc/schema-all-options.sql');
  });
});
