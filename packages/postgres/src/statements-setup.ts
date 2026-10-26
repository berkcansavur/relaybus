import type { PostgresNames } from './names.js';

/** Statements of `setup`: the setup lock and the schema check. */
export class SetupStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
  }

  /**
   * Lock advisory key text for {@link setupLock}. It is derived from the schema
   * name only, so stores with different table prefixes in one schema also
   * serialize their `CREATE SCHEMA IF NOT EXISTS`, which is not race-free.
   */
  get setupLockKey(): string {
    return `relaybus:${this.#names.schema}`;
  }

  /**
   * The state of the schema, taking no table lock. Parameters: `[marker]`. Columns: `marked`, whether
   * the lane table carries `marker` as its comment (the schema statements that produce it already
   * ran), and `payload_json`, whether the payload and headers columns of the event table are both
   * `json` (the conversion of the `jsonb` columns of schemas before 0.4.1 ran).
   */
  get schemaCurrent(): string {
    // Reads the catalogs with the statement's snapshot. `to_regclass` and `obj_description` would go
    // through the backend's catalog caches, which keep "no such table" until the session processes
    // invalidations; a setup that waited for the setup lock would then miss a schema created meanwhile.
    return `SELECT EXISTS (
  SELECT 1 FROM pg_catalog.pg_class AS c
  JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_description AS d
    ON d.objoid = c.oid AND d.classoid = 'pg_catalog.pg_class'::regclass AND d.objsubid = 0
  WHERE n.nspname = '${this.#names.schema}' AND c.relname = '${this.#names.lanesTableName}' AND d.description = $1::text
) AS marked, (
  SELECT count(*) FROM pg_catalog.pg_attribute AS a
  JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
  JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
  WHERE n.nspname = '${this.#names.schema}' AND c.relname = '${this.#names.eventsTableName}'
    AND a.attname IN ('payload', 'headers') AND a.atttypid = 'pg_catalog.json'::pg_catalog.regtype
    AND NOT a.attisdropped
) = 2 AS payload_json`;
  }

  /**
   * Takes the setup lock. Returns a boolean column rather than the `void` of the lock function, which
   * some libraries cannot read (Prisma's raw queries reject a `void` column).
   */
  get setupLock(): string {
    return 'SELECT true AS locked FROM pg_advisory_xact_lock(hashtextextended($1::text, 0))';
  }
}
