import { resolvePostgresNames, type PostgresSchemaOptions } from './names.js';
import { type PostgresStoreState } from './postgres-store-state.js';
import { upgradePayloadToJson } from './schema.js';

/** What the catalog says about the schema (see {@link SetupStatements.schemaCurrent}). */
type SchemaState = {
  /** The lane table carries the schema marker, which 0.4.0 writes too. */
  readonly marked: boolean;
  /** The payload and headers columns are `json`. */
  readonly payloadJson: boolean;
};

/**
 * Brings the schema to the current statements under the setup lock (see
 * {@link PostgresEventStore.setup}): first the payload conversion in a transaction of its own, then
 * the other statements, unless the schema carries the marker, which 0.4.0 writes too and which then
 * needs only the conversion.
 */
export class PostgresSetup<TTransaction> {
  readonly #state: PostgresStoreState<TTransaction>;
  /** The conversion of `jsonb` payload columns. */
  readonly #payloadUpgrade: string;

  constructor(state: PostgresStoreState<TTransaction>, options: PostgresSchemaOptions) {
    this.#state = state;
    this.#payloadUpgrade = upgradePayloadToJson(resolvePostgresNames(options));
  }

  /** Implements {@link PostgresEventStore.setup}. */
  async run(): Promise<void> {
    // DDL on a current schema would still lock the tables, and those locks deadlock with running claims.
    if (isCurrent(await this.#schemaState())) {
      return;
    }
    await this.#underSetupLock(async (transaction) => {
      await this.#run(this.#payloadUpgrade, transaction);
    });
    await this.#underSetupLock(async (transaction, schema) => {
      // A 0.4.0 setup may have created the schema since the first step released the lock.
      if (schema.marked) {
        await this.#run(this.#payloadUpgrade, transaction);
        return;
      }
      for (const statement of this.#state.schemaStatements) {
        await this.#run(statement, transaction);
      }
    });
  }

  /**
   * Runs `work` in a transaction under the setup lock, unless the schema became current. The
   * payload conversion goes first in a transaction of its own, so it holds no other table lock
   * while it waits for the event table (a completion locks deliveries before the event rows).
   */
  async #underSetupLock(
    work: (transaction: TTransaction, schema: SchemaState) => Promise<void>,
  ): Promise<void> {
    await this.#state.executor.transaction(async (transaction) => {
      await this.#state.executor.query(
        this.#state.statements.setup.setupLock,
        [this.#state.setupLockKey],
        transaction,
      );
      const schema = await this.#schemaState(transaction);
      if (!isCurrent(schema)) {
        await work(transaction, schema);
      }
    });
  }

  async #run(statement: string, transaction: TTransaction): Promise<void> {
    await this.#state.executor.query(statement, [], transaction);
  }

  async #schemaState(transaction?: TTransaction): Promise<SchemaState> {
    const rows = await this.#state.executor.query(
      this.#state.statements.setup.schemaCurrent,
      [this.#state.schemaMarker],
      transaction,
    );
    return { marked: rows[0]!['marked'] === true, payloadJson: rows[0]!['payload_json'] === true };
  }
}

/** Current: the marker is there and the payload columns are converted. */
function isCurrent(schema: SchemaState): boolean {
  return schema.marked && schema.payloadJson;
}
