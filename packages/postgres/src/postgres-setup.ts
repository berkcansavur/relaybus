import { resolvePostgresNames, type PostgresSchemaOptions } from './names.js';
import { type PostgresStoreState } from './postgres-store-state.js';
import { upgradePayloadToJson, version040SchemaMarker } from './schema.js';

/**
 * Brings the schema to the current statements under the setup lock (see
 * {@link PostgresEventStore.setup}): first the payload conversion in a transaction of its own, then
 * the other statements, unless the schema became current, or was one 0.4.0 created, which needs
 * only the conversion.
 */
export class PostgresSetup<TTransaction> {
  readonly #state: PostgresStoreState<TTransaction>;
  /** The conversion of `jsonb` payload columns. */
  readonly #payloadUpgrade: string;
  /** The marker of a schema 0.4.0 created, which needs only {@link #payloadUpgrade}. */
  readonly #version040Marker: string;

  constructor(state: PostgresStoreState<TTransaction>, options: PostgresSchemaOptions) {
    this.#state = state;
    this.#payloadUpgrade = upgradePayloadToJson(resolvePostgresNames(options));
    this.#version040Marker = version040SchemaMarker(options);
  }

  /** Implements {@link PostgresEventStore.setup}. */
  async run(): Promise<void> {
    // DDL on a current schema would still lock the tables, and those locks deadlock with running claims.
    if (await this.#schemaIsCurrent()) {
      return;
    }
    await this.#underSetupLock(async (transaction) => {
      if (!(await this.#upgradeVersion040(transaction))) {
        await this.#run(this.#payloadUpgrade, transaction);
      }
    });
    await this.#underSetupLock(async (transaction) => {
      // A 0.4.0 setup may have created the schema since the first step released the lock.
      if (await this.#upgradeVersion040(transaction)) {
        return;
      }
      for (const statement of this.#state.schemaStatements) {
        await this.#run(statement, transaction);
      }
    });
  }

  /**
   * Upgrades a schema 0.4.0 created, which needs only the payload conversion: converts and moves the
   * marker on, so no other schema statement runs. False for any other schema.
   */
  async #upgradeVersion040(transaction: TTransaction): Promise<boolean> {
    if (!(await this.#schemaIsCurrent(transaction, this.#version040Marker))) {
      return false;
    }
    await this.#run(this.#payloadUpgrade, transaction);
    await this.#run(this.#state.schemaStatements.at(-1)!, transaction);
    return true;
  }

  /**
   * Runs `work` in a transaction under the setup lock, unless the schema became current. The
   * payload conversion goes first in a transaction of its own, so it holds no other table lock
   * while it waits for the event table (a completion locks deliveries before the event rows).
   */
  async #underSetupLock(work: (transaction: TTransaction) => Promise<void>): Promise<void> {
    await this.#state.executor.transaction(async (transaction) => {
      await this.#state.executor.query(
        this.#state.statements.setup.setupLock,
        [this.#state.setupLockKey],
        transaction,
      );
      if (!(await this.#schemaIsCurrent(transaction))) {
        await work(transaction);
      }
    });
  }

  async #run(statement: string, transaction: TTransaction): Promise<void> {
    await this.#state.executor.query(statement, [], transaction);
  }

  async #schemaIsCurrent(
    transaction?: TTransaction,
    marker: string = this.#state.schemaMarker,
  ): Promise<boolean> {
    const rows = await this.#state.executor.query(
      this.#state.statements.setup.schemaCurrent,
      [marker],
      transaction,
    );
    return rows[0]!['current'] === true;
  }
}
