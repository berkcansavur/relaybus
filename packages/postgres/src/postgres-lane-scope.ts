import { ConfigurationError } from '@relaybus/core';
import type { PostgresExecutor, PostgresRow } from './executor.js';
import { LANE_COUNT, MAXIMUM_LANE, type PostgresNames } from './names.js';
import { setLaneBlockStart } from './sql-fragments.js';

/** SQLSTATE of a column that does not exist. */
const UNDEFINED_COLUMN = '42703';

/**
 * Gives a store with a lane scope other than the default its block of {@link LANE_COUNT} lanes (see
 * {@link PostgresSchemaOptions.laneScope}). The block is allocated once, on first use, in a
 * transaction of its own: the next lane numbers after the highest lane, never below
 * {@link LANE_COUNT}, under an advisory lock that serializes the stores sharing the lane table. A
 * block that already exists, one allocated by the database owner included, is only read. The
 * default scope owns lanes 0 to 255, which the schema seeds, and needs nothing.
 */
export class PostgresLaneScope<TTransaction> {
  readonly #executor: PostgresExecutor<TTransaction>;
  readonly #names: PostgresNames;
  #ready: Promise<void> | undefined;
  #known = false;

  constructor(executor: PostgresExecutor<TTransaction>, names: PostgresNames) {
    this.#executor = executor;
    this.#names = names;
  }

  /**
   * Resolves once the store's scope has its lanes and the statements know the first of them; a failure
   * is retried by the next call. Looks the block up in `transaction` when given, never waiting for a
   * lookup of another call, so a publish inside a caller's transaction needs no second connection once
   * the block exists; only allocating a block takes a transaction of its own.
   */
  ensure(transaction?: TTransaction): Promise<void> {
    const scope = this.#names.laneScope;
    if (scope === undefined || this.#known) {
      return Promise.resolve();
    }
    if (transaction !== undefined) {
      return this.#resolve(scope, transaction);
    }
    this.#ready ??= this.#resolve(scope, undefined).catch((error: unknown) => {
      this.#ready = undefined;
      throw error;
    });
    return this.#ready;
  }

  async #resolve(scope: string, transaction: TTransaction | undefined): Promise<void> {
    const block = await this.#block(scope, transaction);
    setLaneBlockStart(this.#names, isBlock(block) ? block.first : await this.#allocate(scope));
    this.#known = true;
  }

  /** The first lane of the scope's block, allocating the block unless another process just did. */
  async #allocate(scope: string): Promise<number> {
    return this.#executor.transaction(async (transaction) => {
      await this.#executor.query(
        'SELECT true AS locked FROM pg_advisory_xact_lock(hashtextextended($1::text, 0))',
        [`relaybus-lane-scopes:${this.#names.lanes}`],
        transaction,
      );
      const block = await this.#block(scope, transaction);
      if (isBlock(block)) {
        return block.first;
      }
      if (block.lanes !== 0) {
        throw new ConfigurationError(
          `lane scope '${scope}' has ${block.lanes} lanes in ${this.#names.lanes}, not a block of ${LANE_COUNT}`,
        );
      }
      const inserted = await this.#executor.query(
        `INSERT INTO ${this.#names.lanes} (lane, scope)
SELECT (free.first + block_lane.offset_in_block)::smallint, $1::text
FROM (SELECT GREATEST(COALESCE(max(lane)::int + 1, ${LANE_COUNT}), ${LANE_COUNT}) AS first FROM ${this.#names.lanes}) AS free
CROSS JOIN generate_series(0, ${LANE_COUNT - 1}) AS block_lane(offset_in_block)
WHERE free.first + ${LANE_COUNT - 1} <= ${MAXIMUM_LANE}
RETURNING lane`,
        [scope],
        transaction,
      );
      if (inserted.length !== LANE_COUNT) {
        throw new ConfigurationError(
          `${this.#names.lanes} has no room for the ${LANE_COUNT} lanes of scope '${scope}'`,
        );
      }
      return Math.min(...inserted.map((row) => Number(row['lane'])));
    });
  }

  async #block(scope: string, transaction?: TTransaction): Promise<LaneBlock> {
    let postgresRows: readonly PostgresRow[];
    try {
      postgresRows = await this.#executor.query(
        `SELECT count(*)::int AS lanes, min(lane)::int AS first, max(lane)::int AS last FROM ${this.#names.lanes} WHERE scope = $1::text`,
        [scope],
        transaction,
      );
    } catch (error) {
      if ((error as { code?: unknown }).code === UNDEFINED_COLUMN) {
        throw new ConfigurationError(
          `${this.#names.lanes} has no scope column: run setup() or the lane scope upgrade before using laneScope '${scope}'`,
        );
      }
      throw error;
    }
    const row = postgresRows[0]!;
    return { lanes: Number(row['lanes']), first: Number(row['first']), last: Number(row['last']) };
  }
}

interface LaneBlock {
  readonly lanes: number;
  readonly first: number;
  readonly last: number;
}

/** A whole block: {@link LANE_COUNT} consecutive lanes. */
function isBlock(laneBlock: LaneBlock): boolean {
  return laneBlock.lanes === LANE_COUNT && laneBlock.last - laneBlock.first === LANE_COUNT - 1;
}
