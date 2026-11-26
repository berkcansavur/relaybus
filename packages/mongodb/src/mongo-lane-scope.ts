import { ConfigurationError } from '@relaybus/core';
import type { Collection } from 'mongodb';
import type { LaneDocument } from './documents.js';
import { LANE_COUNT, MAXIMUM_LANE } from './lanes.js';

/** Server error code of a write that broke a unique index. */
const DUPLICATE_KEY = 11_000;

/** Name of the unique index that lets each scope have one block only. */
const SCOPE_HEAD_INDEX = 'scope_head';

/** A lane nobody ever held. */
const FREE_LANE = {
  epoch: 0,
  owner: null,
  holder: null,
  leaseExpiresAt: null,
  inFlight: [],
  lost: [],
} as const;

/**
 * The lanes of a store's lane scope (see `MongoEventStoreOptions.laneScope`). The default scope
 * owns lanes 0 to 255, which `setup()` seeds. Any other scope owns a block of {@link LANE_COUNT}
 * lanes starting at a multiple of {@link LANE_COUNT}, allocated on first use: the block's first lane
 * document carries `scopeHead`, which a unique index keeps to one block per scope, so stores that
 * allocate concurrently agree on one block. A block whose first lane exists but whose other lanes
 * do not (a crash in between) is completed by the next store of the scope.
 */
export class MongoLaneScope {
  readonly #lanes: Collection<LaneDocument>;
  readonly #scope: string | undefined;
  #firstLane = 0;
  #ready: Promise<void> | undefined;

  /** `scope` is undefined for the default scope. */
  constructor(lanes: Collection<LaneDocument>, scope: string | undefined) {
    this.#lanes = lanes;
    this.#scope = scope;
  }

  /** First lane of the scope: 0 for the default scope, known after {@link ensure} resolved. */
  get firstLane(): number {
    return this.#firstLane;
  }

  /**
   * Resolves once the store's scope has its lanes; a failure is retried by the next call. Runs outside
   * any session: a transaction that read the lanes collection would make the index build of a first
   * allocation wait for it, while it waits for the allocation.
   */
  ensure(): Promise<void> {
    if (this.#scope === undefined) {
      return Promise.resolve();
    }
    this.#ready ??= this.#allocate(this.#scope).catch((error: unknown) => {
      this.#ready = undefined;
      throw error;
    });
    return this.#ready;
  }

  async #allocate(scope: string): Promise<void> {
    await this.#lanes.createIndex(
      { scopeHead: 1 },
      {
        name: SCOPE_HEAD_INDEX,
        unique: true,
        partialFilterExpression: { scopeHead: { $exists: true } },
      },
    );
    for (;;) {
      const head = await this.#lanes.findOne({ scopeHead: scope }, { projection: { _id: 1 } });
      if (head !== null) {
        await this.#complete(head._id, scope);
        this.#firstLane = head._id;
        return;
      }
      const last = await this.#lanes.findOne({}, { sort: { _id: -1 }, projection: { _id: 1 } });
      const first = Math.max(
        LANE_COUNT,
        (Math.floor((last?._id ?? 0) / LANE_COUNT) + 1) * LANE_COUNT,
      );
      if (first + LANE_COUNT - 1 > MAXIMUM_LANE) {
        throw new ConfigurationError(
          `${this.#lanes.collectionName} has no room for the ${LANE_COUNT} lanes of scope '${scope}'`,
        );
      }
      try {
        await this.#lanes.insertOne({ _id: first, scope, scopeHead: scope, ...FREE_LANE });
      } catch (error) {
        // Another store took this block or allocated this scope first: look again.
        if (!hasErrorCode(error, DUPLICATE_KEY)) {
          throw error;
        }
      }
    }
  }

  /** Creates the lanes of the block that do not exist yet. */
  async #complete(first: number, scope: string): Promise<void> {
    await this.#lanes.bulkWrite(
      Array.from({ length: LANE_COUNT - 1 }, (_, index) => ({
        updateOne: {
          filter: { _id: first + index + 1 },
          update: { $setOnInsert: { scope, ...FREE_LANE } },
          upsert: true,
        },
      })),
      { ordered: false },
    );
  }
}

function hasErrorCode(error: unknown, code: number): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === code;
}
