import type { PostgresNames } from './names.js';
import { AppendStatements } from './statements-append.js';
import { ClaimStatements } from './statements-claim.js';
import { CompletionStatements } from './statements-completion.js';
import { LaneStatements } from './statements-lanes.js';
import { PurgeStatements } from './statements-purge.js';
import { QueryStatements } from './statements-queries.js';
import { SetupStatements } from './statements-setup.js';

export type { PostgresStatement } from './statement-fragments.js';

/**
 * SQL text for every operation of the store. Statements take their data as
 * parameters; only validated identifiers are interpolated. Documents travel
 * as one JSON text parameter expanded server-side with `jsonb_to_recordset`,
 * which keeps the statements identical across drivers and avoids array
 * literal encoding differences. Events go through `json_to_recordset` instead:
 * it hands each payload and headers object to their `json` columns as the
 * exact text the producer serialized, key order included.
 *
 * Leasing is per lane, not per delivery. A lane row carries the owner, an
 * `epoch` (the fencing token: it grows every time the lane changes hands) and
 * `in_flight`, the ids of the deliveries the owner has claimed. A claim
 * therefore writes one lane row for a whole batch and no delivery row; a
 * completion deletes delivery rows and writes no lane row. `in_flight` may
 * list ids that were completed (their rows are gone): a delivery is in flight
 * only while its row exists as `pending` and its id is listed.
 *
 * **Lock order.** Every statement that can wait for a lock takes its locks in this order, and rows of
 * one table in key order, so no two of them can wait for each other in a cycle:
 *
 * 1. the setup advisory lock and the schema DDL ({@link SetupStatements.setupLock}); only while the schema is created or
 *    upgraded, never on a current schema (see `PostgresEventStore.setup`);
 * 2. `key_sequences` rows, in key order ({@link AppendStatements.insertFreshEvents});
 * 3. `events` rows, in id order ({@link AppendStatements.lockExistingEvents}, the inserts of a publish, the foreign key
 *    checks of new deliveries and completions);
 * 4. `lanes` rows, in lane order: `FOR NO KEY UPDATE` for the owner's held lanes, `FOR KEY SHARE` for
 *    completions, `FOR UPDATE SKIP LOCKED` (never waits) for lanes taken from others;
 * 5. `deliveries` rows, only of lanes locked in step 4 (claims, completions, reschedules, takeovers),
 *    or inserted new (publish), or `dead` ones in id order ({@link QueryStatements.requeue});
 * 6. `event_completions` rows, in event id order ({@link CompletionStatements.complete}).
 *
 * Publishing never touches `lanes`, and dispatching never touches `key_sequences`. Purging only takes
 * `SKIP LOCKED` row locks and then deletes rows nobody references, so it never waits in a cycle either.
 * A caller's own transaction (a transactional handler that completes and publishes) adds the locks of
 * its own statements in the order it runs them; as documented on `append`, keys appended in opposite
 * orders by two such transactions can still deadlock, and the database then rolls one back.
 */
export class PostgresStatements {
  readonly setup: SetupStatements;
  readonly append: AppendStatements;
  readonly lanes: LaneStatements;
  readonly claim: ClaimStatements;
  readonly completion: CompletionStatements;
  readonly queries: QueryStatements;
  readonly purge: PurgeStatements;

  constructor(names: PostgresNames) {
    this.setup = new SetupStatements(names);
    this.append = new AppendStatements(names);
    this.lanes = new LaneStatements(names);
    this.claim = new ClaimStatements(names);
    this.completion = new CompletionStatements(names);
    this.queries = new QueryStatements(names);
    this.purge = new PurgeStatements(names);
  }
}
