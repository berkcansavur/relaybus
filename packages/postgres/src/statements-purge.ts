import type { PostgresNames } from './names.js';
import { countChanged } from './statement-fragments.js';

/** Statements that purge old deliveries and the events nothing references. */
export class PurgeStatements {
  readonly #names: PostgresNames;

  constructor(names: PostgresNames) {
    this.#names = names;
  }

  /**
   * Deletes one bounded batch of purgeable deliveries (or of the completion
   * history) and returns the event id of each deleted row. Parameters:
   * `[cutoff, batchSize]`. `FOR UPDATE SKIP LOCKED` keeps purge from waiting
   * on, or being waited on by, anything.
   */
  purgeCompletedBatch(): string {
    return this.#purgeBatch(this.#names.deliveries, {
      condition: `status = 'completed' AND completed_at < $1::timestamptz`,
      orderColumn: 'completed_at',
    });
  }

  purgeCompletionHistoryBatch(): string {
    return this.#purgeBatch(this.#names.completions, {
      condition: `completed_at < $1::timestamptz`,
      orderColumn: 'completed_at',
    });
  }

  purgeDeadBatch(): string {
    return this.#purgeBatch(this.#names.deliveries, {
      condition: `status = 'dead' AND updated_at < $1::timestamptz`,
      orderColumn: 'updated_at',
    });
  }

  /**
   * Selects and locks one bounded batch of events created before the cutoff
   * that no delivery references and that never had a delivery complete (those
   * are measured from their completion, see {@link lockCompletedOrphanEventsBatch}).
   * Parameters: `[cutoff, batchSize]`. The ids go through
   * {@link deleteOrphanEventsWithoutCompletion} or {@link deleteExpiredCompletedOrphanEvents}, which re-check under a fresh snapshot.
   */
  get lockOrphanEventsBatch(): string {
    return `SELECT event.id FROM ${this.#names.events} AS event
WHERE event.created_at < $1::timestamptz
  AND ${this.#unreferenced('event')}
  AND NOT EXISTS (SELECT 1 FROM ${this.#names.eventCompletions} AS completion WHERE completion.event_id = event.id)
ORDER BY event.created_at, event.id
LIMIT $2::int
FOR UPDATE OF event SKIP LOCKED`;
  }

  /**
   * Like {@link lockOrphanEventsBatch} for events that had a delivery complete:
   * they go once their last completion is older than the cutoff, however old the
   * event itself is. Parameters: `[cutoff, batchSize]`.
   */
  get lockCompletedOrphanEventsBatch(): string {
    return `SELECT event.id FROM ${this.#names.events} AS event
JOIN ${this.#names.eventCompletions} AS completion ON completion.event_id = event.id
WHERE completion.completed_at < $1::timestamptz
  AND ${this.#unreferenced('event')}
ORDER BY completion.completed_at, event.id
LIMIT $2::int
FOR UPDATE OF event SKIP LOCKED`;
  }

  /**
   * Locks the given events, skipping any that a concurrent transaction is
   * referencing from a new delivery (a foreign key check holds `FOR KEY
   * SHARE`, which conflicts with this lock). Parameters: `[eventIdsJson]`.
   */
  get lockEvents(): string {
    return `SELECT id FROM ${this.#names.events}
WHERE id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
FOR UPDATE SKIP LOCKED`;
  }

  /**
   * Deletes events that no delivery references and whose last completion is older than
   * `$2`, checked again under the fresh snapshot of this statement: a completion recorded
   * after the sweep selected the event moves its time and keeps the event. Parameters:
   * `[eventIdsJson, cutoff]`.
   */
  get deleteExpiredCompletedOrphanEvents(): string {
    return countChanged(`DELETE FROM ${this.#names.events} AS event
WHERE event.id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
  AND ${this.#unreferenced('event')}
  AND EXISTS (
    SELECT 1 FROM ${this.#names.eventCompletions} AS completion
    WHERE completion.event_id = event.id AND completion.completed_at < $2::timestamptz
  )`);
  }

  /**
   * Deletes locked events that no delivery references and that never had a delivery complete.
   * It must run as a statement of its own after {@link lockEvents}: it then takes a fresh
   * snapshot that includes every delivery committed before the lock was granted, so an event
   * that just gained a delivery is never cascaded away. Keeps events that had a delivery complete:
   * deleting dead letters must not shorten the retention of completed work.
   */
  get deleteOrphanEventsWithoutCompletion(): string {
    return countChanged(`DELETE FROM ${this.#names.events} AS event
WHERE event.id = ANY (ARRAY(SELECT jsonb_array_elements_text($1::jsonb)))
  AND ${this.#unreferenced('event')}
  AND NOT EXISTS (SELECT 1 FROM ${this.#names.eventCompletions} AS completion WHERE completion.event_id = event.id)`);
  }

  /** True when no delivery and no completion history record references the event. */
  #unreferenced(eventAlias: string): string {
    const history = this.#names.completionHistory
      ? ` AND NOT EXISTS (SELECT 1 FROM ${this.#names.completions} AS done WHERE done.event_id = ${eventAlias}.id)`
      : '';
    return `NOT EXISTS (SELECT 1 FROM ${this.#names.deliveries} AS delivery WHERE delivery.event_id = ${eventAlias}.id)${history}`;
  }

  #purgeBatch(
    table: string,
    { condition, orderColumn }: { readonly condition: string; readonly orderColumn: string },
  ): string {
    return `WITH batch AS MATERIALIZED (
  SELECT id FROM ${table}
  WHERE ${condition}
  ORDER BY ${orderColumn}, id
  LIMIT $2::int
  FOR UPDATE SKIP LOCKED
), deleted AS (
  DELETE FROM ${table} AS delivery USING batch WHERE delivery.id = batch.id RETURNING delivery.event_id
)
SELECT event_id FROM deleted`;
  }
}
