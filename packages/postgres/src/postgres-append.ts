import { type AppendEntry, type AppendResult, type EventEnvelope } from '@relaybus/core';
import type { PostgresRow } from './executor.js';
import { type PostgresStoreState } from './postgres-store-state.js';
import { readCount } from './row-mapping.js';

/**
 * Least time between two wake-up notifications of one store. A transaction that sends `NOTIFY` holds a
 * database-wide lock from its commit until the commit is flushed, so a notification per publishing
 * transaction made concurrent publishers commit one at a time.
 */
const NOTIFICATION_INTERVAL_MS = 10;

interface EventRow {
  readonly id: string;
  readonly type: string;
  readonly source: string;
  readonly key: string | null;
  readonly sequence: string | null;
  readonly payload: unknown;
  readonly headers: EventEnvelope['headers'];
  readonly correlation_id: string | null;
  readonly causation_id: string | null;
  readonly occurred_at: string;
  readonly handlers: readonly string[];
}

interface DeliveryRow {
  readonly id: string;
  readonly event_id: string;
  readonly handler: string;
  readonly available_at: string;
}

/** What `#appendToExistingEvents` needs from the append that found existing events. */
type AppendToExistingEventsInput = {
  readonly plan: AppendPlan;
  readonly fresh: readonly PostgresRow[];
  readonly deliveries: readonly DeliveryRow[];
  readonly handlersByEvent: ReadonlyMap<string, string[]>;
  /** Grows by every event that turns out to be gone and is inserted again. */
  readonly insertedEventIds: Set<string>;
  readonly createdAt: string;
  readonly wakeAt: string | null;
};

/** appender */
export class PostgresAppend<TTransaction> {
  readonly #state: PostgresStoreState<TTransaction>;

  constructor(state: PostgresStoreState<TTransaction>) {
    this.#state = state;
  }

  /** Implements {@link PostgresEventStore.append}. */
  async append(
    transaction: TTransaction,
    entries: readonly AppendEntry[],
    now: Date,
  ): Promise<AppendResult> {
    const plan = planAppend(entries);
    if (plan.events.length === 0) {
      return { appendedEventIds: [], duplicateEventIds: [], appendedDeliveryCount: 0 };
    }
    const createdAt = now.toISOString();
    const wakeAt = this.#wakeAt(plan.deliveries, now, transaction);
    const handlersByEvent = handlersByEventOf(plan.deliveries);
    const keyCounts = keyCountsOf(plan.events);
    const fresh = await this.#state.query(
      this.#state.statements.append.insertFreshEvents,
      [
        JSON.stringify(
          plan.events.map((event, ordinal) => ({
            ordinal,
            ...toEventRow(event, null, handlersByEvent.get(event.id) ?? []),
          })),
        ),
        createdAt,
        JSON.stringify(plan.deliveries),
        JSON.stringify([...keyCounts].map(([key, count]) => ({ key, count }))),
        wakeAt,
      ],
      transaction,
    );
    const insertedEventIds = new Set(
      fresh.filter((row) => row['inserted'] === true).map((row) => row['id'] as string),
    );
    let appendedDeliveryCount = readCount(fresh[0]!, 'deliveries');
    const remaining = plan.deliveries.filter(
      (delivery) => !insertedEventIds.has(delivery.event_id),
    );
    if (remaining.length > 0) {
      appendedDeliveryCount += await this.#appendToExistingEvents(transaction, {
        plan,
        fresh,
        deliveries: remaining,
        handlersByEvent,
        insertedEventIds,
        createdAt,
        wakeAt,
      });
    }
    return { ...splitOutcomes(plan, insertedEventIds), appendedDeliveryCount };
  }

  /**
   * The time the publishing statements notify the dispatchers for (deliveries due by then wake them),
   * or `null` for no notification. An append in a transaction of the caller's always notifies, at its
   * commit. In a transaction of this store, only the first append within {@link NOTIFICATION_INTERVAL_MS}
   * does; the others leave the wake-up to {@link #wakeAfterCommit}, which runs once their transaction
   * committed, so no committed delivery waits for the poll.
   */
  #wakeAt(deliveries: readonly DeliveryRow[], now: Date, transaction: TTransaction): string | null {
    if (
      !this.#state.notifies ||
      !deliveries.some((delivery) => Date.parse(delivery.available_at) <= now.getTime())
    ) {
      return null;
    }
    const own = this.#state.ownTransactions.get(transaction);
    if (
      own !== undefined &&
      Date.now() - this.#state.lastNotificationAt < NOTIFICATION_INTERVAL_MS
    ) {
      own.wake = true;
      return null;
    }
    this.#state.lastNotificationAt = Date.now();
    return now.toISOString();
  }

  /**
   * Wakes the dispatchers for a committed transaction whose appends sent no notification: at once when
   * none was sent within {@link NOTIFICATION_INTERVAL_MS}, otherwise once when the interval ends. A
   * wake-up already scheduled covers this transaction too, since it is sent after this commit. Nothing is
   * sent once the store is closed or the `pg` pool behind it is ending: no dispatcher can be woken through
   * it then, and the deliveries are found by polling.
   */
  wakeAfterCommit(): void {
    if (this.#state.trailingNotification !== undefined || this.#state.closed) {
      return;
    }
    this.#state.trailingNotification = setTimeout(
      () => {
        this.#state.trailingNotification = undefined;
        if (this.#state.pool?.ending === true) {
          return;
        }
        this.#state.lastNotificationAt = Date.now();
        this.#state.query(this.#state.statements.append.notify, []).catch((error: unknown) => {
          this.#state.logger.error(
            `[PostgresEventStore](wakeAfterCommit) Waking the dispatchers failed, they find the deliveries when they poll | error=${error instanceof Error ? error.message : String(error)}`,
          );
        });
      },
      Math.max(0, this.#state.lastNotificationAt + NOTIFICATION_INTERVAL_MS - Date.now()),
    );
  }

  /**
   * Adds the deliveries of events the publishing statement found already stored. An event that already
   * exists may be getting the same new handler from another transaction at this very moment. Whoever
   * holds the event decides, so the freshness check of the insert sees what the other transaction
   * committed, even if its delivery has completed since. An existing event that a purge removed
   * meanwhile is inserted again (with the sequence reserved for it) instead of being skipped; its id is
   * added to `insertedEventIds`. Returns how many deliveries were inserted.
   */
  async #appendToExistingEvents(
    transaction: TTransaction,
    input: AppendToExistingEventsInput,
  ): Promise<number> {
    const { plan, fresh, deliveries, handlersByEvent, insertedEventIds, createdAt, wakeAt } = input;
    const sequences = new Map(
      fresh.map((row) => [row['id'] as string, row['sequence'] as string | null]),
    );
    const reinserted: string[] = [];
    let existing = plan.events
      .filter((event) => !insertedEventIds.has(event.id))
      .map((event) =>
        toEventRow(event, sequences.get(event.id)!, handlersByEvent.get(event.id) ?? []),
      );
    while (existing.length > 0) {
      const locked = new Set(
        (
          await this.#state.query(
            this.#state.statements.append.lockExistingEvents,
            [JSON.stringify(existing.map((row) => row.id).sort())],
            transaction,
          )
        ).map((row) => row['id'] as string),
      );
      const gone = existing.filter((row) => !locked.has(row.id));
      if (gone.length > 0) {
        const inserted = await this.#state.query(
          this.#state.statements.append.insertEvents,
          [JSON.stringify(gone), createdAt],
          transaction,
        );
        for (const row of inserted) {
          insertedEventIds.add(row['id'] as string);
          reinserted.push(row['id'] as string);
        }
      }
      existing = gone.filter((row) => !insertedEventIds.has(row.id));
    }
    const inserted = await this.#state.query(
      this.#state.statements.append.insertDeliveries,
      [JSON.stringify(deliveries), createdAt, JSON.stringify(reinserted), wakeAt],
      transaction,
    );
    return inserted.length;
  }
}

interface AppendPlan {
  /** First occurrence of every event id in the call, in entry order. */
  readonly events: readonly EventEnvelope[];
  readonly deliveries: readonly DeliveryRow[];
  /** One per entry: its event id, and whether an earlier entry of the call carried the same id. */
  readonly outcomes: ReadonlyArray<{ readonly eventId: string; readonly repeated: boolean }>;
}

function planAppend(entries: readonly AppendEntry[]): AppendPlan {
  const events: EventEnvelope[] = [];
  const deliveries: DeliveryRow[] = [];
  const outcomes: Array<{ eventId: string; repeated: boolean }> = [];
  const seenEventIds = new Set<string>();
  const seenDeliveries = new Set<string>();
  for (const { event, deliveries: newDeliveries } of entries) {
    const repeated = seenEventIds.has(event.id);
    seenEventIds.add(event.id);
    outcomes.push({ eventId: event.id, repeated });
    if (!repeated) {
      events.push(event);
    }
    for (const delivery of newDeliveries) {
      const identity = `${event.id}\u0000${delivery.handler}`;
      if (!seenDeliveries.has(identity)) {
        seenDeliveries.add(identity);
        deliveries.push({
          id: delivery.id,
          event_id: event.id,
          handler: delivery.handler,
          available_at: delivery.availableAt.toISOString(),
        });
      }
    }
  }
  return { events, deliveries, outcomes };
}

function toEventRow(
  event: EventEnvelope,
  sequence: string | null,
  handlers: readonly string[],
): EventRow {
  return {
    id: event.id,
    type: event.type,
    source: event.source,
    key: event.key,
    sequence,
    payload: event.payload,
    headers: event.headers,
    correlation_id: event.correlationId,
    causation_id: event.causationId,
    occurred_at: event.occurredAt.toISOString(),
    handlers,
  };
}

/** The handlers each event gets a delivery for, in delivery order. */
function handlersByEventOf(deliveries: readonly DeliveryRow[]): Map<string, string[]> {
  const handlersByEvent = new Map<string, string[]>();
  for (const delivery of deliveries) {
    handlersByEvent.set(delivery.event_id, [
      ...(handlersByEvent.get(delivery.event_id) ?? []),
      delivery.handler,
    ]);
  }
  return handlersByEvent;
}

/** How many of the events carry each key: the sequences each key reserves. */
function keyCountsOf(events: AppendPlan['events']): Map<string, number> {
  const keyCounts = new Map<string, number>();
  for (const event of events) {
    if (event.key !== null) {
      keyCounts.set(event.key, (keyCounts.get(event.key) ?? 0) + 1);
    }
  }
  return keyCounts;
}

/** Each entry's event id as appended (new and inserted by this call) or duplicate, in entry order. */
function splitOutcomes(
  plan: AppendPlan,
  insertedEventIds: ReadonlySet<string>,
): { appendedEventIds: string[]; duplicateEventIds: string[] } {
  const appendedEventIds: string[] = [];
  const duplicateEventIds: string[] = [];
  for (const { eventId, repeated } of plan.outcomes) {
    (!repeated && insertedEventIds.has(eventId) ? appendedEventIds : duplicateEventIds).push(
      eventId,
    );
  }
  return { appendedEventIds, duplicateEventIds };
}
