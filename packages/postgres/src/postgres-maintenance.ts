import {
  type DeliveryQuery,
  type DeliveryRecord,
  type EventStoreStatistics,
  type PurgeRequest,
  type PurgeResult,
} from '@relaybus/core';
import type { PostgresRow } from './executor.js';
import { type PostgresStoreState } from './postgres-store-state.js';
import { mapDeliveryRecord, mapOldestDueAt, readCount } from './row-mapping.js';

/** maintenance */
export class PostgresMaintenance<TTransaction> {
  readonly #state: PostgresStoreState<TTransaction>;

  constructor(state: PostgresStoreState<TTransaction>) {
    this.#state = state;
  }

  /** Implements {@link PostgresEventStore.relinquish}. */
  async relinquish(leaseOwner: string): Promise<void> {
    await this.#state.query(this.#state.statements.claim.relinquish, [leaseOwner]);
  }

  /** Implements {@link PostgresEventStore.listDeliveries}. */
  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    const statement = this.#state.statements.queries.listDeliveries(query);
    const rows = await this.#state.executor.query(statement.sql, statement.parameters);
    return rows.map(mapDeliveryRecord);
  }

  /** Implements {@link PostgresEventStore.requeue}. */
  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    const rows = await this.#state.query(this.#state.statements.queries.requeue, [
      JSON.stringify(deliveryIds),
      now.toISOString(),
    ]);
    return readCount(rows[0]!, 'count');
  }

  /** Implements {@link PostgresEventStore.statistics}. */
  async statistics(now: Date): Promise<EventStoreStatistics> {
    const stored: Record<string, number> = { pending: 0, dead: 0, completed: 0 };
    for (const row of await this.#state.query(this.#state.statements.queries.countByStatus, [])) {
      stored[row['status'] as string] = readCount(row, 'count');
    }
    const [running] = await this.#state.query(this.#state.statements.queries.countRunning, []);
    const processing = readCount(running!, 'count');
    let completed = stored['completed']!;
    if (this.#state.completionHistory) {
      const [history] = await this.#state.query(
        this.#state.statements.queries.countCompletionHistory,
        [],
      );
      completed += readCount(history!, 'count');
    }
    const oldestDue = await this.#state.query(this.#state.statements.queries.oldestDue, [
      now.toISOString(),
    ]);
    return {
      // The counts come from separate statements; a delivery that finishes in between must not make this negative.
      pending: Math.max(0, stored['pending']! - processing),
      processing,
      completed,
      dead: stored['dead']!,
      oldestDueAt: mapOldestDueAt(oldestDue),
    };
  }

  /** Implements {@link PostgresEventStore.purge}. */
  async purge(request: PurgeRequest): Promise<PurgeResult> {
    const total = { deletedDeliveries: 0, deletedEvents: 0 };
    const batches: Array<[string, Date | undefined, string]> = [
      [
        this.#state.statements.purge.purgeCompletedBatch(),
        request.completedBefore,
        this.#state.statements.purge.deleteOrphanEventsWithoutCompletion,
      ],
      [
        this.#state.statements.purge.purgeDeadBatch(),
        request.deadBefore,
        this.#state.statements.purge.deleteOrphanEventsWithoutCompletion,
      ],
    ];
    if (this.#state.completionHistory) {
      batches.push([
        this.#state.statements.purge.purgeCompletionHistoryBatch(),
        request.completedBefore,
        this.#state.statements.purge.deleteOrphanEventsWithoutCompletion,
      ]);
    }
    for (const [sql, cutoff, deleteOrphans] of batches) {
      if (cutoff === undefined) {
        continue;
      }
      let deletedInBatch = this.#state.purgeBatchSize;
      while (deletedInBatch === this.#state.purgeBatchSize) {
        const batch = await this.#purgeBatch(sql, cutoff, deleteOrphans);
        deletedInBatch = batch.deletedDeliveries;
        total.deletedDeliveries += batch.deletedDeliveries;
        total.deletedEvents += batch.deletedEvents;
      }
    }
    const orphanCutoff = [request.completedBefore, request.deadBefore]
      .filter((cutoff) => cutoff !== undefined)
      .sort((first, second) => first.getTime() - second.getTime())[0];
    if (orphanCutoff !== undefined) {
      total.deletedEvents += await this.#purgeOrphanEvents(
        this.#state.statements.purge.lockOrphanEventsBatch,
        this.#state.statements.purge.deleteOrphanEventsWithoutCompletion,
        orphanCutoff,
        false,
      );
    }
    if (request.completedBefore !== undefined) {
      total.deletedEvents += await this.#purgeOrphanEvents(
        this.#state.statements.purge.lockCompletedOrphanEventsBatch,
        this.#state.statements.purge.deleteExpiredCompletedOrphanEvents,
        request.completedBefore,
        true,
      );
    }
    return total;
  }

  async #purgeOrphanEvents(
    lockStatement: string,
    deleteStatement: string,
    cutoff: Date,
    recheckCutoff: boolean,
  ): Promise<number> {
    let deletedEvents = 0;
    let removedInBatch = this.#state.purgeBatchSize;
    while (removedInBatch === this.#state.purgeBatchSize) {
      removedInBatch = await this.#state.executor.transaction(async (transaction) => {
        const locked = await this.#state.query(
          lockStatement,
          [cutoff.toISOString(), this.#state.purgeBatchSize],
          transaction,
        );
        const ids = JSON.stringify(locked.map((row: PostgresRow) => row['id'] as string));
        const removed = await this.#state.query(
          deleteStatement,
          recheckCutoff ? [ids, cutoff.toISOString()] : [ids],
          transaction,
        );
        return readCount(removed[0]!, 'count');
      });
      deletedEvents += removedInBatch;
    }
    return deletedEvents;
  }

  async #purgeBatch(
    sql: string,
    cutoff: Date,
    deleteOrphans: string,
  ): Promise<{ deletedDeliveries: number; deletedEvents: number }> {
    return this.#state.executor.transaction(async (transaction) => {
      const deleted = await this.#state.query(
        sql,
        [cutoff.toISOString(), this.#state.purgeBatchSize],
        transaction,
      );
      if (deleted.length === 0) {
        return { deletedDeliveries: 0, deletedEvents: 0 };
      }
      const eventIds = JSON.stringify([
        ...new Set(deleted.map((row) => row['event_id'] as string)),
      ]);
      const locked = await this.#state.query(
        this.#state.statements.purge.lockEvents,
        [eventIds],
        transaction,
      );
      const removed = await this.#state.query(
        deleteOrphans,
        [JSON.stringify(locked.map((row: PostgresRow) => row['id'] as string))],
        transaction,
      );
      return { deletedDeliveries: deleted.length, deletedEvents: readCount(removed[0]!, 'count') };
    });
  }
}
