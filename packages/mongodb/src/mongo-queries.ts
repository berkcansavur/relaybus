import {
  type DeliveryQuery,
  type DeliveryRecord,
  type EventEnvelope,
  type EventStoreStatistics,
} from '@relaybus/core';
import type { Filter } from 'mongodb';
import {
  toCompletedDeliveryRecord,
  toDeliveryRecord,
  toEnvelope,
  type DeliveryDocument,
} from './documents.js';
import type { MongoStoreState } from './mongo-store-state.js';

const DEFAULT_LIST_LIMIT = 100;

/** Reads for listing, requeueing and statistics, and the lookups the claim shares. */
export class MongoQueries {
  readonly #state: MongoStoreState;

  constructor(state: MongoStoreState) {
    this.#state = state;
  }

  /** Implements {@link MongoEventStore.listDeliveries}. */
  async listDeliveries(query: DeliveryQuery): Promise<DeliveryRecord[]> {
    // Strings only: an object here (say, parsed from an admin request) would be a query operator.
    const status = query.status === undefined ? undefined : String(query.status);
    const common = {
      ...(query.handler === undefined ? {} : { handler: String(query.handler) }),
      ...(query.eventType === undefined ? {} : { eventType: String(query.eventType) }),
      ...(query.eventId === undefined ? {} : { eventId: String(query.eventId) }),
    };
    const limit = query.limit ?? DEFAULT_LIST_LIMIT;
    const lanes = await this.#state.lanes
      .find({ 'inFlight.0': { $exists: true } }, { projection: { epoch: 0 } })
      .toArray();
    const listed = lanes.flatMap((lane) => lane.inFlight);
    // A Map, not an object: an untrusted status such as 'constructor' must not find anything.
    const derivedStatuses = new Map<string, Filter<DeliveryDocument>>([
      ['processing', { status: 'pending', _id: { $in: listed } }],
      ['pending', { status: 'pending', _id: { $nin: listed } }],
    ]);
    const filter: Filter<DeliveryDocument> = {
      ...common,
      ...(status === undefined
        ? {}
        : (derivedStatuses.get(status) ?? { status: status as DeliveryDocument['status'] })),
    };
    const sort = { createdAt: 1, _id: 1 } as const;
    const documents = await this.#state.deliveries.find(filter, { sort, limit }).toArray();
    const history =
      this.#state.completionHistory && (status === undefined || status === 'completed')
        ? await this.#state.completions.find(common, { sort, limit }).toArray()
        : [];
    const events = await this.readEnvelopes([
      ...documents.map((document) => document.eventId),
      ...history.map((document) => document.eventId),
    ]);
    const laneById = new Map(lanes.map((lane) => [lane._id, lane]));
    const records = [
      ...documents.map((document) => {
        const lane = laneById.get(document.laneId);
        const runs =
          document.status === 'pending' && lane?.inFlight.includes(document._id) === true;
        return toDeliveryRecord(document, events.get(document.eventId)!, runs ? lane : undefined);
      }),
      ...history.map((document) =>
        toCompletedDeliveryRecord(document, events.get(document.eventId)!),
      ),
    ];
    return records
      .sort(
        (left, right) =>
          left.createdAt.getTime() - right.createdAt.getTime() || compareText(left.id, right.id),
      )
      .slice(0, limit);
  }

  /** Implements {@link MongoEventStore.requeue}. */
  async requeue(deliveryIds: readonly string[], now: Date): Promise<number> {
    const result = await this.#state.deliveries.updateMany(
      { _id: { $in: deliveryIds.map(String) }, status: 'dead' },
      {
        $set: {
          status: 'pending',
          lane: 'ready',
          attempts: 0,
          availableAt: now,
          dueAt: now,
          updatedAt: now,
        },
      },
    );
    return result.modifiedCount;
  }

  /** Implements {@link MongoEventStore.statistics}. */
  async statistics(now: Date): Promise<EventStoreStatistics> {
    const lanes = await this.#state.lanes
      .find({ 'inFlight.0': { $exists: true } }, { projection: { inFlight: 1, leaseExpiresAt: 1 } })
      .toArray();
    // Index-only counts in parallel: measured at 300k deliveries they take 27 ms, against
    // 121 ms for one $group, which has to read every document or index entry.
    const [stored, dead, legacyCompleted, history, running] = await Promise.all([
      this.#state.deliveries.countDocuments({ status: 'pending' }),
      this.#state.deliveries.countDocuments({ status: 'dead' }),
      this.#state.deliveries.countDocuments({ status: 'completed' }),
      this.#state.completionHistory ? this.#state.completions.countDocuments() : Promise.resolve(0),
      this.findRunning(lanes.flatMap((lane) => lane.inFlight)),
    ]);
    const runningIds = new Set(running.map((document) => document._id));
    const waiting = await this.#state.deliveries.findOne(
      { dueAt: { $lte: now }, _id: { $nin: [...runningIds] } },
      { sort: { dueAt: 1 }, projection: { dueAt: 1 } },
    );
    const dueTimes = lanes
      .filter(
        (lane) =>
          lane.leaseExpiresAt !== null &&
          lane.leaseExpiresAt <= now &&
          lane.inFlight.some((id) => runningIds.has(id)),
      )
      .map((lane) => lane.leaseExpiresAt!.getTime());
    if (waiting !== null) {
      dueTimes.push(waiting.dueAt!.getTime());
    }
    return {
      // The counts are separate reads; a completion between them must not make this negative.
      pending: Math.max(0, stored - running.length),
      processing: running.length,
      completed: legacyCompleted + history,
      dead,
      oldestDueAt: dueTimes.length === 0 ? null : new Date(Math.min(...dueTimes)),
    };
  }

  /** The listed deliveries that run: those whose document still exists as pending. */
  async findRunning(listed: readonly string[]): Promise<DeliveryDocument[]> {
    if (listed.length === 0) {
      return [];
    }
    return this.#state.deliveries
      .find(
        { _id: { $in: [...listed] }, status: 'pending' },
        { projection: { handler: 1, key: 1, laneId: 1 } },
      )
      .toArray();
  }

  async readEnvelopes(eventIds: readonly string[]): Promise<Map<string, EventEnvelope>> {
    const documents = await this.#state.events
      .find({ _id: { $in: [...new Set(eventIds)] } })
      .toArray();
    return new Map(documents.map((document) => [document._id, toEnvelope(document)]));
  }
}

function compareText(left: string, right: string): number {
  return Number(left > right) - Number(left < right);
}
