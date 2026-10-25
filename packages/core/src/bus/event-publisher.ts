import { withTraceHeaders, type BusObservation } from '../bus-observation.js';
import type { Clock } from '../clock.js';
import { ConfigurationError, TransactionRequiredError } from '../errors.js';
import type { AppendEntry, AppendResult, EventStore, NewDelivery } from '../event-store.js';
import {
  assertEventType,
  keyFromDefinition,
  validatePayload,
  type DomainEvent,
  type EventClass,
  type EventEnvelope,
} from '../event.js';
import { getCurrentScope, type ExecutionScope } from '../execution-scope.js';
import type { HandlerRegistration, HandlerRegistry } from '../handler-registry.js';
import type { IdentifierGenerator } from '../identifier.js';
import type { Notifier } from '../notifier.js';
import type { PublishOptions, TransactionMode } from '../relay-bus-options.js';
import { Milliseconds } from '../values/milliseconds.js';

/** The options of one publish call, and the id `publish` (not `publishAll`) may fix. */
export type PublishCall<TTransaction> = {
  readonly options: PublishOptions<TTransaction>;
  readonly id: string | undefined;
};

/** What a publish call inherits from the running handler and the clock. */
type PublishContext<TTransaction> = PublishCall<TTransaction> & {
  readonly scope: ExecutionScope | undefined;
  readonly now: Date;
};

/** One event of a publish call with what it inherits from the call and the running handler. */
type CreateEnvelopeInput<TTransaction> = PublishContext<TTransaction> & {
  readonly event: DomainEvent;
  readonly correlationId: string | undefined;
};

/** What publishing needs from the bus. */
export type EventPublisherParts<TTransaction> = {
  readonly store: EventStore<TTransaction>;
  readonly registry: HandlerRegistry;
  readonly clock: Clock;
  readonly notifier: Notifier;
  readonly observation: BusObservation | undefined;
  readonly source: string;
  readonly transactionMode: TransactionMode;
  readonly transactionResolver: (() => TTransaction | undefined) | undefined;
  readonly generateIdentifier: IdentifierGenerator;
  readonly createDelivery: (registration: HandlerRegistration, availableAt: Date) => NewDelivery;
  /** Logs or throws for an event no handler or route takes. */
  readonly reportUnrouted: (envelope: EventEnvelope) => void;
  /** Wakes the dispatcher shortly after new deliveries were stored. */
  /** Wakes the dispatcher for new deliveries; `committed` is false while the caller's transaction is open. */
  readonly scheduleWake: (committed: boolean) => void;
};

/**
 * Stores published events in the caller's transaction: builds each envelope (id, key,
 * correlation and causation from the running handler), a delivery per registration of its type,
 * and appends what has deliveries: the outbox side of the bus.
 */
export class EventPublisher<TTransaction> {
  readonly #parts: EventPublisherParts<TTransaction>;

  constructor(parts: EventPublisherParts<TTransaction>) {
    this.#parts = parts;
  }

  /** Publishes inside the tracer's span, with its trace headers, when the bus is observed. */
  publish(
    events: readonly DomainEvent[],
    call: PublishCall<TTransaction>,
  ): Promise<EventEnvelope[]> {
    const { observation } = this.#parts;
    if (observation === undefined) {
      return this.#publish(events, call);
    }
    return observation.publish(
      { eventTypes: events.map((event) => String(event.type)) },
      (headers) =>
        this.#publish(events, { ...call, options: withTraceHeaders(call.options, headers) }),
    );
  }

  async #publish(
    events: readonly DomainEvent[],
    { options, id }: PublishCall<TTransaction>,
  ): Promise<EventEnvelope[]> {
    if (events.length === 0) {
      return [];
    }
    const now = this.#parts.clock.now();
    const delayMs = options.delay === undefined ? 0 : Milliseconds.of(options.delay, 'delay');
    const availableAt = new Date(now.getTime() + delayMs);
    const context: PublishContext<TTransaction> = { options, id, scope: getCurrentScope(), now };
    const entries = await this.#createEntries(events, { context, availableAt });
    const deliverable = entries.filter((entry) => entry.deliveries.length > 0);
    if (deliverable.length > 0) {
      await this.#appendDeliverable(deliverable, { context, delayMs });
    }
    return entries.map((entry) => entry.event);
  }

  /** One entry per event, in order: its envelope and a delivery for every registration of its type. */
  async #createEntries(
    events: readonly DomainEvent[],
    {
      context,
      availableAt,
    }: { readonly context: PublishContext<TTransaction>; readonly availableAt: Date },
  ): Promise<AppendEntry[]> {
    const { registry, createDelivery, reportUnrouted } = this.#parts;
    const entries: AppendEntry[] = [];
    const { options, scope } = context;
    let correlationId = options.correlationId ?? scope?.event?.correlationId ?? scope?.event?.id;
    for (const event of events) {
      const envelope = await this.#createEnvelope({ ...context, event, correlationId });
      correlationId = envelope.correlationId!;
      const registrations = registry.forEventType(envelope.type);
      if (registrations.length === 0) {
        reportUnrouted(envelope);
      }
      entries.push({
        event: envelope,
        deliveries: registrations.map((registration) => createDelivery(registration, availableAt)),
      });
    }
    return entries;
  }

  /** Appends the entries that have deliveries, reports what was new and wakes the dispatcher when it is due now. */
  async #appendDeliverable(
    entries: readonly AppendEntry[],
    {
      context,
      delayMs,
    }: { readonly context: PublishContext<TTransaction>; readonly delayMs: number },
  ): Promise<void> {
    const { result, committed } = await this.#appendInTransaction(entries, context);
    const appended = new Set(result.appendedEventIds);
    const published = entries
      .map((entry) => entry.event)
      .filter((envelope) => appended.has(envelope.id));
    if (published.length > 0) {
      this.#parts.notifier.notify('onPublished', {
        envelopes: published,
        deliveries: result.appendedDeliveryCount,
      });
      if (delayMs === 0) {
        this.#parts.scheduleWake(committed);
      }
    }
  }

  /** Appends in the caller's transaction, else in one of the store's; `committed` tells which. */
  async #appendInTransaction(
    entries: readonly AppendEntry[],
    { options, scope, now }: PublishContext<TTransaction>,
  ): Promise<{ readonly result: AppendResult; readonly committed: boolean }> {
    const { store, transactionMode, transactionResolver } = this.#parts;
    const transaction =
      options.transaction ??
      (scope?.transaction as TTransaction | undefined) ??
      transactionResolver?.();
    if (transaction !== undefined) {
      return { result: await store.append(transaction, entries, now), committed: false };
    }
    if (transactionMode === 'required') {
      throw new TransactionRequiredError(
        'publish found no transaction. Pass { transaction }, publish inside bus.transaction(), configure transactionResolver, or set transactionMode to optional',
      );
    }
    const result = await store.transaction((ownTransaction) =>
      store.append(ownTransaction, entries, now),
    );
    return { result, committed: true };
  }

  async #createEnvelope(input: CreateEnvelopeInput<TTransaction>): Promise<EventEnvelope> {
    const { event, options, id, scope, now, correlationId } = input;
    if (typeof event.type !== 'string') {
      throw new ConfigurationError(
        'publish expects an instance of a class created with defineEvent',
      );
    }
    assertEventType(event.type);
    const schema = (event.constructor as Partial<EventClass>).schema;
    const payload = await validatePayload(event.type, schema, event.payload);
    if (options.key !== undefined && options.key.length === 0) {
      throw new ConfigurationError('key must not be empty');
    }
    const eventId = id ?? this.#parts.generateIdentifier();
    return {
      id: eventId,
      type: event.type,
      source: this.#parts.source,
      key: options.key ?? keyFromDefinition(event, payload),
      payload,
      headers: { ...options.headers },
      correlationId: correlationId ?? eventId,
      causationId: scope?.event?.id ?? null,
      occurredAt: now,
    };
  }
}
