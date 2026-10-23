import { hostname } from 'node:os';
import { systemClock, type Clock } from './clock.js';
import { Dispatcher, type DispatcherSettings } from './dispatcher.js';
import { toMilliseconds, toTimerMilliseconds, type Duration } from './duration.js';
import {
  ConfigurationError,
  InvalidEnvelopeError,
  RelayBusError,
  TransactionRequiredError,
  toErrorInfo,
} from './errors.js';
import {
  assertEventType,
  validatePayload,
  type DomainEvent,
  type EventClass,
  type EventEnvelope,
  type EventHeaders,
  type EventReference,
} from './event.js';
import type {
  AppendEntry,
  DeliveryQuery,
  DeliveryRecord,
  EventStore,
  EventStoreStatistics,
  NewDelivery,
  PurgeResult,
} from './event-store.js';
import { getCurrentScope, runInScope, type ExecutionScope } from './execution-scope.js';
import {
  HandlerRegistry,
  type EventHandler,
  type HandlerDescriptor,
  type HandlerRegistration,
  type RegistrationDefaults,
  type SubscribeOptions,
} from './handler-registry.js';
import { createUuidV7, type IdentifierGenerator } from './identifier.js';
import { Notifier } from './notifier.js';
import type { RelayBusLogger, RelayBusObserver } from './observer.js';
import {
  DEFAULT_ROUTE_RETRY_POLICY,
  createRetryPolicy,
  type RetryPolicyOptions,
} from './retry-policy.js';
import type { StandardSchemaV1 } from './standard-schema.js';
import type { Transport } from './transport.js';

/**
 * - `required`: `publish` throws {@link TransactionRequiredError} when it finds no transaction.
 * - `optional`: `publish` opens its own transaction instead. The event is still durable,
 *   but no longer atomic with business writes made outside that transaction.
 */
export type TransactionMode = 'required' | 'optional';

/**
 * What `publish` does with an event that has no handler or route in this
 * process. Deliveries are created from the publisher's registrations, so a
 * process must register every handler and route of the events it publishes.
 * - `warn`: log a warning and store nothing.
 * - `throw`: throw a {@link ConfigurationError}, rolling back the caller's transaction.
 */
export type UnroutedEventPolicy = 'warn' | 'throw';

export interface DispatcherOptions {
  /** Run handlers in this process. Disable on publish-only processes. Default true. */
  readonly enabled?: boolean;
  /** Maximum handlers running at once. Default 10. */
  readonly concurrency?: number;
  /** Maximum deliveries claimed per query. Default equals concurrency. */
  readonly batchSize?: number;
  /** Wait between claims when idle. Retries are picked up on this cadence. Default `'1s'`. */
  readonly pollInterval?: Duration;
  /** Lease length; renewed every third of it while the handler runs. Default `'30s'`. */
  readonly leaseDuration?: Duration;
  /** How long `stop` waits for running handlers before aborting them. Default `'30s'`. */
  readonly shutdownTimeout?: Duration;
  /**
   * Longest a finished delivery waits so that deliveries finishing around the
   * same time are completed with one store call (when the store supports it).
   * A delivery that finishes while nothing else runs is completed at once.
   * `0` completes every delivery on its own. Default `'2ms'`.
   */
  readonly completionBatchWindow?: Duration;
  /** Most deliveries completed in one store call. Default 200. */
  readonly completionBatchSize?: number;
}

export interface RetentionOptions {
  /**
   * Delete completed deliveries older than this. Default `'7d'`; `false` keeps them.
   * Deduplication of received events and of publishes with a caller-supplied id
   * only lasts as long as the event is kept: keep this longer than any broker
   * redelivery or publish retry window.
   */
  readonly completed?: Duration | false;
  /** Delete dead deliveries older than this. Default `false` (keep for inspection). */
  readonly dead?: Duration | false;
  /** How often the cleanup runs. Default `'1h'`. */
  readonly interval?: Duration;
}

export interface RouteOptions {
  /** Event class or type forwarded to the transport. */
  readonly event: EventReference;
  /** Name of a configured transport. */
  readonly transport: string;
  /** Topic, exchange or queue. Default: the event type. */
  readonly destination?: string;
  /** Default: retry until the broker accepts the event, at most every 5 minutes. */
  readonly retry?: RetryPolicyOptions;
  /** Abort a publish that hangs and retry it. Default `'30s'`. */
  readonly timeout?: Duration;
}

export interface InboundSubscriptionOptions {
  readonly transport: string;
  readonly destinations: readonly string[];
}

export interface RelayBusOptions<TTransaction = unknown> {
  readonly store: EventStore<TTransaction>;
  /** Name of this service, recorded on every event. Default `'relaybus'`. */
  readonly source?: string;
  /** Default `'required'`. */
  readonly transactionMode?: TransactionMode;
  /**
   * Finds the caller's active transaction when none is passed to `publish`
   * and none was opened with `bus.transaction`, for example from
   * `@nestjs-cls/transactional`. Return undefined when there is none.
   */
  readonly transactionResolver?: () => TTransaction | undefined;
  /** Default `'warn'`. */
  readonly unroutedEvents?: UnroutedEventPolicy;
  /** Default retry policy of every handler. Routes have their own default. */
  readonly retry?: RetryPolicyOptions;
  /** Default handler timeout. Default `false` (no timeout). */
  readonly handlerTimeout?: Duration | false;
  readonly dispatcher?: DispatcherOptions;
  readonly retention?: RetentionOptions;
  readonly transports?: readonly Transport[];
  /** Events forwarded to brokers after they commit. */
  readonly routes?: readonly RouteOptions[];
  /** Broker destinations whose events are stored and dispatched to local handlers. */
  readonly subscriptions?: readonly InboundSubscriptionOptions[];
  /** Create tables and indexes on start. Default true. */
  readonly setupStore?: boolean;
  readonly observers?: readonly RelayBusObserver[];
  /** Default: `console`. */
  readonly logger?: RelayBusLogger;
  readonly clock?: Clock;
  readonly generateIdentifier?: IdentifierGenerator;
  readonly random?: () => number;
}

export interface PublishOptions<TTransaction = unknown> {
  /** Transaction to write into. Default: the current scope, then `transactionResolver`. */
  readonly transaction?: TTransaction;
  /**
   * Ordering and partition key. Events with the same key are handled one at a
   * time per handler, in commit order. A delayed or retrying event holds back
   * later events of its key.
   */
  readonly key?: string;
  /** Deliver no earlier than this delay from now. */
  readonly delay?: Duration;
  readonly headers?: EventHeaders;
  /**
   * Default: inherited from the event being handled, otherwise the id of the
   * first published event (shared by every event of a `publishAll` call).
   */
  readonly correlationId?: string;
}

export interface PublishOneOptions<TTransaction = unknown> extends PublishOptions<TTransaction> {
  /**
   * Event id. Pass a deterministic id (for example derived from a business key)
   * to make publishing idempotent: when an event with this id is already
   * stored, nothing is written and the stored event is left unchanged.
   */
  readonly id?: string;
}

export type ReceiveStatus = 'accepted' | 'duplicate' | 'ignored';

type BusState = 'idle' | 'starting' | 'running' | 'stopping' | 'stopped';

const DEFAULTS = {
  concurrency: 10,
  pollInterval: '1s',
  leaseDuration: '30s',
  shutdownTimeout: '30s',
  completionBatchWindow: '2ms',
  completionBatchSize: 200,
  completedRetention: '7d',
  retentionInterval: '1h',
  routeTimeoutMs: 30_000,
} as const;

const TRANSPORT_HANDLER_PREFIX = 'relaybus.transport';
const WAKE_DELAY_MS = 25;

/**
 * Reliable event bus: events are stored with business data in one transaction,
 * then dispatched to every subscribed handler and route with independent
 * retries, leases and dead-lettering. A bus instance can be started once.
 */
export class RelayBus<TTransaction = unknown> {
  readonly #store: EventStore<TTransaction>;
  readonly #source: string;
  readonly #transactionMode: TransactionMode;
  readonly #transactionResolver: (() => TTransaction | undefined) | undefined;
  readonly #unroutedEvents: UnroutedEventPolicy;
  readonly #registry = new HandlerRegistry();
  readonly #defaults: RegistrationDefaults;
  readonly #transports = new Map<string, Transport>();
  readonly #subscriptions: readonly InboundSubscriptionOptions[];
  readonly #notifier: Notifier;
  readonly #logger: RelayBusLogger;
  readonly #clock: Clock;
  readonly #generateIdentifier: IdentifierGenerator;
  readonly #dispatcher: Dispatcher;
  readonly #dispatcherEnabled: boolean;
  readonly #setupStore: boolean;
  readonly #retention: { completedMs: number | null; deadMs: number | null; intervalMs: number };
  #retentionTimer: NodeJS.Timeout | undefined;
  #state: BusState = 'idle';
  #starting: Promise<void> | undefined;
  #stopping: Promise<void> | undefined;

  constructor(options: RelayBusOptions<TTransaction>) {
    this.#store = options.store;
    this.#source = options.source ?? 'relaybus';
    this.#transactionMode = oneOf(
      options.transactionMode ?? 'required',
      ['required', 'optional'],
      'transactionMode',
    );
    this.#unroutedEvents = oneOf(
      options.unroutedEvents ?? 'warn',
      ['warn', 'throw'],
      'unroutedEvents',
    );
    this.#transactionResolver = options.transactionResolver;
    this.#defaults = {
      retry: createRetryPolicy(options.retry),
      timeoutMs:
        options.handlerTimeout === undefined || options.handlerTimeout === false
          ? null
          : toTimerMilliseconds(options.handlerTimeout, 'handlerTimeout'),
    };
    this.#logger = options.logger ?? console;
    this.#notifier = new Notifier(options.observers ?? [], this.#logger);
    this.#clock = options.clock ?? systemClock;
    this.#generateIdentifier = options.generateIdentifier ?? (() => createUuidV7());
    this.#setupStore = options.setupStore ?? true;
    this.#retention = createRetentionSettings(options.retention);
    for (const transport of options.transports ?? []) {
      if (this.#transports.has(transport.name)) {
        throw new ConfigurationError(`Transport name '${transport.name}' is used twice`);
      }
      this.#transports.set(transport.name, transport);
    }
    for (const route of options.routes ?? []) {
      this.#registerRoute(route);
    }
    this.#subscriptions = options.subscriptions ?? [];
    for (const subscription of this.#subscriptions) {
      const transport = this.#requireTransport(subscription.transport);
      if (transport.subscribe === undefined) {
        throw new ConfigurationError(`Transport '${transport.name}' cannot subscribe`);
      }
    }
    this.#dispatcherEnabled = options.dispatcher?.enabled ?? true;
    this.#dispatcher = new Dispatcher({
      store: this.#store,
      registry: this.#registry,
      clock: this.#clock,
      random: options.random ?? Math.random,
      notifier: this.#notifier,
      settings: createDispatcherSettings(options.dispatcher, this.#generateIdentifier),
    });
  }

  get store(): EventStore<TTransaction> {
    return this.#store;
  }

  get isRunning(): boolean {
    return this.#state === 'running';
  }

  /** True while the bus runs and every transport reports itself ready. */
  isReady(): boolean {
    if (this.#state !== 'running') {
      return false;
    }
    for (const transport of this.#transports.values()) {
      if (transport.isReady?.() === false) {
        return false;
      }
    }
    return true;
  }

  /**
   * Subscribes a handler to an event class or type. Register handlers in every
   * process that publishes the event: deliveries are created at publish time
   * from the publisher's registrations.
   */
  subscribe<TPayload>(
    event: EventClass<TPayload>,
    handler: EventHandler<TPayload, TTransaction>,
    options: SubscribeOptions,
  ): void;
  subscribe(
    eventType: string,
    handler: EventHandler<unknown, TTransaction>,
    options: SubscribeOptions,
  ): void;
  subscribe(
    event: EventClass<unknown> | string,
    handler: EventHandler<unknown, TTransaction>,
    options: SubscribeOptions,
  ): void {
    const { type, schema } = describeEvent(event);
    this.#registry.register(
      {
        eventType: type,
        schema,
        handle: handler as EventHandler,
        options,
        forwardsToTransport: false,
      },
      this.#defaults,
    );
  }

  /** Registered handlers and routes, for diagnostics. */
  handlers(): readonly HandlerDescriptor[] {
    return this.#registry.all().map(toDescriptor);
  }

  /**
   * Stores an event in the current transaction. Handlers run after the
   * transaction commits. Returns the envelope that was written, or that would
   * have been written when the id already exists.
   */
  async publish<TPayload>(
    event: DomainEvent<TPayload>,
    options: PublishOneOptions<TTransaction> = {},
  ): Promise<EventEnvelope<TPayload>> {
    const [envelope] = await this.#publish([event], options, options.id);
    return envelope as EventEnvelope<TPayload>;
  }

  /** Stores several events atomically, in order, sharing one correlation id. */
  async publishAll(
    events: readonly DomainEvent[],
    options: PublishOptions<TTransaction> = {},
  ): Promise<EventEnvelope[]> {
    return this.#publish(events, options, undefined);
  }

  /**
   * Runs `work` in a new store transaction. `publish` calls inside it join the
   * transaction without passing it explicitly.
   */
  async transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T> {
    return this.#store.transaction((transaction) =>
      runInScope({ transaction }, () => work(transaction)),
    );
  }

  /**
   * Stores an event received from a broker and schedules it for local handlers.
   * Idempotent on the event id. Transports call it and acknowledge the message
   * only after it resolves. Throws {@link InvalidEnvelopeError} for malformed
   * events, which transports should dead-letter instead of redelivering.
   */
  async receive(envelope: EventEnvelope): Promise<ReceiveStatus> {
    const normalized = normalizeEnvelope(envelope);
    const registrations = this.#registry
      .forEventType(normalized.type)
      .filter((registration) => !registration.forwardsToTransport);
    if (registrations.length === 0) {
      this.#notifier.notify('onReceived', { envelope: normalized, status: 'ignored' });
      return 'ignored';
    }
    const now = this.#clock.now();
    const entry: AppendEntry = {
      event: normalized,
      deliveries: registrations.map((registration) => this.#createDelivery(registration, now)),
    };
    const result = await this.#store.transaction((transaction) =>
      this.#store.append(transaction, [entry], now),
    );
    const status: ReceiveStatus = result.duplicateEventIds.length > 0 ? 'duplicate' : 'accepted';
    this.#notifier.notify('onReceived', { envelope: normalized, status });
    if (result.appendedDeliveryCount > 0) {
      this.#scheduleWake();
    }
    return status;
  }

  /**
   * Creates the store schema, connects transports, subscribes and starts
   * dispatching. A bus starts once: after a failed start or a stop it is
   * stopped for good, because its transports are closed; create a new instance.
   */
  async start(): Promise<void> {
    if (this.#state !== 'idle') {
      throw new RelayBusError(
        this.#state === 'stopped'
          ? 'A stopped relaybus cannot be started again; create a new instance'
          : `Cannot start a relaybus that is ${this.#state}`,
      );
    }
    this.#state = 'starting';
    this.#starting = this.#startComponents();
    try {
      await this.#starting;
    } finally {
      this.#starting = undefined;
    }
  }

  /**
   * Stops dispatching, waits for running handlers, closes transports and the
   * store. Waits for a start in progress first. Safe to call more than once.
   */
  async stop(): Promise<void> {
    if (this.#stopping !== undefined) {
      return this.#stopping;
    }
    if (this.#state === 'starting') {
      await this.#starting!.catch(() => undefined);
    }
    if (this.#state === 'idle') {
      // Batches run by hand may have left settlements that the dispatcher keeps retrying in the background.
      await this.#dispatcher.stop();
      return;
    }
    if (this.#state !== 'running') {
      return;
    }
    this.#stopping = this.#stopComponents();
    return this.#stopping;
  }

  /**
   * Claims and runs one batch of due deliveries, resolving when they settle.
   * Returns how many ran. Intended for tests and scripts.
   */
  async runOnce(): Promise<number> {
    return this.#dispatcher.processBatch();
  }

  /**
   * Runs batches until nothing is due. Deliveries scheduled in the future
   * (retries, delays) are not waited for. Intended for tests.
   */
  async drain(maxBatches = 1_000): Promise<number> {
    let total = 0;
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const processed = await this.runOnce();
      if (processed === 0) {
        return total;
      }
      total += processed;
    }
    throw new RelayBusError(`drain did not finish within ${maxBatches} batches`);
  }

  listDeliveries(query: DeliveryQuery = {}): Promise<DeliveryRecord[]> {
    return this.#store.listDeliveries(query);
  }

  /** Moves dead deliveries back to pending with a fresh attempt budget. */
  async requeue(deliveryIds: readonly string[]): Promise<number> {
    const moved = await this.#store.requeue(deliveryIds, this.#clock.now());
    if (moved > 0) {
      this.#dispatcher.wake();
    }
    return moved;
  }

  statistics(): Promise<EventStoreStatistics> {
    return this.#store.statistics(this.#clock.now());
  }

  /** Applies the retention policy once. Runs automatically while the bus is running. */
  async purgeExpired(): Promise<PurgeResult> {
    const now = this.#clock.now().getTime();
    const { completedMs, deadMs } = this.#retention;
    try {
      const result = await this.#store.purge({
        ...(completedMs === null ? {} : { completedBefore: new Date(now - completedMs) }),
        ...(deadMs === null ? {} : { deadBefore: new Date(now - deadMs) }),
      });
      this.#notifier.notify('onPurged', { result });
      return result;
    } catch (error) {
      this.#notifier.failure('purge', error);
      throw error;
    }
  }

  async #startComponents(): Promise<void> {
    try {
      if (this.#setupStore) {
        await this.#store.setup?.();
      }
      for (const transport of this.#transports.values()) {
        await transport.connect?.();
      }
      for (const subscription of this.#subscriptions) {
        const transport = this.#transports.get(subscription.transport)!;
        await transport.subscribe!(subscription.destinations, async (envelope) => {
          await this.receive(envelope);
        });
      }
    } catch (error) {
      await this.#closeTransports();
      this.#state = 'stopped';
      throw error;
    }
    if (this.#dispatcherEnabled) {
      this.#dispatcher.start();
    }
    if (this.#retention.completedMs !== null || this.#retention.deadMs !== null) {
      this.#retentionTimer = setInterval(() => {
        this.purgeExpired().catch(() => undefined);
      }, this.#retention.intervalMs);
      this.#retentionTimer.unref();
    }
    this.#state = 'running';
    this.#logger.log(
      `[RelayBus](start) relaybus started | handlers=${this.#registry.all().length} dispatcher=${this.#dispatcherEnabled}`,
    );
  }

  async #stopComponents(): Promise<void> {
    this.#state = 'stopping';
    clearInterval(this.#retentionTimer);
    try {
      await this.#dispatcher.stop();
      await this.#closeTransports();
      await this.#store.close?.();
    } finally {
      this.#state = 'stopped';
    }
  }

  async #publish(
    events: readonly DomainEvent[],
    options: PublishOptions<TTransaction>,
    id: string | undefined,
  ): Promise<EventEnvelope[]> {
    if (events.length === 0) {
      return [];
    }
    const now = this.#clock.now();
    const delayMs = options.delay === undefined ? 0 : toMilliseconds(options.delay, 'delay');
    const availableAt = new Date(now.getTime() + delayMs);
    const scope = getCurrentScope();
    const entries: AppendEntry[] = [];
    let correlationId = options.correlationId ?? scope?.event?.correlationId ?? scope?.event?.id;
    for (const event of events) {
      const envelope = await this.#createEnvelope(event, options, id, scope, now, correlationId);
      correlationId = envelope.correlationId!;
      const registrations = this.#registry.forEventType(envelope.type);
      if (registrations.length === 0) {
        this.#reportUnrouted(envelope);
      }
      entries.push({
        event: envelope,
        deliveries: registrations.map((registration) =>
          this.#createDelivery(registration, availableAt),
        ),
      });
    }
    const deliverable = entries.filter((entry) => entry.deliveries.length > 0);
    if (deliverable.length > 0) {
      const result = await this.#appendInTransaction(deliverable, options, scope, now);
      const appended = new Set(result.appendedEventIds);
      const published = deliverable
        .map((entry) => entry.event)
        .filter((envelope) => appended.has(envelope.id));
      if (published.length > 0) {
        this.#notifier.notify('onPublished', {
          envelopes: published,
          deliveries: result.appendedDeliveryCount,
        });
        if (delayMs === 0) {
          this.#scheduleWake();
        }
      }
    }
    return entries.map((entry) => entry.event);
  }

  #reportUnrouted(envelope: EventEnvelope): void {
    const message = `No handler or route for event type, event not stored | type=${envelope.type} id=${envelope.id}`;
    if (this.#unroutedEvents === 'throw') {
      throw new ConfigurationError(message);
    }
    this.#logger.warn(`[RelayBus](publish) ${message}`);
  }

  async #appendInTransaction(
    entries: readonly AppendEntry[],
    options: PublishOptions<TTransaction>,
    scope: ExecutionScope | undefined,
    now: Date,
  ): Promise<{ appendedEventIds: readonly string[]; appendedDeliveryCount: number }> {
    const transaction =
      options.transaction ??
      (scope?.transaction as TTransaction | undefined) ??
      this.#transactionResolver?.();
    if (transaction !== undefined) {
      return this.#store.append(transaction, entries, now);
    }
    if (this.#transactionMode === 'required') {
      throw new TransactionRequiredError(
        'publish found no transaction. Pass { transaction }, publish inside bus.transaction(), configure transactionResolver, or set transactionMode to optional',
      );
    }
    return this.#store.transaction((ownTransaction) =>
      this.#store.append(ownTransaction, entries, now),
    );
  }

  async #createEnvelope(
    event: DomainEvent,
    options: PublishOptions<TTransaction>,
    id: string | undefined,
    scope: ExecutionScope | undefined,
    now: Date,
    correlationId: string | undefined,
  ): Promise<EventEnvelope> {
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
    const eventId = id ?? this.#generateIdentifier();
    return {
      id: eventId,
      type: event.type,
      source: this.#source,
      key: options.key ?? null,
      payload,
      headers: { ...options.headers },
      correlationId: correlationId ?? eventId,
      causationId: scope?.event?.id ?? null,
      occurredAt: now,
    };
  }

  #createDelivery(registration: HandlerRegistration, availableAt: Date): NewDelivery {
    return { id: this.#generateIdentifier(), handler: registration.id, availableAt };
  }

  #registerRoute(route: RouteOptions): void {
    const transport = this.#requireTransport(route.transport);
    const { type } = describeEvent(route.event);
    const destination = route.destination ?? type;
    this.#registry.register(
      {
        eventType: type,
        schema: undefined,
        handle: (envelope) => transport.publish(envelope, destination),
        options: {
          id: `${TRANSPORT_HANDLER_PREFIX}:${transport.name}:${destination}:${type}`,
          ...(route.retry === undefined ? {} : { retry: route.retry }),
          ...(route.timeout === undefined ? {} : { timeout: route.timeout }),
        },
        forwardsToTransport: true,
      },
      { retry: DEFAULT_ROUTE_RETRY_POLICY, timeoutMs: DEFAULTS.routeTimeoutMs },
    );
  }

  #requireTransport(name: string): Transport {
    const transport = this.#transports.get(name);
    if (transport === undefined) {
      throw new ConfigurationError(`No transport named '${name}' is configured`);
    }
    return transport;
  }

  #scheduleWake(): void {
    // A store that reports new deliveries wakes the dispatcher itself, right after the commit.
    if (this.#state !== 'running' || this.#dispatcher.watchesNewDeliveries) {
      return;
    }
    // The publishing transaction commits after publish returns; claim shortly after.
    setTimeout(() => this.#dispatcher.wake(), WAKE_DELAY_MS).unref();
  }

  async #closeTransports(): Promise<void> {
    for (const transport of this.#transports.values()) {
      try {
        await transport.close();
      } catch (error) {
        this.#logger.error(
          `[RelayBus](closeTransports) Failed to close transport | transport=${transport.name} error=${toErrorInfo(error).message}`,
        );
      }
    }
  }
}

function toDescriptor(registration: HandlerRegistration): HandlerDescriptor {
  return {
    id: registration.id,
    eventType: registration.eventType,
    retry: registration.retry,
    timeoutMs: registration.timeoutMs,
    concurrency: registration.concurrency,
    transactional: registration.transactional,
    forwardsToTransport: registration.forwardsToTransport,
  };
}

function describeEvent(event: EventReference): {
  type: string;
  schema: StandardSchemaV1 | undefined;
} {
  if (typeof event === 'string') {
    assertEventType(event);
    return { type: event, schema: undefined };
  }
  assertEventType(event.type);
  return { type: event.type, schema: (event as Partial<EventClass>).schema };
}

/**
 * Validates an event received from a broker and fills fields that JSON-based
 * wire formats commonly drop (`undefined` key, headers, correlation ids).
 */
function normalizeEnvelope(envelope: EventEnvelope): EventEnvelope {
  if (typeof envelope.id !== 'string' || envelope.id.length === 0) {
    throw new InvalidEnvelopeError('Received event has no id');
  }
  if (typeof envelope.type !== 'string') {
    throw new InvalidEnvelopeError(`Received event ${envelope.id} has no type`);
  }
  try {
    assertEventType(envelope.type);
  } catch (error) {
    throw new InvalidEnvelopeError(toErrorInfo(error).message);
  }
  const occurredAt = toDate(envelope.occurredAt);
  if (occurredAt === undefined) {
    throw new InvalidEnvelopeError(`Received event ${envelope.id} has an invalid occurredAt`);
  }
  return {
    id: envelope.id,
    type: envelope.type,
    source: typeof envelope.source === 'string' ? envelope.source : 'unknown',
    key: optionalText(envelope.key),
    payload: envelope.payload,
    headers: { ...envelope.headers },
    correlationId: optionalText(envelope.correlationId),
    causationId: optionalText(envelope.causationId),
    occurredAt,
  };
}

function toDate(value: Date | string): Date | undefined {
  const date = typeof value === 'string' ? new Date(value) : value;
  return date instanceof Date && !Number.isNaN(date.getTime()) ? date : undefined;
}

function optionalText(value: string | null | undefined): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function oneOf<T extends string>(value: T, allowed: readonly T[], name: string): T {
  if (!allowed.includes(value)) {
    throw new ConfigurationError(
      `${name} must be one of ${allowed.map((option) => `'${option}'`).join(', ')}, received '${String(value)}'`,
    );
  }
  return value;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new ConfigurationError(`${name} must be a positive integer, received ${value}`);
  }
  return value;
}

function createDispatcherSettings(
  options: DispatcherOptions | undefined,
  generateIdentifier: IdentifierGenerator,
): DispatcherSettings {
  const concurrency = positiveInteger(
    options?.concurrency ?? DEFAULTS.concurrency,
    'dispatcher.concurrency',
  );
  return {
    owner: `${hostname()}:${process.pid}:${generateIdentifier()}`,
    concurrency,
    batchSize: positiveInteger(options?.batchSize ?? concurrency, 'dispatcher.batchSize'),
    pollIntervalMs: toTimerMilliseconds(
      options?.pollInterval ?? DEFAULTS.pollInterval,
      'dispatcher.pollInterval',
    ),
    leaseDurationMs: toTimerMilliseconds(
      options?.leaseDuration ?? DEFAULTS.leaseDuration,
      'dispatcher.leaseDuration',
    ),
    shutdownTimeoutMs: toTimerMilliseconds(
      options?.shutdownTimeout ?? DEFAULTS.shutdownTimeout,
      'dispatcher.shutdownTimeout',
    ),
    completionBatchWindowMs: toMilliseconds(
      options?.completionBatchWindow ?? DEFAULTS.completionBatchWindow,
      'dispatcher.completionBatchWindow',
    ),
    completionBatchSize: positiveInteger(
      options?.completionBatchSize ?? DEFAULTS.completionBatchSize,
      'dispatcher.completionBatchSize',
    ),
  };
}

function createRetentionSettings(options: RetentionOptions | undefined): {
  completedMs: number | null;
  deadMs: number | null;
  intervalMs: number;
} {
  const completed = options?.completed ?? DEFAULTS.completedRetention;
  const dead = options?.dead ?? false;
  return {
    completedMs: completed === false ? null : toMilliseconds(completed, 'retention.completed'),
    deadMs: dead === false ? null : toMilliseconds(dead, 'retention.dead'),
    intervalMs: toTimerMilliseconds(
      options?.interval ?? DEFAULTS.retentionInterval,
      'retention.interval',
    ),
  };
}
