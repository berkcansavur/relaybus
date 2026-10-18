import { hostname } from 'node:os';
import { systemClock, type Clock } from './clock.js';
import { Dispatcher, type DispatcherSettings } from './dispatcher.js';
import { toMilliseconds, type Duration } from './duration.js';
import {
  ConfigurationError,
  RelayBusError,
  TransactionRequiredError,
  type ErrorInfo,
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
import { getCurrentScope, runInScope } from './execution-scope.js';
import {
  HandlerRegistry,
  type EventHandler,
  type HandlerRegistration,
  type SubscribeOptions,
} from './handler-registry.js';
import { createUuidV7, type IdentifierGenerator } from './identifier.js';
import { Notifier } from './notifier.js';
import type { RelayBusLogger, RelayBusObserver } from './observer.js';
import { createRetryPolicy, type RetryPolicyOptions } from './retry-policy.js';
import type { StandardSchemaV1 } from './standard-schema.js';
import type { Transport } from './transport.js';

/**
 * - `required`: `publish` throws {@link TransactionRequiredError} when it finds no transaction.
 * - `optional`: `publish` opens its own transaction instead. The event is still durable,
 *   but no longer atomic with business writes made outside that transaction.
 */
export type TransactionMode = 'required' | 'optional';

export interface DispatcherOptions {
  /** Run handlers in this process. Disable on publish-only processes. Default true. */
  readonly enabled?: boolean;
  /** Maximum handlers running at once. Default 10. */
  readonly concurrency?: number;
  /** Maximum deliveries claimed per query. Default equals concurrency. */
  readonly batchSize?: number;
  /** Wait between claims when idle. Default `'1s'`. */
  readonly pollInterval?: Duration;
  /** Lease length; renewed every third of it while the handler runs. Default `'30s'`. */
  readonly leaseDuration?: Duration;
  /** How long `stop` waits for running handlers. Default `'30s'`. */
  readonly shutdownTimeout?: Duration;
}

export interface RetentionOptions {
  /** Delete completed deliveries older than this. Default `'7d'`. `false` keeps them. */
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
  readonly retry?: RetryPolicyOptions;
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
  /** Default retry policy of every handler and route. */
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
  /** Ordering and partition key. Events with the same key are handled one at a time, in order. */
  readonly key?: string;
  /** Deliver no earlier than this delay from now. */
  readonly delay?: Duration;
  readonly headers?: EventHeaders;
  /** Default: inherited from the event being handled, otherwise the event's own id. */
  readonly correlationId?: string;
}

export interface PublishOneOptions<TTransaction = unknown> extends PublishOptions<TTransaction> {
  /**
   * Event id. Pass a deterministic id (for example derived from a business key)
   * to make publishing idempotent: a second publish with the same id is ignored.
   */
  readonly id?: string;
}

export type ReceiveStatus = 'accepted' | 'duplicate' | 'ignored';

const DEFAULTS = {
  concurrency: 10,
  pollInterval: '1s',
  leaseDuration: '30s',
  shutdownTimeout: '30s',
  completedRetention: '7d',
  retentionInterval: '1h',
} as const;

const TRANSPORT_HANDLER_PREFIX = 'relaybus.transport';
const WAKE_DELAY_MS = 25;

/**
 * Reliable event bus: events are stored with business data in one transaction,
 * then dispatched to every subscribed handler and route with independent
 * retries, leases and dead-lettering.
 */
export class RelayBus<TTransaction = unknown> {
  readonly #store: EventStore<TTransaction>;
  readonly #source: string;
  readonly #transactionMode: TransactionMode;
  readonly #transactionResolver: (() => TTransaction | undefined) | undefined;
  readonly #registry = new HandlerRegistry();
  readonly #defaults: { retry: ReturnType<typeof createRetryPolicy>; timeoutMs: number | null };
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
  #state: 'idle' | 'starting' | 'running' | 'stopping' = 'idle';

  constructor(options: RelayBusOptions<TTransaction>) {
    this.#store = options.store;
    this.#source = options.source ?? 'relaybus';
    this.#transactionMode = options.transactionMode ?? 'required';
    if (this.#transactionMode !== 'required' && this.#transactionMode !== 'optional') {
      throw new ConfigurationError(
        `transactionMode must be 'required' or 'optional', received '${String(this.#transactionMode)}'`,
      );
    }
    this.#transactionResolver = options.transactionResolver;
    this.#defaults = {
      retry: createRetryPolicy(options.retry),
      timeoutMs:
        options.handlerTimeout === undefined || options.handlerTimeout === false
          ? null
          : positive(toMilliseconds(options.handlerTimeout, 'handlerTimeout'), 'handlerTimeout'),
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

  /** Subscribes a handler to an event class or type. */
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
  handlers(): readonly HandlerRegistration[] {
    return this.#registry.all();
  }

  /**
   * Stores an event in the current transaction. Handlers run after the
   * transaction commits. Returns the stored envelope.
   */
  async publish<TPayload>(
    event: DomainEvent<TPayload>,
    options: PublishOneOptions<TTransaction> = {},
  ): Promise<EventEnvelope<TPayload>> {
    const [envelope] = await this.#publish([event], options, options.id);
    return envelope as EventEnvelope<TPayload>;
  }

  /** Stores several events atomically, in order. */
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
   * Idempotent on the event id. Transports call it; resolve before acknowledging.
   */
  async receive(envelope: EventEnvelope): Promise<ReceiveStatus> {
    assertEnvelope(envelope);
    const registrations = this.#registry
      .forEventType(envelope.type)
      .filter((registration) => !registration.forwardsToTransport);
    if (registrations.length === 0) {
      return 'ignored';
    }
    const now = this.#clock.now();
    const entry: AppendEntry = {
      event: envelope,
      deliveries: registrations.map((registration) => this.#createDelivery(registration, now)),
    };
    const result = await this.#store.transaction((transaction) =>
      this.#store.append(transaction, [entry], now),
    );
    const duplicate = result.duplicateEventIds.length > 0;
    this.#notifier.notify('onReceived', { envelope, duplicate });
    if (result.appendedDeliveryCount > 0) {
      this.#scheduleWake();
    }
    return duplicate ? 'duplicate' : 'accepted';
  }

  /** Creates the store schema, connects transports, subscribes and starts dispatching. */
  async start(): Promise<void> {
    if (this.#state !== 'idle') {
      throw new RelayBusError(`Cannot start a relaybus that is ${this.#state}`);
    }
    this.#state = 'starting';
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
      this.#state = 'idle';
      await this.#closeTransports();
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

  /** Stops dispatching, waits for running handlers, closes transports and the store. */
  async stop(): Promise<void> {
    if (this.#state !== 'running') {
      return;
    }
    this.#state = 'stopping';
    clearInterval(this.#retentionTimer);
    await this.#dispatcher.stop();
    await this.#closeTransports();
    await this.#store.close?.();
    this.#state = 'idle';
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
    for (const event of events) {
      const envelope = await this.#createEnvelope(event, options, id, scope?.event, now);
      const registrations = this.#registry.forEventType(envelope.type);
      if (registrations.length === 0) {
        this.#logger.warn(
          `[RelayBus](publish) No handler or route for event type, event not stored | type=${envelope.type} id=${envelope.id}`,
        );
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
      const result = await this.#appendInTransaction(deliverable, options, scope?.transaction, now);
      this.#notifier.notify('onPublished', {
        envelopes: deliverable.map((entry) => entry.event),
        deliveries: result,
      });
      if (delayMs === 0) {
        this.#scheduleWake();
      }
    }
    return entries.map((entry) => entry.event);
  }

  async #appendInTransaction(
    entries: readonly AppendEntry[],
    options: PublishOptions<TTransaction>,
    scopedTransaction: unknown,
    now: Date,
  ): Promise<number> {
    const transaction =
      options.transaction ??
      (scopedTransaction as TTransaction | undefined) ??
      this.#transactionResolver?.();
    if (transaction !== undefined) {
      return (await this.#store.append(transaction, entries, now)).appendedDeliveryCount;
    }
    if (this.#transactionMode === 'required') {
      throw new TransactionRequiredError(
        'publish found no transaction. Pass { transaction }, publish inside bus.transaction(), configure transactionResolver, or set transactionMode to optional',
      );
    }
    return this.#store.transaction(
      async (ownTransaction) =>
        (await this.#store.append(ownTransaction, entries, now)).appendedDeliveryCount,
    );
  }

  async #createEnvelope(
    event: DomainEvent,
    options: PublishOptions<TTransaction>,
    id: string | undefined,
    cause: EventEnvelope | undefined,
    now: Date,
  ): Promise<EventEnvelope> {
    const eventClass = event.constructor as Partial<EventClass>;
    if (typeof event.type !== 'string') {
      throw new ConfigurationError(
        'publish expects an instance of a class created with defineEvent',
      );
    }
    assertEventType(event.type);
    const payload = await validatePayload(event.type, eventClass.schema, event.payload);
    const eventId = id ?? this.#generateIdentifier();
    if (options.key !== undefined && options.key.length === 0) {
      throw new ConfigurationError('key must not be empty');
    }
    return {
      id: eventId,
      type: event.type,
      source: this.#source,
      key: options.key ?? null,
      payload,
      headers: { ...options.headers },
      correlationId: options.correlationId ?? cause?.correlationId ?? cause?.id ?? eventId,
      causationId: cause?.id ?? null,
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
          id: `${TRANSPORT_HANDLER_PREFIX}:${transport.name}:${destination}`,
          ...(route.retry === undefined ? {} : { retry: route.retry }),
        },
        forwardsToTransport: true,
      },
      this.#defaults,
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
    if (this.#state !== 'running') {
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
          `[RelayBus](closeTransports) Failed to close transport | transport=${transport.name} error=${(error as ErrorInfo).message}`,
        );
      }
    }
  }
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

function assertEnvelope(envelope: EventEnvelope): void {
  if (typeof envelope.id !== 'string' || envelope.id.length === 0) {
    throw new ConfigurationError('Received event has no id');
  }
  assertEventType(envelope.type);
  if (!(envelope.occurredAt instanceof Date) || Number.isNaN(envelope.occurredAt.getTime())) {
    throw new ConfigurationError(`Received event ${envelope.id} has an invalid occurredAt`);
  }
}

function positive(value: number, name: string): number {
  if (value <= 0) {
    throw new ConfigurationError(`${name} must be greater than zero`);
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
    pollIntervalMs: positive(
      toMilliseconds(options?.pollInterval ?? DEFAULTS.pollInterval, 'dispatcher.pollInterval'),
      'dispatcher.pollInterval',
    ),
    leaseDurationMs: positive(
      toMilliseconds(options?.leaseDuration ?? DEFAULTS.leaseDuration, 'dispatcher.leaseDuration'),
      'dispatcher.leaseDuration',
    ),
    shutdownTimeoutMs: toMilliseconds(
      options?.shutdownTimeout ?? DEFAULTS.shutdownTimeout,
      'dispatcher.shutdownTimeout',
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
    intervalMs: positive(
      toMilliseconds(options?.interval ?? DEFAULTS.retentionInterval, 'retention.interval'),
      'retention.interval',
    ),
  };
}
