import {
  assertCatalogsCovered,
  toCatalogRequirement,
  type CatalogDeclaration,
  type CatalogRequirement,
  type EventTypeRegistrations,
} from './catalog-coverage.js';
import { BusObservation, withTraceHeaders, type ReceiveOrigin } from './bus-observation.js';
import { CatalogPublisher } from './catalog-publisher.js';
import {
  resolveCatalogHandlers,
  type Catalog,
  type CatalogEventSelection,
  type HandlersOf,
  type SubscribeCatalogOptions,
} from './catalog.js';
import { systemClock, type Clock } from './clock.js';
import { DispatchObservation } from './dispatch-observation.js';
import { Dispatcher, type DispatcherSettings } from './dispatcher.js';
import { toMilliseconds, toTimerMilliseconds } from './duration.js';
import {
  ConfigurationError,
  DrainIncompleteError,
  LifecycleError,
  TransactionRequiredError,
  toErrorInfo,
} from './errors.js';
import type {
  AppendEntry,
  CloseOptions,
  DeliveryQuery,
  DeliveryRecord,
  EventStore,
  EventStoreStatistics,
  NewDelivery,
  PurgeResult,
} from './event-store.js';
import {
  assertEventType,
  keyFromDefinition,
  validatePayload,
  type DomainEvent,
  type EventClass,
  type EventEnvelope,
  type EventReference,
} from './event.js';
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
import { schedule, type CancelTimer } from './internal/timers.js';
import { Notifier } from './notifier.js';
import type { RelayBusLogger } from './observer.js';
import { normalizeEnvelope } from './received-envelope.js';
import type {
  InboundSubscriptionOptions,
  PublishOneOptions,
  PublishOptions,
  ReceiveStatus,
  RelayBusOptions,
  RouteOptions,
  TransactionMode,
  UnroutedEventPolicy,
} from './relay-bus-options.js';
import {
  DEFAULTS,
  createDispatcherSettings,
  createHandlerDefaults,
  createRetentionSettings,
  isDispatcherEnabled,
  oneOf,
  resolveBacklogInterval,
  resolveTransactionTimeout,
} from './relay-bus-settings.js';
import { DEFAULT_ROUTE_RETRY_POLICY } from './retry-policy.js';
import { groupRoutes, type RouteGroup } from './route-groups.js';
import {
  withinDeadline,
  type ShutdownDeadline,
  type StopOptions,
  type StopResult,
} from './shutdown-deadline.js';
import { runBudgetedTransaction } from './transaction-budget.js';
import type { StandardSchemaV1 } from './standard-schema.js';
import type { Transport } from './transport.js';

export type {
  DispatcherOptions,
  InboundSubscriptionOptions,
  PublishOneOptions,
  PublishOptions,
  ReceiveStatus,
  RelayBusOptions,
  RetentionOptions,
  RouteOptions,
  TransactionMode,
  UnroutedEventPolicy,
} from './relay-bus-options.js';

type BusState = 'idle' | 'starting' | 'running' | 'stopping' | 'stopped';

/** Batches `drain` runs at most before it gives up. */
const DEFAULT_DRAIN_BATCHES = 1_000;
const WAKE_DELAY_MS = 25;

/** The events of one publish call with what their envelopes and deliveries inherit. */
type CreateEntriesInput<TTransaction> = {
  readonly events: readonly DomainEvent[];
  readonly options: PublishOptions<TTransaction>;
  readonly id: string | undefined;
  readonly scope: ExecutionScope | undefined;
  readonly now: Date;
  readonly availableAt: Date;
};

/** The entries of one publish call that have deliveries, and how to append them. */
type AppendDeliverableInput<TTransaction> = {
  readonly entries: readonly AppendEntry[];
  readonly options: PublishOptions<TTransaction>;
  readonly scope: ExecutionScope | undefined;
  readonly now: Date;
  readonly delayMs: number;
};

/** One event of a publish call with what it inherits from the call and the running handler. */
type CreateEnvelopeInput<TTransaction> = {
  readonly event: DomainEvent;
  readonly options: PublishOptions<TTransaction>;
  readonly id: string | undefined;
  readonly scope: ExecutionScope | undefined;
  readonly now: Date;
  readonly correlationId: string | undefined;
};

/**
 * Reliable event bus: events are stored with business data in one transaction,
 * then dispatched to every subscribed handler and route with independent
 * retries, leases and dead-lettering. A bus instance can be started once.
 */
export class RelayBus<TTransaction = unknown> {
  readonly #store: EventStore<TTransaction>;
  readonly #source: string;
  readonly #transactionMode: TransactionMode;
  readonly #transactionTimeoutMs: number;
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
  readonly #catalogRequirements: CatalogRequirement[] = [];
  readonly #setupStore: boolean;
  readonly #retention: { completedMs: number | null; deadMs: number | null; intervalMs: number };
  #retentionTimer: CancelTimer | undefined;
  readonly #observation: BusObservation | undefined;
  readonly #backlogIntervalMs: number | undefined;
  #backlogTimer: CancelTimer | undefined;
  readonly #shutdownTimeoutMs: number;
  #state: BusState = 'idle';
  #starting: Promise<void> | undefined;
  #stopping: Promise<StopResult> | undefined;

  constructor(options: RelayBusOptions<TTransaction>) {
    this.#store = options.store;
    this.#source = options.source ?? 'relaybus';
    this.#transactionMode = oneOf(options.transactionMode ?? 'required', {
      allowed: ['required', 'optional'],
      name: 'transactionMode',
    });
    this.#unroutedEvents = oneOf(options.unroutedEvents ?? 'warn', {
      allowed: ['warn', 'throw'],
      name: 'unroutedEvents',
    });
    this.#transactionResolver = options.transactionResolver;
    this.#transactionTimeoutMs = resolveTransactionTimeout(options);
    this.#defaults = createHandlerDefaults(options);
    this.#logger = options.logger ?? console;
    this.#notifier = new Notifier(options.observers ?? [], this.#logger);
    this.#clock = options.clock ?? systemClock;
    this.#observation = BusObservation.create({
      notifier: this.#notifier,
      tracer: options.tracer,
      clock: this.#clock,
    });
    this.#backlogIntervalMs = resolveBacklogInterval(options);
    this.#generateIdentifier = options.generateIdentifier ?? (() => createUuidV7());
    this.#setupStore = options.setupStore ?? true;
    this.#retention = createRetentionSettings(options.retention);
    this.#subscriptions = options.subscriptions ?? [];
    this.#registerConnections(options);
    this.#dispatcherEnabled = isDispatcherEnabled(options);
    const settings = createDispatcherSettings(options.dispatcher, this.#generateIdentifier);
    this.#shutdownTimeoutMs = settings.shutdownTimeoutMs;
    this.#dispatcher = this.#createDispatcher(settings, options);
  }

  #createDispatcher(
    settings: DispatcherSettings,
    options: RelayBusOptions<TTransaction>,
  ): Dispatcher {
    return new Dispatcher({
      store: this.#store,
      registry: this.#registry,
      clock: this.#clock,
      random: options.random ?? Math.random,
      notifier: this.#notifier,
      settings,
      observation: DispatchObservation.create({
        notifier: this.#notifier,
        tracer: options.tracer,
        clock: this.#clock,
      }),
    });
  }

  /** Registers the transports, then the routes, checks the subscriptions and registers the catalogs. */
  #registerConnections(options: RelayBusOptions<TTransaction>): void {
    for (const transport of options.transports ?? []) {
      if (this.#transports.has(transport.name)) {
        throw new ConfigurationError(`Transport name '${transport.name}' is used twice`);
      }
      this.#transports.set(transport.name, transport);
    }
    this.#registerRoutes(options.routes ?? []);
    for (const subscription of this.#subscriptions) {
      const transport = this.#requireTransport(subscription.transport);
      if (transport.subscribe === undefined) {
        throw new ConfigurationError(`Transport '${transport.name}' cannot subscribe`);
      }
    }
    for (const catalog of options.catalogs ?? []) {
      this.registerCatalog(catalog);
    }
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
  // eslint-disable-next-line relaybus/max-two-params -- public RelayBus.subscribe signature
  subscribe(
    event: EventClass<unknown> | string,
    handler: EventHandler<unknown, TTransaction>,
    options: SubscribeOptions,
  ): void {
    const { type, schema } = describeEvent(event);
    this.#registry.register(
      {
        eventTypes: [type],
        schema,
        handle: handler as EventHandler,
        options,
        forwardsToTransport: false,
      },
      this.#defaults,
    );
  }

  /**
   * Subscribes the `on${EventName}` methods of `handlers` to the events of
   * `catalog`, or to the events named in `options.events`, with the stable
   * handler ids `${prefix}.on${EventName}`. The prefix is `options.idPrefix`,
   * else the catalog name, else the class name of `handlers`. Methods run with
   * `this` bound to `handlers`; methods of events that are not selected are
   * ignored. A missing handler of a selected event fails to compile, and
   * throws {@link ConfigurationError} at runtime, as does an invalid or taken
   * id; handlers registered before the failure stay registered.
   *
   * ```ts
   * bus.subscribeCatalog(OrderEvents, new BillingHandlers(), { events: ['OrderPlaced'] });
   * ```
   */
  subscribeCatalog<TCatalog extends Catalog, const TName extends keyof TCatalog>(
    catalog: TCatalog,
    handlers: NoInfer<HandlersOf<TCatalog, TName, TTransaction>>,
    options: SubscribeCatalogOptions<NoInfer<TCatalog>, TName> & {
      readonly events: readonly TName[];
    },
  ): void;
  /** Without a statically known `events` list, every handler of the catalog is required. */
  subscribeCatalog<TCatalog extends Catalog>(
    catalog: TCatalog,
    handlers: HandlersOf<TCatalog, keyof TCatalog, TTransaction>,
    options?: SubscribeCatalogOptions<NoInfer<TCatalog>>,
  ): void;
  // eslint-disable-next-line relaybus/max-two-params -- public RelayBus.subscribeCatalog signature
  subscribeCatalog<TCatalog extends Catalog>(
    catalog: TCatalog,
    handlers: object,
    options: SubscribeCatalogOptions<TCatalog, keyof TCatalog> = {},
  ): void {
    for (const binding of resolveCatalogHandlers(catalog, { handlers, options })) {
      this.subscribe(binding.eventClass as EventClass<unknown>, binding.handle, binding.options);
    }
  }

  /**
   * Adds a catalog, or a `consume` / `produce` declaration, to the checks
   * `start()` runs; `start()` throws a {@link ConfigurationError} naming each
   * event that is not covered and what it lacks. Equivalent to the `catalogs`
   * option.
   */
  registerCatalog(catalog: Catalog | CatalogDeclaration): void {
    this.#catalogRequirements.push(toCatalogRequirement(catalog));
  }

  /**
   * A view of this bus that publishes only the events of `catalog`, or only
   * the events named in `options.events` (for example the events this service
   * produces). Other events fail to compile and are rejected at runtime.
   */
  forCatalog<TCatalog extends Catalog, const TName extends keyof TCatalog = keyof TCatalog>(
    catalog: TCatalog,
    options: CatalogEventSelection<TName> = {},
  ): CatalogPublisher<TCatalog, TTransaction, TName> {
    return new CatalogPublisher<TCatalog, TTransaction, TName>(catalog, this, options);
  }

  /** Registered handlers and routes, for diagnostics. */
  handlers(): readonly HandlerDescriptor[] {
    return this.#registry
      .all()
      .flatMap((registration) =>
        registration.eventTypes.map((eventType) => toDescriptor(registration, eventType)),
      );
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
    const [envelope] = await this.#observedPublish([event], { options, id: options.id });
    return envelope as EventEnvelope<TPayload>;
  }

  /** Stores several events atomically, in order, sharing one correlation id. */
  async publishAll(
    events: readonly DomainEvent[],
    options: PublishOptions<TTransaction> = {},
  ): Promise<EventEnvelope[]> {
    return this.#observedPublish(events, { options, id: undefined });
  }

  /**
   * Runs `work` in a new store transaction. `publish` calls inside it join the
   * transaction without passing it explicitly. The transaction is held to `transactionTimeout`
   * and rolled back with a {@link TransactionTimeoutError} when it runs past it.
   */
  async transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T> {
    return runBudgetedTransaction(this.#store, {
      clock: this.#clock,
      timeoutMs: this.#transactionTimeoutMs,
      work: (transaction) => runInScope({ transaction }, () => work(transaction)),
    });
  }

  /**
   * The transaction `publish` would join right now: the one of the enclosing `bus.transaction`
   * (or `@RelayBusTransaction()` method, or transactional handler), else the one
   * `transactionResolver` finds. Repositories use it to write in the same transaction.
   */
  currentTransaction(): TTransaction | undefined {
    return (
      (getCurrentScope()?.transaction as TTransaction | undefined) ?? this.#transactionResolver?.()
    );
  }

  /**
   * Stores an event received from a broker and schedules it for local handlers.
   * Idempotent on the event id. Transports call it and acknowledge the message
   * only after it resolves. Throws {@link InvalidEnvelopeError} for malformed
   * events, which transports should dead-letter instead of redelivering.
   */
  async receive(envelope: EventEnvelope): Promise<ReceiveStatus> {
    return this.#observation === undefined
      ? this.#receive(envelope)
      : this.#observedReceive(envelope, undefined);
  }

  #observedReceive(
    envelope: EventEnvelope,
    origin: ReceiveOrigin | undefined,
  ): Promise<ReceiveStatus> {
    return this.#observation!.receive({ envelope, origin }, () => this.#receive(envelope));
  }

  async #receive(envelope: EventEnvelope): Promise<ReceiveStatus> {
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
      throw new LifecycleError(
        this.#state === 'stopped'
          ? 'A stopped relaybus cannot be started again; create a new instance'
          : `Cannot start a relaybus that is ${this.#state}`,
      );
    }
    // Before any side effect, so a failed check leaves the bus idle and startable.
    assertCatalogsCovered(this.#catalogRequirements, (type) => this.#registrationsOf(type));
    this.#state = 'starting';
    this.#starting = this.#startComponents();
    try {
      await this.#starting;
    } finally {
      this.#starting = undefined;
    }
  }

  /**
   * Stops claiming, waits for running handlers, hands back leases, closes transports and the store,
   * all within `options.timeout` (default `dispatcher.shutdownTimeout`). When the deadline passes it
   * aborts the handlers still running, force-closes what relaybus opened itself (never pools or
   * clients the application passed in) and resolves anyway: it never rejects because of the
   * deadline. Abandoned deliveries stay leased until their lease expires and then run again
   * (at-least-once). Waits for a start in progress first. Safe to call more than once: later calls
   * get the result of the first.
   */
  async stop(options: StopOptions = {}): Promise<StopResult> {
    const timeoutMs =
      options.timeout === undefined
        ? this.#shutdownTimeoutMs
        : toTimerMilliseconds(options.timeout, 'timeout');
    if (this.#stopping !== undefined) {
      return this.#stopping;
    }
    if (this.#state !== 'running' && this.#state !== 'starting') {
      return this.#stopOutsideRun(timeoutMs);
    }
    this.#stopping = this.#stopComponents(timeoutMs);
    const result = await this.#stopping;
    if (!result.graceful) {
      this.#logger.warn(
        `[RelayBus](stop) Shutdown deadline passed, forced close | abandoned=${result.abandonedDeliveries} | timeoutMs=${timeoutMs}`,
      );
    }
    return result;
  }

  /**
   * A bus that is not running: an idle one only settles what batches run by hand left behind (and
   * stays startable); a stopped one has nothing left to do.
   */
  async #stopOutsideRun(timeoutMs: number): Promise<StopResult> {
    if (this.#state !== 'idle') {
      return { graceful: true, abandonedDeliveries: 0, durationMs: 0 };
    }
    return withinDeadline({ clock: this.#clock, timeoutMs }, (deadline) =>
      this.#dispatcher.stop(deadline),
    );
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
  async drain(maxBatches = DEFAULT_DRAIN_BATCHES): Promise<number> {
    let total = 0;
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const processed = await this.runOnce();
      if (processed === 0) {
        return total;
      }
      total += processed;
    }
    throw new DrainIncompleteError(`drain did not finish within ${maxBatches} batches`);
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
        await transport.subscribe!(subscription.destinations, async (envelope, message) => {
          await (this.#observation === undefined
            ? this.#receive(envelope)
            : this.#observedReceive(envelope, { transport, destination: message?.destination }));
        });
      }
    } catch (error) {
      await withinDeadline(
        { clock: this.#clock, timeoutMs: this.#shutdownTimeoutMs },
        async (deadline) => {
          await this.#closeTransports(deadline);
          return 0;
        },
      );
      this.#state = 'stopped';
      throw error;
    }
    if (this.#state !== 'starting') {
      // A stop gave up waiting for this start and closed the transports and the store already.
      throw new LifecycleError('relaybus was stopped before it finished starting');
    }
    if (this.#dispatcherEnabled) {
      this.#dispatcher.start();
    }
    this.#scheduleRetention();
    this.#scheduleBacklog();
    this.#state = 'running';
    this.#logger.log(
      `[RelayBus](startComponents) relaybus started | handlers=${this.#registry.all().length} dispatcher=${this.#dispatcherEnabled}`,
    );
  }

  /** Applies the retention policy every `retention.interval` while the bus runs, when it deletes anything. */
  #scheduleRetention(): void {
    if (this.#retention.completedMs !== null || this.#retention.deadMs !== null) {
      this.#retentionTimer = schedule(this.#clock, {
        run: () => {
          this.purgeExpired().catch(() => undefined);
        },
        delayMs: this.#retention.intervalMs,
        repeat: true,
        unref: true,
      });
    }
  }

  /** Reports the backlog every `backlogInterval` while the bus runs, when an observer wants it. */
  #scheduleBacklog(): void {
    if (this.#backlogIntervalMs !== undefined) {
      this.#backlogTimer = this.#observation?.startBacklog({
        intervalMs: this.#backlogIntervalMs,
        statistics: () => this.statistics(),
      });
    }
  }

  /**
   * Waits for a start in progress (a failed start leaves nothing to stop), then stops the dispatcher
   * and closes the transports and the store. A start still running at the deadline is given up: the
   * transports and the store are force-closed and the start rejects when it resumes.
   */
  async #stopComponents(timeoutMs: number): Promise<StopResult> {
    try {
      return await withinDeadline({ clock: this.#clock, timeoutMs }, async (deadline) => {
        if (this.#state === 'starting') {
          const started = await deadline.race(this.#starting!.catch(() => undefined));
          if (started === 'timeout') {
            this.#state = 'stopping';
            await this.#closeConnections(deadline);
            return 0;
          }
        }
        if (this.#state !== 'running') {
          return 0;
        }
        this.#state = 'stopping';
        this.#retentionTimer?.();
        this.#backlogTimer?.();
        const abandoned = await this.#dispatcher.stop(deadline);
        await this.#closeConnections(deadline);
        return abandoned;
      });
    } finally {
      this.#state = 'stopped';
    }
  }

  async #closeConnections(deadline: ShutdownDeadline): Promise<void> {
    await this.#closeTransports(deadline);
    const options: CloseOptions = { signal: deadline.signal };
    await deadline.race(Promise.resolve(this.#store.close?.(options)));
  }

  /** Publishes inside the tracer's span, with its trace headers, when the bus is observed. */
  #observedPublish(
    events: readonly DomainEvent[],
    input: { readonly options: PublishOptions<TTransaction>; readonly id: string | undefined },
  ): Promise<EventEnvelope[]> {
    const observation = this.#observation;
    if (observation === undefined) {
      return this.#publish(events, input);
    }
    return observation.publish(
      { eventTypes: events.map((event) => String(event.type)) },
      (headers) =>
        this.#publish(events, { ...input, options: withTraceHeaders(input.options, headers) }),
    );
  }

  async #publish(
    events: readonly DomainEvent[],
    {
      options,
      id,
    }: { readonly options: PublishOptions<TTransaction>; readonly id: string | undefined },
  ): Promise<EventEnvelope[]> {
    if (events.length === 0) {
      return [];
    }
    const now = this.#clock.now();
    const delayMs = options.delay === undefined ? 0 : toMilliseconds(options.delay, 'delay');
    const availableAt = new Date(now.getTime() + delayMs);
    const scope = getCurrentScope();
    const entries = await this.#createEntries({ events, options, id, scope, now, availableAt });
    const deliverable = entries.filter((entry) => entry.deliveries.length > 0);
    if (deliverable.length > 0) {
      await this.#appendDeliverable({ entries: deliverable, options, scope, now, delayMs });
    }
    return entries.map((entry) => entry.event);
  }

  /** One entry per event, in order: its envelope and a delivery for every registration of its type. */
  async #createEntries(input: CreateEntriesInput<TTransaction>): Promise<AppendEntry[]> {
    const { events, options, id, scope, now, availableAt } = input;
    const entries: AppendEntry[] = [];
    let correlationId = options.correlationId ?? scope?.event?.correlationId ?? scope?.event?.id;
    for (const event of events) {
      const envelope = await this.#createEnvelope({
        event,
        options,
        id,
        scope,
        now,
        correlationId,
      });
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
    return entries;
  }

  /** Appends the entries that have deliveries, reports what was new and wakes the dispatcher when it is due now. */
  async #appendDeliverable(input: AppendDeliverableInput<TTransaction>): Promise<void> {
    const { entries, options, scope, now, delayMs } = input;
    const result = await this.#appendInTransaction(entries, { options, scope, now });
    const appended = new Set(result.appendedEventIds);
    const published = entries
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

  #reportUnrouted(envelope: EventEnvelope): void {
    const message = `No handler or route for event type, event not stored | type=${envelope.type} id=${envelope.id}`;
    if (this.#unroutedEvents === 'throw') {
      throw new ConfigurationError(message);
    }
    this.#logger.warn(`[RelayBus](reportUnrouted) ${message}`);
  }

  async #appendInTransaction(
    entries: readonly AppendEntry[],
    {
      options,
      scope,
      now,
    }: {
      readonly options: PublishOptions<TTransaction>;
      readonly scope: ExecutionScope | undefined;
      readonly now: Date;
    },
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
    const eventId = id ?? this.#generateIdentifier();
    return {
      id: eventId,
      type: event.type,
      source: this.#source,
      key: options.key ?? keyFromDefinition(event, payload),
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

  #registrationsOf(eventType: string): EventTypeRegistrations {
    const registrations = this.#registry.forEventType(eventType);
    const routes = registrations.filter((registration) => registration.forwardsToTransport).length;
    return { handlers: registrations.length - routes, routes };
  }

  #registerRoutes(routes: readonly RouteOptions[]): void {
    for (const route of routes) {
      this.#requireTransport(route.transport);
    }
    for (const group of groupRoutes(routes)) {
      this.#registerRoute(group);
    }
  }

  /** Registers one handler for every route to a destination, so a key's events reach it in commit order. */
  #registerRoute(group: RouteGroup): void {
    const transport = this.#transports.get(group.transport)!;
    const { route, destination } = group;
    this.#registry.register(
      {
        eventTypes: group.eventTypes,
        legacyIds: group.legacyIds,
        schema: undefined,
        handle:
          this.#observation?.sender(transport, destination) ??
          ((envelope) => transport.publish(envelope, destination)),
        options: {
          id: group.id,
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
    schedule(this.#clock, {
      run: () => this.#dispatcher.wake(),
      delayMs: WAKE_DELAY_MS,
      unref: true,
    });
  }

  /** Closes every transport, each at most until the deadline; past it they are asked to force. */
  async #closeTransports(deadline: ShutdownDeadline): Promise<void> {
    const options: CloseOptions = { signal: deadline.signal };
    for (const transport of this.#transports.values()) {
      try {
        await deadline.race(transport.close(options));
      } catch (error) {
        this.#logger.error(
          `[RelayBus](closeTransports) Failed to close transport | transport=${transport.name} error=${toErrorInfo(error).message}`,
        );
      }
    }
  }
}

function toDescriptor(registration: HandlerRegistration, eventType: string): HandlerDescriptor {
  return {
    id: registration.id,
    eventType,
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
