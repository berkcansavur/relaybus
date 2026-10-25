import {
  assertCatalogsCovered,
  toCatalogRequirement,
  type CatalogDeclaration,
  type EventTypeRegistrations,
} from './catalog-coverage.js';
import { BusLifecycle } from './bus/bus-lifecycle.js';
import { describeEvent, toDescriptor } from './bus/handler-descriptions.js';
import { assembleRelayBus, type RelayBusParts } from './bus/relay-bus-assembly.js';
import { CatalogPublisher } from './catalog-publisher.js';
import {
  resolveCatalogHandlers,
  type Catalog,
  type CatalogEventSelection,
  type HandlersOf,
  type SubscribeCatalogOptions,
} from './catalog.js';
import { ConfigurationError, DrainIncompleteError, LifecycleError, toErrorInfo } from './errors.js';
import type {
  CloseOptions,
  DeliveryQuery,
  DeliveryRecord,
  EventStore,
  EventStoreStatistics,
  PurgeResult,
} from './event-store.js';
import type { DomainEvent, EventClass, EventEnvelope } from './event.js';
import { getCurrentScope, runInScope } from './execution-scope.js';
import type { EventHandler, HandlerDescriptor, SubscribeOptions } from './handler-registry.js';
import { schedule } from './internal/timers.js';
import type {
  PublishOneOptions,
  PublishOptions,
  ReceiveStatus,
  RelayBusOptions,
} from './relay-bus-options.js';
import {
  withinDeadline,
  type ShutdownDeadline,
  type StopOptions,
  type StopResult,
} from './shutdown-deadline.js';
import { runBudgetedTransaction } from './transaction-budget.js';
import { Milliseconds } from './values/milliseconds.js';

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

/** Batches `drain` runs at most before it gives up. */
const DEFAULT_DRAIN_BATCHES = 1_000;
const WAKE_DELAY_MS = 25;

/**
 * Reliable event bus: events are stored with business data in one transaction,
 * then dispatched to every subscribed handler and route with independent
 * retries, leases and dead-lettering. A bus instance can be started once.
 */
export class RelayBus<TTransaction = unknown> {
  readonly #parts: RelayBusParts<TTransaction>;
  readonly #lifecycle: BusLifecycle;

  constructor(options: RelayBusOptions<TTransaction>) {
    this.#parts = assembleRelayBus(options, {
      reportUnrouted: (envelope) => this.#reportUnrouted(envelope),
      scheduleWake: () => this.#scheduleWake(),
      statistics: () => this.statistics(),
    });
    this.#lifecycle = new BusLifecycle({
      ...this.#parts,
      closeTransports: (deadline) => this.#closeTransports(deadline),
    });
  }

  get store(): EventStore<TTransaction> {
    return this.#parts.store;
  }

  get isRunning(): boolean {
    return this.#lifecycle.state === 'running';
  }

  /** True while the bus runs and every transport reports itself ready. */
  isReady(): boolean {
    return this.#lifecycle.state === 'running' && this.#parts.connections.allReady();
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
    this.#parts.registry.register(
      {
        eventTypes: [type],
        schema,
        handle: handler as EventHandler,
        options,
        forwardsToTransport: false,
      },
      this.#parts.defaults,
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
    this.#parts.catalogRequirements.push(toCatalogRequirement(catalog));
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
    return this.#parts.registry
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
    const [envelope] = await this.#parts.publisher.publish([event], { options, id: options.id });
    return envelope as EventEnvelope<TPayload>;
  }

  /** Stores several events atomically, in order, sharing one correlation id. */
  async publishAll(
    events: readonly DomainEvent[],
    options: PublishOptions<TTransaction> = {},
  ): Promise<EventEnvelope[]> {
    return this.#parts.publisher.publish(events, { options, id: undefined });
  }

  /**
   * Runs `work` in a new store transaction. `publish` calls inside it join the
   * transaction without passing it explicitly. The transaction is held to `transactionTimeout`
   * and rolled back with a {@link TransactionTimeoutError} when it runs past it.
   */
  async transaction<T>(work: (transaction: TTransaction) => Promise<T>): Promise<T> {
    return runBudgetedTransaction(this.#parts.store, {
      clock: this.#parts.clock,
      timeoutMs: this.#parts.transactionTimeoutMs,
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
      (getCurrentScope()?.transaction as TTransaction | undefined) ??
      this.#parts.transactionResolver?.()
    );
  }

  /**
   * Stores an event received from a broker and schedules it for local handlers.
   * Idempotent on the event id. Transports call it and acknowledge the message
   * only after it resolves. Throws {@link InvalidEnvelopeError} for malformed
   * events, which transports should dead-letter instead of redelivering.
   */
  receive(envelope: EventEnvelope): Promise<ReceiveStatus> {
    return this.#parts.receiver.receive(envelope);
  }

  /**
   * Creates the store schema, connects transports, subscribes and starts
   * dispatching. A bus starts once: after a failed start or a stop it is
   * stopped for good, because its transports are closed; create a new instance.
   */
  async start(): Promise<void> {
    this.#lifecycle.assertStartable();
    // Before any side effect, so a failed check leaves the bus idle and startable.
    assertCatalogsCovered(this.#parts.catalogRequirements, (type) => this.#registrationsOf(type));
    const starting = this.#lifecycle.beginStart(() => this.#startComponents());
    try {
      await starting;
    } finally {
      this.#lifecycle.endStart();
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
        ? this.#parts.shutdownTimeoutMs
        : Milliseconds.forTimer(options.timeout, 'timeout');
    const { stopping, first } = this.#lifecycle.stop(timeoutMs);
    if (!first) {
      return stopping;
    }
    const result = await stopping;
    if (!result.graceful) {
      this.#parts.logger.warn(
        `[RelayBus](stop) Shutdown deadline passed, forced close | abandoned=${result.abandonedDeliveries} | timeoutMs=${timeoutMs}`,
      );
    }
    return result;
  }

  /**
   * Claims and runs one batch of due deliveries, resolving when they settle.
   * Returns how many ran. Intended for tests and scripts.
   */
  async runOnce(): Promise<number> {
    return this.#parts.dispatcher.processBatch();
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
    return this.#parts.store.listDeliveries(query);
  }

  /** Moves dead deliveries back to pending with a fresh attempt budget. */
  async requeue(deliveryIds: readonly string[]): Promise<number> {
    const moved = await this.#parts.store.requeue(deliveryIds, this.#parts.clock.now());
    if (moved > 0) {
      this.#parts.dispatcher.wake();
    }
    return moved;
  }

  statistics(): Promise<EventStoreStatistics> {
    return this.#parts.store.statistics(this.#parts.clock.now());
  }

  /** Applies the retention policy once. Runs automatically while the bus is running. */
  purgeExpired(): Promise<PurgeResult> {
    return this.#parts.maintenance.purgeExpired();
  }

  // The awaits stay in this method, as many as before: with no store setup and no transport a
  // start completes synchronously, so the bus is running as soon as start() returns.
  async #startComponents(): Promise<void> {
    const { setupStore, store, connections } = this.#parts;
    try {
      if (setupStore) {
        await store.setup?.();
      }
      for (const transport of connections.transports) {
        await transport.connect?.();
      }
      for (const subscription of connections.subscriptions) {
        await connections.subscribe(subscription, (envelope, origin) =>
          this.#parts.receiver.receive(envelope, origin),
        );
      }
    } catch (error) {
      await this.#closeAfterFailedStart();
      this.#lifecycle.markStopped();
      throw error;
    }
    if (this.#lifecycle.state !== 'starting') {
      // A stop gave up waiting for this start and closed the transports and the store already.
      throw new LifecycleError('relaybus was stopped before it finished starting');
    }
    if (this.#parts.dispatcherEnabled) {
      this.#parts.dispatcher.start();
    }
    this.#parts.maintenance.start();
    this.#lifecycle.markRunning();
    this.#parts.logger.log(
      `[RelayBus](startComponents) relaybus started | handlers=${this.#parts.registry.all().length} dispatcher=${this.#parts.dispatcherEnabled}`,
    );
  }

  /** Closes the transports a failed start may have connected, within the shutdown timeout. */
  #closeAfterFailedStart(): Promise<StopResult> {
    return withinDeadline(
      { clock: this.#parts.clock, timeoutMs: this.#parts.shutdownTimeoutMs },
      async (deadline) => {
        await this.#closeTransports(deadline);
        return 0;
      },
    );
  }

  #reportUnrouted(envelope: EventEnvelope): void {
    const message = `No handler or route for event type, event not stored | type=${envelope.type} id=${envelope.id}`;
    if (this.#parts.unroutedEvents === 'throw') {
      throw new ConfigurationError(message);
    }
    this.#parts.logger.warn(`[RelayBus](reportUnrouted) ${message}`);
  }

  #registrationsOf(eventType: string): EventTypeRegistrations {
    const registrations = this.#parts.registry.forEventType(eventType);
    const routes = registrations.filter((registration) => registration.forwardsToTransport).length;
    return { handlers: registrations.length - routes, routes };
  }

  #scheduleWake(): void {
    // A store that reports new deliveries wakes the dispatcher itself, right after the commit.
    if (this.#lifecycle.state !== 'running' || this.#parts.dispatcher.watchesNewDeliveries) {
      return;
    }
    // The publishing transaction commits after publish returns; claim shortly after.
    schedule(this.#parts.clock, {
      run: () => this.#parts.dispatcher.wake(),
      delayMs: WAKE_DELAY_MS,
      unref: true,
    });
  }

  /** Closes every transport, each at most until the deadline; past it they are asked to force. */
  async #closeTransports(deadline: ShutdownDeadline): Promise<void> {
    const options: CloseOptions = { signal: deadline.signal };
    for (const transport of this.#parts.connections.transports) {
      try {
        await deadline.race(transport.close(options));
      } catch (error) {
        this.#parts.logger.error(
          `[RelayBus](closeTransports) Failed to close transport | transport=${transport.name} error=${toErrorInfo(error).message}`,
        );
      }
    }
  }
}
