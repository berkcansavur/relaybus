import { BusObservation } from '../bus-observation.js';
import { toCatalogRequirement, type CatalogRequirement } from '../catalog-coverage.js';
import { systemClock, type Clock } from '../clock.js';
import { DispatchObservation } from '../dispatch-observation.js';
import { Dispatcher } from '../dispatcher.js';
import type { EventEnvelope } from '../event.js';
import type { EventStore, EventStoreStatistics, NewDelivery } from '../event-store.js';
import {
  HandlerRegistry,
  type HandlerRegistration,
  type RegistrationDefaults,
} from '../handler-registry.js';
import { createUuidV7, type IdentifierGenerator } from '../identifier.js';
import { Notifier } from '../notifier.js';
import type { RelayBusLogger } from '../observer.js';
import type { RelayBusOptions, UnroutedEventPolicy } from '../relay-bus-options.js';
import {
  createDispatcherSettings,
  createHandlerDefaults,
  createRetentionSettings,
  isDispatcherEnabled,
  oneOf,
  resolveBacklogInterval,
  resolveTransactionTimeout,
} from '../relay-bus-settings.js';
import { BusConnections } from './bus-connections.js';
import { BusMaintenance } from './bus-maintenance.js';
import { EventPublisher } from './event-publisher.js';
import { EventReceiver } from './event-receiver.js';

/** What the parts of a bus call back into the bus for. */
export type RelayBusHooks = {
  /** Logs or throws for an event no handler or route takes. */
  readonly reportUnrouted: (envelope: EventEnvelope) => void;
  /** Wakes the dispatcher shortly after new deliveries were stored. */
  readonly scheduleWake: (committed: boolean) => void;
  readonly statistics: () => Promise<EventStoreStatistics>;
};

/** The settings and services every part of a bus shares, resolved from its options. */
type BusCore<TTransaction> = {
  readonly store: EventStore<TTransaction>;
  readonly source: string;
  readonly transactionMode: 'required' | 'optional';
  readonly unroutedEvents: UnroutedEventPolicy;
  readonly transactionResolver: (() => TTransaction | undefined) | undefined;
  readonly transactionTimeoutMs: number;
  readonly registry: HandlerRegistry;
  readonly defaults: RegistrationDefaults;
  readonly logger: RelayBusLogger;
  readonly notifier: Notifier;
  readonly clock: Clock;
  readonly observation: BusObservation | undefined;
  readonly generateIdentifier: IdentifierGenerator;
  readonly setupStore: boolean;
};

/** Everything a {@link RelayBus} is made of. */
export type RelayBusParts<TTransaction> = BusCore<TTransaction> & {
  readonly maintenance: BusMaintenance;
  readonly connections: BusConnections;
  readonly catalogRequirements: CatalogRequirement[];
  readonly dispatcherEnabled: boolean;
  readonly shutdownTimeoutMs: number;
  readonly dispatcher: Dispatcher;
  readonly publisher: EventPublisher<TTransaction>;
  readonly receiver: EventReceiver;
};

/**
 * Builds the parts of a bus from its options. Options are read and checked in a fixed order, so
 * the first invalid one is the one reported.
 */
export function assembleRelayBus<TTransaction>(
  options: RelayBusOptions<TTransaction>,
  hooks: RelayBusHooks,
): RelayBusParts<TTransaction> {
  const core = resolveCore(options);
  const backlogIntervalMs = resolveBacklogInterval(options);
  const maintenance = new BusMaintenance({
    ...core,
    retention: createRetentionSettings(options.retention),
    backlogIntervalMs,
    statistics: hooks.statistics,
  });
  const connections = new BusConnections(core, options.subscriptions ?? []);
  connections.register({ transports: options.transports ?? [], routes: options.routes ?? [] });
  const catalogRequirements: CatalogRequirement[] = [];
  for (const catalog of options.catalogs ?? []) {
    catalogRequirements.push(toCatalogRequirement(catalog));
  }
  const dispatcherEnabled = isDispatcherEnabled(options);
  const settings = createDispatcherSettings(options.dispatcher, core.generateIdentifier);
  const dispatcher = new Dispatcher({
    ...core,
    random: options.random ?? Math.random,
    settings,
    observation: DispatchObservation.create({ ...core, tracer: options.tracer }),
  });
  const inboxAndOutbox = createInboxAndOutbox(core, hooks);
  connections.commitWireTypes();
  return {
    ...core,
    maintenance,
    connections,
    catalogRequirements,
    dispatcherEnabled,
    shutdownTimeoutMs: settings.shutdownTimeoutMs,
    dispatcher,
    ...inboxAndOutbox,
  };
}

function resolveCore<TTransaction>(options: RelayBusOptions<TTransaction>): BusCore<TTransaction> {
  const source = options.source ?? 'relaybus';
  const transactionMode = oneOf(options.transactionMode ?? 'required', {
    allowed: ['required', 'optional'],
    name: 'transactionMode',
  });
  const unroutedEvents = oneOf(options.unroutedEvents ?? 'warn', {
    allowed: ['warn', 'throw'],
    name: 'unroutedEvents',
  });
  const transactionTimeoutMs = resolveTransactionTimeout(options);
  const defaults = createHandlerDefaults(options);
  const logger = options.logger ?? console;
  const notifier = new Notifier(options.observers ?? [], logger);
  const clock = options.clock ?? systemClock;
  return {
    store: options.store,
    source,
    transactionMode,
    unroutedEvents,
    transactionResolver: options.transactionResolver,
    transactionTimeoutMs,
    registry: new HandlerRegistry(),
    defaults,
    logger,
    notifier,
    clock,
    observation: BusObservation.create({ notifier, tracer: options.tracer, clock }),
    generateIdentifier: options.generateIdentifier ?? (() => createUuidV7()),
    setupStore: options.setupStore ?? true,
  };
}

/** The publisher (outbox side) and the receiver (inbox side) of a bus. */
function createInboxAndOutbox<TTransaction>(
  core: BusCore<TTransaction>,
  hooks: RelayBusHooks,
): { publisher: EventPublisher<TTransaction>; receiver: EventReceiver } {
  const createDelivery = (registration: HandlerRegistration, availableAt: Date): NewDelivery => ({
    id: core.generateIdentifier(),
    handler: registration.id,
    availableAt,
  });
  return {
    publisher: new EventPublisher<TTransaction>({
      ...core,
      createDelivery,
      reportUnrouted: hooks.reportUnrouted,
      scheduleWake: hooks.scheduleWake,
    }),
    receiver: new EventReceiver({ ...core, createDelivery, scheduleWake: hooks.scheduleWake }),
  };
}
