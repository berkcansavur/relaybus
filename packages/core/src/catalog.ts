import { ConfigurationError } from './errors.js';
import { assertEventType, type DomainEvent, type EventEnvelope, type PayloadOf } from './event.js';
import type { EventHandler, HandlerContext, SubscribeOptions } from './handler-registry.js';
import type { StandardSchemaV1 } from './standard-schema.js';

/**
 * An event class as a catalog stores it: any class created with `defineEvent`,
 * whatever its payload type. Use {@link PayloadOf} to recover the payload.
 */
export interface CatalogEventClass {
  new (payload: never): DomainEvent;
  readonly type: string;
  readonly schema: StandardSchemaV1 | undefined;
}

/**
 * The shape of a catalog: event classes by name. Create one with
 * {@link defineCatalog}; the name of each entry becomes part of the handler
 * method that consumes it (`OrderPlaced` is consumed by `onOrderPlaced`).
 */
export interface Catalog {
  readonly [eventName: string]: CatalogEventClass;
}

/** Names of the events of a catalog, as string literals. */
export type CatalogEventName<TCatalog extends Catalog> = Extract<keyof TCatalog, string>;

/**
 * Union of the event instances a catalog can publish, for example
 * `OrderPlaced | OrderShipped`; with `TName`, only those of the named events.
 */
export type CatalogEvents<
  TCatalog extends Catalog,
  TName extends keyof TCatalog = keyof TCatalog,
> = InstanceType<TCatalog[TName & CatalogEventName<TCatalog>]>;

/**
 * Envelope a handler receives for an event class:
 * `EventOf<typeof OrderPlaced>` is `EventEnvelope<{ orderId: string }>`.
 */
export type EventOf<TEvent> = EventEnvelope<PayloadOf<TEvent>>;

/**
 * Same as {@link HandlersOf} with a required list of events:
 * `PartialHandlersOf<typeof OrderEvents, 'OrderPlaced'>` equals
 * `HandlersOf<typeof OrderEvents, 'OrderPlaced'>`.
 *
 * ```ts
 * class AuditHandlers implements PartialHandlersOf<typeof OrderEvents, 'OrderPlaced'> {
 *   onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
 * }
 * ```
 */
export type PartialHandlersOf<
  TCatalog extends Catalog,
  TName extends keyof TCatalog,
  TTransaction = unknown,
> = {
  readonly [TKey in TName & string as `on${TKey}`]: (
    event: EventOf<TCatalog[TKey]>,
    context: HandlerContext<TTransaction>,
  ) => unknown;
};

/**
 * Handler methods for the events of a catalog: one `on${EventName}` method per
 * event, taking that event's envelope. A class that
 * `implements HandlersOf<typeof OrderEvents>` fails to compile until it has a
 * method with the right payload type for each event. `TName` narrows it to the
 * events a service consumes, which is how a service handles part of a catalog
 * shared between services:
 *
 * ```ts
 * class BillingHandlers implements HandlersOf<typeof OrderEvents, 'OrderPlaced'> {
 *   onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
 * }
 * ```
 *
 * `TTransaction` types `context.transaction` for transactional handlers.
 */
export type HandlersOf<
  TCatalog extends Catalog,
  TName extends keyof TCatalog = keyof TCatalog,
  TTransaction = unknown,
> = PartialHandlersOf<TCatalog, TName, TTransaction>;

/**
 * Names of catalog events that an API call is limited to. Every name must be a
 * key of the catalog, which the compiler checks; duplicates and an empty list
 * are rejected at runtime with a {@link ConfigurationError}. Omitting the list
 * means every event of the catalog.
 */
export interface CatalogEventSelection<TName> {
  readonly events?: readonly TName[];
}

/** Options of {@link defineCatalog}. */
export interface DefineCatalogOptions {
  /**
   * Name of the catalog, used in boot-check messages and as the default prefix
   * of the handler ids that `subscribeCatalog` registers.
   */
  readonly name?: string;
}

/**
 * Options of `RelayBus.subscribeCatalog`: {@link SubscribeOptions} for every
 * handler of the catalog, and optionally the events to subscribe to. The plain
 * `SubscribeCatalogOptions`, without a catalog, names no events, so it fits
 * every catalog.
 */
export type SubscribeCatalogOptions<
  TCatalog extends Catalog = Catalog,
  TName extends keyof TCatalog = string extends keyof TCatalog ? never : keyof TCatalog,
> = Omit<SubscribeOptions, 'id'> &
  CatalogEventSelection<TName> & {
    /**
     * Prefix of the handler ids, which become `${idPrefix}.on${EventName}`.
     * Default: the catalog name, then the class name of the handlers object.
     * Deliveries are recorded under these ids, so renaming them orphans pending
     * deliveries.
     */
    readonly idPrefix?: string;
  };

/** One event of a catalog together with the handler method that consumes it. */
export interface CatalogEntry {
  /** Name under which the class was registered, for example `OrderPlaced`. */
  readonly name: string;
  readonly eventClass: CatalogEventClass;
  /** Event type string, for example `order.placed.v1`. */
  readonly type: string;
  /** Expected handler method, for example `onOrderPlaced`. */
  readonly handlerName: string;
}

/** What {@link describeCatalog} knows about a catalog. */
export interface CatalogDescription {
  readonly name: string | undefined;
  readonly entries: readonly CatalogEntry[];
}

/** A handler registration derived from a catalog by {@link resolveCatalogHandlers}. */
export interface CatalogHandlerBinding {
  readonly eventClass: CatalogEventClass;
  readonly handle: EventHandler;
  readonly options: SubscribeOptions;
}

// Letters, digits and underscores: the name becomes part of a method name and of
// handler ids, which do not accept `$`.
const EVENT_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

const descriptions = new WeakMap<object, CatalogDescription>();

/**
 * Groups event classes into a catalog: the complete set of events one service
 * or domain owns. The catalog is frozen and keeps its keys and classes in its
 * type, which enables compile-time checks:
 *
 * ```ts
 * export const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });
 *
 * class ShippingHandlers implements HandlersOf<typeof OrderEvents> {
 *   onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
 *   // Compile error until onOrderShipped is added.
 * }
 * ```
 *
 * Throws {@link ConfigurationError} for an empty catalog, an entry that is not
 * an event class, a name that is not made of letters, digits and underscores,
 * or two entries with the same event type.
 */
export function defineCatalog<const TCatalog extends Catalog>(
  events: TCatalog,
  options: DefineCatalogOptions = {},
): Readonly<TCatalog> {
  const label = options.name === undefined ? 'Catalog' : `Catalog '${options.name}'`;
  const names = Object.keys(events);
  if (names.length === 0) {
    throw new ConfigurationError(`${label} must contain at least one event`);
  }
  const owners = new Map<string, string>();
  const entries = names.map((name): CatalogEntry => {
    const eventClass = events[name] as CatalogEventClass;
    if (!EVENT_NAME_PATTERN.test(name)) {
      throw new ConfigurationError(
        `${label} entry '${name}' must be a valid identifier made of letters, digits and underscores ` +
          `because it names the handler method on${name} and its handler id`,
      );
    }
    if (typeof eventClass !== 'function' || typeof eventClass.type !== 'string') {
      throw new ConfigurationError(
        `${label} entry '${name}' must be an event class created with defineEvent`,
      );
    }
    assertEventType(eventClass.type);
    const owner = owners.get(eventClass.type);
    if (owner !== undefined) {
      throw new ConfigurationError(
        `${label} entries '${owner}' and '${name}' share the event type '${eventClass.type}'`,
      );
    }
    owners.set(eventClass.type, name);
    return Object.freeze({ name, eventClass, type: eventClass.type, handlerName: `on${name}` });
  });
  const catalog = Object.freeze({ ...events });
  descriptions.set(catalog, Object.freeze({ name: options.name, entries: Object.freeze(entries) }));
  return catalog;
}

/**
 * Describes a catalog created by {@link defineCatalog}. The description and its
 * entries are frozen. Throws {@link ConfigurationError} for any other object,
 * so a hand-written record cannot slip past the checks that rely on it.
 */
export function describeCatalog(catalog: Catalog): CatalogDescription {
  const description = descriptions.get(catalog);
  if (description === undefined) {
    throw new ConfigurationError('Expected a catalog created with defineCatalog');
  }
  return description;
}

/** How messages name a catalog: `catalog 'OrderEvents'`, or `the catalog` when it has no name. */
export function catalogLabel(catalog: Catalog): string {
  const { name } = describeCatalog(catalog);
  return name === undefined ? 'the catalog' : `catalog '${name}'`;
}

/**
 * Resolves the event names of a {@link CatalogEventSelection} to their catalog
 * entries, in catalog order; every entry when `events` is undefined. Throws
 * {@link ConfigurationError} for an empty list, a name the catalog does not
 * have, or a name listed twice.
 */
export function selectCatalogEntries(
  catalog: Catalog,
  events: readonly PropertyKey[] | undefined,
): readonly CatalogEntry[] {
  const { entries } = describeCatalog(catalog);
  if (events === undefined) {
    return entries;
  }
  const label = catalogLabel(catalog);
  if (events.length === 0) {
    throw new ConfigurationError(
      `The events selected from ${label} must name at least one event; omit events to select all of them`,
    );
  }
  const selected = new Set<PropertyKey>();
  for (const event of events) {
    if (!entries.some((entry) => entry.name === event)) {
      throw new ConfigurationError(
        `${capitalize(label)} has no event named '${String(event)}'; its events are ${entries
          .map((entry) => entry.name)
          .join(', ')}`,
      );
    }
    if (selected.has(event)) {
      throw new ConfigurationError(`Event '${String(event)}' of ${label} is selected twice`);
    }
    selected.add(event);
  }
  return entries.filter((entry) => selected.has(entry.name));
}

function capitalize(text: string): string {
  return `${text.charAt(0).toUpperCase()}${text.slice(1)}`;
}

/**
 * Returns the entry of `catalog` that `event` belongs to, matched by event type
 * so duplicate package copies still work. Throws {@link ConfigurationError}
 * when the event is not part of the catalog.
 */
export function requireCatalogEntry(catalog: Catalog, event: DomainEvent): CatalogEntry {
  const entry = describeCatalog(catalog).entries.find((candidate) => candidate.type === event.type);
  if (entry === undefined) {
    throw new ConfigurationError(
      `Event type '${String(event.type)}' is not part of ${catalogLabel(catalog)}`,
    );
  }
  return entry;
}

/**
 * Maps the `on${EventName}` methods of `handlers` to registrations with the
 * stable ids `${prefix}.on${EventName}`, for the events selected by
 * `options.events` (default: all). Methods of events that are not selected are
 * ignored. Throws {@link ConfigurationError} listing every missing method, or
 * when no prefix can be derived.
 */
export function resolveCatalogHandlers<TCatalog extends Catalog, TName extends keyof TCatalog>(
  catalog: TCatalog,
  handlers: object,
  options: SubscribeCatalogOptions<TCatalog, TName>,
): CatalogHandlerBinding[] {
  const { name } = describeCatalog(catalog);
  const { idPrefix, events, ...subscribeOptions } = options;
  const entries = selectCatalogEntries(catalog, events);
  const lookup = handlers as Record<string, unknown>;
  const missing = entries.filter((entry) => typeof lookup[entry.handlerName] !== 'function');
  if (missing.length > 0) {
    throw new ConfigurationError(
      `Handlers for catalog ${name === undefined ? 'events' : `'${name}'`} lack methods: ${missing
        .map((entry) => entry.handlerName)
        .join(', ')}`,
    );
  }
  const prefix = idPrefix ?? name ?? classNameOf(handlers);
  if (prefix === undefined) {
    throw new ConfigurationError(
      'Cannot derive handler ids: name the catalog, pass an idPrefix or use a class instance as handlers',
    );
  }
  return entries.map((entry) => {
    const method = lookup[entry.handlerName] as EventHandler;
    return {
      eventClass: entry.eventClass,
      handle: (envelope, context) => method.call(handlers, envelope, context),
      options: { ...subscribeOptions, id: `${prefix}.${entry.handlerName}` },
    };
  });
}

function classNameOf(handlers: object): string | undefined {
  const className = handlers.constructor.name;
  return className === 'Object' || className === '' ? undefined : className;
}

/**
 * Checks an object passed to {@link defineHandlers} against the selected
 * entries of its catalog: every `on${EventName}` method present, nothing else,
 * all functions.
 */
function assertExactHandlers(
  catalog: Catalog,
  entries: readonly CatalogEntry[],
  handlers: object,
): void {
  const expected = new Set(entries.map((entry) => entry.handlerName));
  const present = Object.keys(handlers);
  const missing = [...expected].filter((handlerName) => !present.includes(handlerName));
  const extra = present.filter((key) => !expected.has(key));
  const notFunctions = present.filter(
    (key) => expected.has(key) && typeof (handlers as Record<string, unknown>)[key] !== 'function',
  );
  if (missing.length + extra.length + notFunctions.length > 0) {
    const problems = [
      ...missing.map((key) => `missing ${key}`),
      ...extra.map((key) => `unexpected ${key}`),
      ...notFunctions.map((key) => `${key} is not a function`),
    ];
    throw new ConfigurationError(
      `Handlers do not match ${catalogLabel(catalog)}: ${problems.join(', ')}`,
    );
  }
}

/**
 * The handler signature of one catalog event, written as a method so that its
 * parameters are compared bivariantly: a handler may annotate its context as
 * `HandlerContext<PoolClient>` and still satisfy it. {@link MatchingHandlers}
 * restores the strict check of the event parameter.
 */
type BivariantHandler<TEvent> = {
  handle(event: EventOf<TEvent>, context: HandlerContext): unknown;
}['handle'];

/**
 * The shape {@link defineHandlers} constrains its handlers to: one method per
 * selected event, with the context typed loosely enough for any transaction type.
 */
export type HandlerShapeOf<TCatalog extends Catalog, TName extends keyof TCatalog> = {
  readonly [TKey in TName & string as `on${TKey}`]: BivariantHandler<TCatalog[TKey]>;
};

/**
 * Checks every key of `THandlers` against the selected events of a catalog:
 * keys that are no handler of a selected event become `never`, and a handler
 * whose event parameter does not accept the event's envelope becomes the
 * expected signature, so the compiler reports the mismatch at that key.
 */
export type MatchingHandlers<TCatalog extends Catalog, TName extends keyof TCatalog, THandlers> = {
  readonly [TKey in keyof THandlers]: TKey extends `on${infer TEventName extends TName & string}`
    ? THandlers[TKey] extends (event: infer TEvent, ...rest: never[]) => unknown
      ? EventOf<TCatalog[TEventName]> extends TEvent
        ? THandlers[TKey]
        : (event: EventOf<TCatalog[TEventName]>, context: HandlerContext) => unknown
      : (event: EventOf<TCatalog[TEventName]>, context: HandlerContext) => unknown
    : never;
};

/**
 * Declares the handlers of a catalog as an object: every event, or the events
 * named in `options.events`. Compilation fails when a handler is missing,
 * misnamed, extra (including a handler of an event that is not selected) or
 * takes the wrong payload; the same checks run at runtime for callers without
 * type checking. A handler may type its context with a transaction type, for
 * example `HandlerContext<PoolClient>`.
 *
 * ```ts
 * const handlers = defineHandlers(OrderEvents, {
 *   onOrderPlaced: (event) => reserveStock(event.payload.orderId),
 *   onOrderShipped: (event) => notify(event.payload.orderId),
 * });
 * bus.subscribeCatalog(OrderEvents, handlers, { idPrefix: 'fulfilment' });
 *
 * const billing = defineHandlers(
 *   OrderEvents,
 *   { onOrderPlaced: (event) => charge(event.payload.orderId) },
 *   { events: ['OrderPlaced'] },
 * );
 * bus.subscribeCatalog(OrderEvents, billing, { events: ['OrderPlaced'] });
 * ```
 */
export function defineHandlers<
  TCatalog extends Catalog,
  THandlers extends HandlerShapeOf<TCatalog, TName>,
  const TName extends keyof TCatalog,
>(
  catalog: TCatalog,
  handlers: THandlers & MatchingHandlers<TCatalog, TName, THandlers>,
  options: { readonly events: readonly TName[] },
): THandlers;
/** Without `events`, every handler of the catalog is required. */
export function defineHandlers<
  TCatalog extends Catalog,
  THandlers extends HandlerShapeOf<TCatalog, keyof TCatalog>,
>(
  catalog: TCatalog,
  handlers: THandlers & MatchingHandlers<TCatalog, keyof TCatalog, THandlers>,
  options?: { readonly events?: undefined },
): THandlers;
export function defineHandlers(
  catalog: Catalog,
  handlers: object,
  options: { readonly events?: readonly PropertyKey[] | undefined } = {},
): object {
  assertExactHandlers(catalog, selectCatalogEntries(catalog, options.events), handlers);
  return handlers;
}

/**
 * Maps every key of `THandlers` that is not a handler of the selected events
 * of the catalog (default: all of them) to `never`.
 */
export type NoExtraHandlers<
  TCatalog extends Catalog,
  THandlers,
  TName extends keyof TCatalog = keyof TCatalog,
> = {
  readonly [TKey in Exclude<keyof THandlers, keyof HandlersOf<TCatalog, TName>>]: never;
};
