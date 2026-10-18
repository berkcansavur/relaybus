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
 * `OrderPlaced | OrderShipped`.
 */
export type CatalogEvents<TCatalog extends Catalog> = InstanceType<
  TCatalog[CatalogEventName<TCatalog>]
>;

/**
 * Envelope a handler receives for an event class:
 * `EventOf<typeof OrderPlaced>` is `EventEnvelope<{ orderId: string }>`.
 */
export type EventOf<TEvent> = EventEnvelope<PayloadOf<TEvent>>;

/**
 * Handler methods for the chosen events of a catalog: one `on${EventName}`
 * method per name in `TName`, taking that event's envelope. Use it for
 * consumers that deliberately handle a subset of a catalog.
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
 * Handler methods for every event of a catalog. A class that
 * `implements HandlersOf<typeof OrderEvents>` fails to compile until it has an
 * `on${EventName}` method with the right payload type for each event.
 */
export type HandlersOf<TCatalog extends Catalog, TTransaction = unknown> = PartialHandlersOf<
  TCatalog,
  keyof TCatalog,
  TTransaction
>;

/** Options of {@link defineCatalog}. */
export interface DefineCatalogOptions {
  /**
   * Name of the catalog, used in boot-check messages and as the default prefix
   * of the handler ids that `subscribeCatalog` registers.
   */
  readonly name?: string;
}

/** Options of `RelayBus.subscribeCatalog`: {@link SubscribeOptions} for every handler of the catalog. */
export type SubscribeCatalogOptions = Omit<SubscribeOptions, 'id'> & {
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

const EVENT_NAME_PATTERN = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

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
 * an event class, a name that cannot be part of a method name, or two entries
 * with the same event type.
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
        `${label} entry '${name}' must be a valid identifier because it names the handler method on${name}`,
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
    return { name, eventClass, type: eventClass.type, handlerName: `on${name}` };
  });
  const catalog = Object.freeze({ ...events });
  descriptions.set(catalog, Object.freeze({ name: options.name, entries: Object.freeze(entries) }));
  return catalog;
}

/**
 * Describes a catalog created by {@link defineCatalog}. Throws
 * {@link ConfigurationError} for any other object, so a hand-written record
 * cannot slip past the checks that rely on it.
 */
export function describeCatalog(catalog: Catalog): CatalogDescription {
  const description = descriptions.get(catalog);
  if (description === undefined) {
    throw new ConfigurationError('Expected a catalog created with defineCatalog');
  }
  return description;
}

/**
 * Throws {@link ConfigurationError} listing every catalog event for which
 * `hasRegistration` finds no handler or route, with the handler method each
 * one expects.
 */
export function assertCatalogsCovered(
  catalogs: Iterable<Catalog>,
  hasRegistration: (eventType: string) => boolean,
): void {
  const missing: string[] = [];
  for (const catalog of catalogs) {
    const { name, entries } = describeCatalog(catalog);
    for (const entry of entries) {
      if (!hasRegistration(entry.type)) {
        missing.push(
          `${name === undefined ? '' : `${name}.`}${entry.name} (${entry.type}): expected handler method ${entry.handlerName}`,
        );
      }
    }
  }
  if (missing.length > 0) {
    throw new ConfigurationError(
      `Catalog events without a handler or route:\n${missing.map((line) => `  - ${line}`).join('\n')}`,
    );
  }
}

/**
 * Throws {@link ConfigurationError} when `event` is not an instance of a class
 * in `catalog` (matched by event type, so duplicate package copies still work).
 */
export function assertInCatalog(catalog: Catalog, event: DomainEvent): void {
  const { name, entries } = describeCatalog(catalog);
  if (!entries.some((entry) => entry.type === event.type)) {
    throw new ConfigurationError(
      `Event type '${String(event.type)}' is not part of ${name === undefined ? 'the catalog' : `catalog '${name}'`}`,
    );
  }
}

/**
 * Maps the `on${EventName}` methods of `handlers` to registrations with the
 * stable ids `${prefix}.on${EventName}`. Throws {@link ConfigurationError}
 * listing every missing method, or when no prefix can be derived.
 */
export function resolveCatalogHandlers(
  catalog: Catalog,
  handlers: object,
  options: SubscribeCatalogOptions,
): CatalogHandlerBinding[] {
  const { name, entries } = describeCatalog(catalog);
  const { idPrefix, ...subscribeOptions } = options;
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
 * Checks an object passed to {@link defineHandlers} against its catalog: every
 * `on${EventName}` method present, nothing else, all functions.
 */
function assertExactHandlers(catalog: Catalog, handlers: object): void {
  const { name, entries } = describeCatalog(catalog);
  const label = name === undefined ? 'the catalog' : `catalog '${name}'`;
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
    throw new ConfigurationError(`Handlers do not match ${label}: ${problems.join(', ')}`);
  }
}

/**
 * Declares the handlers of a whole catalog as an object. Compilation fails
 * when a handler is missing, misnamed, extra or takes the wrong payload; the
 * same checks run at runtime for callers without type checking.
 *
 * ```ts
 * const handlers = defineHandlers(OrderEvents, {
 *   onOrderPlaced: (event) => reserveStock(event.payload.orderId),
 *   onOrderShipped: (event) => notify(event.payload.orderId),
 * });
 * bus.subscribeCatalog(OrderEvents, handlers, { idPrefix: 'fulfilment' });
 * ```
 */
export function defineHandlers<TCatalog extends Catalog, THandlers extends HandlersOf<TCatalog>>(
  catalog: TCatalog,
  handlers: THandlers & NoExtraHandlers<TCatalog, THandlers>,
): THandlers {
  assertExactHandlers(catalog, handlers);
  return handlers;
}

/** Maps every key of `THandlers` that the catalog does not expect to `never`. */
export type NoExtraHandlers<TCatalog extends Catalog, THandlers> = {
  readonly [TKey in Exclude<keyof THandlers, keyof HandlersOf<TCatalog>>]: never;
};
