import {
  describeCatalog,
  selectCatalogEntries,
  type Catalog,
  type CatalogEntry,
  type CatalogEventSelection,
} from './catalog.js';
import { ConfigurationError } from './errors.js';

/**
 * What a service does with the events of a catalog:
 * - `consume`: it handles them, so each needs a local handler.
 * - `produce`: it publishes them for other services, so each needs a route to a transport.
 */
export type CatalogRole = 'consume' | 'produce';

/**
 * The part of a catalog that one service consumes or produces, created with
 * {@link consume} or {@link produce} and passed to the `catalogs` option of the
 * bus (or `bus.registerCatalog`). `start()` then verifies the role for each
 * declared event.
 */
export interface CatalogDeclaration<
  TCatalog extends Catalog = Catalog,
  TName extends keyof TCatalog = keyof TCatalog,
> {
  readonly catalog: TCatalog;
  readonly role: CatalogRole;
  /** Names of the declared events, in catalog order. */
  readonly events: readonly Extract<TName, string>[];
  /** The declared events with their types and handler methods, in catalog order. */
  readonly entries: readonly CatalogEntry[];
}

/** What `start()` requires of a catalog event. */
type CoverageNeed = 'handler' | 'route' | 'handler-or-route';

/** One catalog, or one role of a catalog, that `start()` checks. */
export interface CatalogRequirement {
  readonly catalogName: string | undefined;
  readonly entries: readonly CatalogEntry[];
  readonly need: CoverageNeed;
}

/** How many local handlers and transport routes this process has for an event type. */
export interface EventTypeRegistrations {
  readonly handlers: number;
  readonly routes: number;
}

const declarations = new WeakSet<object>();

/**
 * Declares that this service consumes events of a catalog, typically one
 * shared between services through a contracts package. `start()` fails unless
 * every declared event has a local handler (`subscribeCatalog`, `subscribe`,
 * `@EventConsumer`, `@Subscribe`); a route to a transport does not count.
 * Without `events`, every event of the catalog is declared. Unknown names do
 * not compile and are rejected at runtime with a {@link ConfigurationError}.
 *
 * ```ts
 * new RelayBus({ store, catalogs: [consume(OrderEvents, { events: ['OrderPlaced'] })] });
 * ```
 */
export function consume<
  TCatalog extends Catalog,
  const TName extends keyof TCatalog = keyof TCatalog,
>(
  catalog: TCatalog,
  options: CatalogEventSelection<TName> = {},
): CatalogDeclaration<TCatalog, TName> {
  return declare(catalog, 'consume', options);
}

/**
 * Declares that this service publishes events of a catalog for other services.
 * `start()` fails unless every declared event has a route to a transport, so
 * nothing is published without a destination; a local handler does not count.
 * Combine it with {@link consume} for events the service also handles itself.
 * Without `events`, every event of the catalog is declared.
 *
 * ```ts
 * new RelayBus({
 *   store,
 *   transports: [kafka],
 *   routes: [{ event: OrderPlaced, transport: 'kafka', destination: 'orders' }],
 *   catalogs: [produce(OrderEvents, { events: ['OrderPlaced'] })],
 * });
 * ```
 */
export function produce<
  TCatalog extends Catalog,
  const TName extends keyof TCatalog = keyof TCatalog,
>(
  catalog: TCatalog,
  options: CatalogEventSelection<TName> = {},
): CatalogDeclaration<TCatalog, TName> {
  return declare(catalog, 'produce', options);
}

function declare<TCatalog extends Catalog, TName extends keyof TCatalog>(
  catalog: TCatalog,
  role: CatalogRole,
  options: CatalogEventSelection<TName>,
): CatalogDeclaration<TCatalog, TName> {
  const entries = selectCatalogEntries(catalog, options.events);
  const declaration = Object.freeze({
    catalog,
    role,
    events: Object.freeze(entries.map((entry) => entry.name as Extract<TName, string>)),
    entries,
  });
  declarations.add(declaration);
  return declaration;
}

/**
 * Turns an entry of the `catalogs` option into what `start()` checks. A bare
 * catalog requires a handler or a route for every event; a declaration
 * requires a handler (consume) or a route (produce) for its events. Throws
 * {@link ConfigurationError} for anything that is neither.
 */
export function toCatalogRequirement(value: Catalog | CatalogDeclaration): CatalogRequirement {
  if (declarations.has(value)) {
    const declaration = value as CatalogDeclaration;
    return {
      catalogName: describeCatalog(declaration.catalog).name,
      entries: declaration.entries,
      need: declaration.role === 'consume' ? 'handler' : 'route',
    };
  }
  const { name, entries } = describeCatalog(value as Catalog);
  return { catalogName: name, entries, need: 'handler-or-route' };
}

/**
 * Throws {@link ConfigurationError} listing, per event, what a requirement
 * lacks: the handler method a consumed event needs, the route a produced event
 * needs, or, for a bare catalog, any handler or route.
 */
export function assertCatalogsCovered(
  requirements: Iterable<CatalogRequirement>,
  registrationsOf: (eventType: string) => EventTypeRegistrations,
): void {
  const problems = new Set<string>();
  let onlyBareCatalogs = true;
  for (const { catalogName, entries, need } of requirements) {
    for (const entry of entries) {
      const problem = describeGap(entry, need, registrationsOf(entry.type));
      if (problem !== undefined) {
        problems.add(`${catalogName === undefined ? '' : `${catalogName}.`}${problem}`);
        onlyBareCatalogs &&= need === 'handler-or-route';
      }
    }
  }
  if (problems.size > 0) {
    const heading = onlyBareCatalogs
      ? 'Catalog events without a handler or route:'
      : 'Catalog events this service does not cover:';
    throw new ConfigurationError(
      `${heading}\n${[...problems].map((line) => `  - ${line}`).join('\n')}`,
    );
  }
}

function describeGap(
  entry: CatalogEntry,
  need: CoverageNeed,
  { handlers, routes }: EventTypeRegistrations,
): string | undefined {
  const subject = `${entry.name} (${entry.type})`;
  if (need === 'handler-or-route') {
    return handlers + routes > 0
      ? undefined
      : `${subject}: expected handler method ${entry.handlerName}`;
  }
  if (need === 'handler') {
    if (handlers > 0) {
      return undefined;
    }
    const routeNote = routes > 0 ? '; its route to a transport does not handle it here' : '';
    return `${subject}: consumed here, needs handler method ${entry.handlerName}${routeNote}`;
  }
  if (routes > 0) {
    return undefined;
  }
  const handlerNote = handlers > 0 ? '; a local handler does not deliver it to other services' : '';
  return `${subject}: produced here, needs a route to a transport${handlerNote}`;
}
