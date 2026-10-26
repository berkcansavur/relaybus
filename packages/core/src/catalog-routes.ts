import { isCatalogDeclaration, type CatalogDeclaration } from './catalog-coverage.js';
import { catalogLabel, describeCatalog, wireNamesOf, type Catalog } from './catalog.js';
import { checkWireNames, type WireNames } from './catalog-wire-names.js';
import { ConfigurationError, DuplicateRouteEventError } from './errors.js';
import type { RouteOptions } from './relay-bus-options.js';
import {
  mergeRouteWireTypes,
  type DeclaredWireType,
  type TransportWireTypes,
} from './route-wire-types.js';

/**
 * Options of {@link route}: a {@link RouteOptions} without `event`, because the route sends every
 * event of its catalog declaration, each as a route of its own with these settings.
 */
export interface CatalogRouteOptions<TName extends string = string> extends Omit<
  RouteOptions,
  'event'
> {
  /**
   * Names the wire format uses for these events instead of their event types, by catalog event
   * name. They override the `wireTypes` of the catalog and reach the codec of `transport`, which
   * writes and reads the events under those names, as the `wireTypes` of `jsonEnvelope` would.
   * Needs a codec that maps type names (`jsonEnvelope`) and a transport that accepts them.
   */
  readonly wireTypes?: WireNames<TName>;
}

/** The routes of a catalog declaration, created by {@link route}. */
export interface CatalogRoute {
  readonly declaration: CatalogDeclaration;
  readonly options: CatalogRouteOptions;
  /** The checked `wireTypes` of the route, by catalog event name. */
  readonly wireNames: ReadonlyMap<string, string>;
}

const catalogRoutes = new WeakSet<object>();

/**
 * Routes the events of a `produce(...)` declaration to one transport with shared settings. Each
 * event becomes a route to `destination` (default: its own event type), so several events of a
 * catalog reach one topic or queue without a route apiece, in commit order per key. Pass it in
 * `routes` next to plain routes; a plain route per event keeps working.
 *
 * ```ts
 * routes: [
 *   route(produce(OrderEvents, { events: ['OrderPlaced', 'OrderShipped'] }), {
 *     transport: 'kafka',
 *     destination: 'orders',
 *     wireTypes: { OrderPlaced: 'Order Placed' },
 *   }),
 * ]
 * ```
 *
 * @throws ConfigurationError for anything but a `produce(...)` declaration, and
 * `UnknownCatalogEventError` or `WireTypeCollisionError` for bad `wireTypes`.
 */
export function route<TCatalog extends Catalog, TName extends keyof TCatalog>(
  declaration: CatalogDeclaration<TCatalog, TName>,
  options: CatalogRouteOptions<Extract<TName, string>>,
): CatalogRoute {
  if (!isCatalogDeclaration(declaration) || declaration.role !== 'produce') {
    throw new ConfigurationError('route() takes a produce(catalog, { events }) declaration');
  }
  const wireNames = checkWireNames(options.wireTypes, {
    events: declaration.events,
    label: `The route of ${catalogLabel(declaration.catalog)}`,
  });
  const created = Object.freeze({
    declaration,
    options: Object.freeze({ ...options }),
    wireNames,
  });
  catalogRoutes.add(created);
  return created;
}

/**
 * Event type to wire name of the events of `catalog` that have one (`wireTypes` of
 * `defineCatalog`), for the `wireTypes` of a `jsonEnvelope` that reads them: a service that only
 * consumes a catalog has no route to declare them, and gives its codec these instead.
 */
export function catalogWireTypes(catalog: Catalog): Readonly<Record<string, string>> {
  return Object.fromEntries(
    describeCatalog(catalog).entries.flatMap((entry) => {
      const wire = wireNamesOf(catalog).get(entry.name);
      return wire === undefined ? [] : [[entry.type, wire]];
    }),
  );
}

/** Plain routes and catalog routes, as `RelayBusOptions.routes` accepts them. */
export type RouteDefinition = RouteOptions | CatalogRoute;

/** The plain routes of `definitions`, and the wire names its catalog routes declare per transport. */
export interface ExpandedRoutes {
  readonly routes: readonly RouteOptions[];
  readonly wireTypes: TransportWireTypes;
}

type ExpandedRoute = {
  readonly route: RouteOptions;
  readonly type: string;
  readonly fromCatalog: boolean;
  readonly wireType: string | undefined;
};

function isCatalogRoute(definition: RouteDefinition): definition is CatalogRoute {
  return catalogRoutes.has(definition);
}

/**
 * Turns every catalog route into one plain route per event. Definitions without a catalog route
 * come back as they are. Throws {@link DuplicateRouteEventError} when a catalog route sends an
 * event to a destination another route sends it to, and `WireTypeCollisionError` for clashing
 * wire names.
 */
export function expandRoutes(definitions: readonly RouteDefinition[]): ExpandedRoutes {
  if (!definitions.some(isCatalogRoute)) {
    return { routes: definitions as readonly RouteOptions[], wireTypes: new Map() };
  }
  const expanded = definitions.flatMap((definition) =>
    isCatalogRoute(definition) ? expandCatalogRoute(definition) : [plainRoute(definition)],
  );
  assertNotRoutedTwice(expanded);
  return {
    routes: expanded.map((item) => item.route),
    wireTypes: mergeRouteWireTypes(
      expanded.flatMap(declaredWireType),
      expanded.map((item) => ({ transport: item.route.transport, type: item.type })),
    ),
  };
}

function declaredWireType({
  route: { transport },
  type,
  wireType,
}: ExpandedRoute): DeclaredWireType[] {
  return wireType === undefined ? [] : [{ transport, type, wire: wireType }];
}

function plainRoute(definition: RouteOptions): ExpandedRoute {
  const { event } = definition;
  const type = typeof event === 'string' ? event : event.type;
  return { route: definition, type, fromCatalog: false, wireType: undefined };
}

function expandCatalogRoute({ declaration, options, wireNames }: CatalogRoute): ExpandedRoute[] {
  const catalogNames = wireNamesOf(declaration.catalog);
  const shared = Object.fromEntries(
    Object.entries(options).filter(([name]) => name !== 'wireTypes'),
  ) as Omit<CatalogRouteOptions, 'wireTypes'>;
  return declaration.entries.map((entry) => ({
    route: { ...shared, event: entry.eventClass },
    type: entry.type,
    fromCatalog: true,
    wireType: wireNames.get(entry.name) ?? catalogNames.get(entry.name),
  }));
}

function assertNotRoutedTwice(expanded: readonly ExpandedRoute[]): void {
  const seen = new Map<string, ExpandedRoute>();
  for (const item of expanded) {
    const destination = item.route.destination ?? item.type;
    const key = JSON.stringify([item.route.transport, destination, item.type]);
    const earlier = seen.get(key);
    // Two plain routes keep failing in groupRoutes, as before.
    if (earlier !== undefined && (earlier.fromCatalog || item.fromCatalog)) {
      throw new DuplicateRouteEventError(
        `Event type ${item.type} is routed to '${destination}' of transport '${item.route.transport}' twice`,
      );
    }
    seen.set(key, item);
  }
}
