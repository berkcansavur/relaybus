import {
  ConfigurationError,
  consume,
  describeCatalog,
  produce,
  type Catalog,
  type CatalogDeclaration,
  type CatalogRole,
} from '@relaybus/core';

/**
 * What one feature module consumes and produces, given to `RelayBusModule.forFeature`. Each item is
 * a whole catalog or a `consume(...)` / `produce(...)` declaration of some of its events.
 */
export interface RelayBusFeatureOptions {
  /** Events the importing module handles: each needs a `@RelayBusConsumer` method (or a catalog consumer) among its providers. */
  readonly consumes?: readonly (Catalog | CatalogDeclaration)[];
  /** Events the importing module publishes for other services: each needs a route to a transport. */
  readonly produces?: readonly (Catalog | CatalogDeclaration)[];
}

/** One event of a feature contract, named for error messages. */
export type ContractEvent = {
  /** `Catalog.EventName`. */
  readonly label: string;
  readonly type: string;
};

/**
 * The contract of the module that imports `RelayBusModule.forFeature(...)`, checked when the
 * application initialises.
 */
export class RelayBusFeatureContract {
  readonly consumes: readonly ContractEvent[];
  readonly produces: readonly ContractEvent[];

  /** @throws ConfigurationError for a `produce(...)` in `consumes`, a `consume(...)` in `produces`, or something that is not a catalog. */
  constructor(options: RelayBusFeatureOptions) {
    this.consumes = contractEvents(options.consumes ?? [], 'consume');
    this.produces = contractEvents(options.produces ?? [], 'produce');
  }
}

function contractEvents(
  items: readonly (Catalog | CatalogDeclaration)[],
  role: CatalogRole,
): ContractEvent[] {
  return items.flatMap((item) => {
    const declaration = isDeclaration(item)
      ? item
      : role === 'consume'
        ? consume(item)
        : produce(item);
    const name = describeCatalog(declaration.catalog).name ?? 'catalog';
    if (declaration.role !== role) {
      throw new ConfigurationError(
        `RelayBusModule.forFeature ${role}s takes catalogs and ${role}(...) declarations, ` +
          `received ${declaration.role}(${name})`,
      );
    }
    return declaration.entries.map((entry) => ({
      label: `${name}.${entry.name}`,
      type: entry.eventClass.type,
    }));
  });
}

function isDeclaration(item: Catalog | CatalogDeclaration): item is CatalogDeclaration {
  return typeof item['role'] === 'string' && Array.isArray(item['entries']);
}
