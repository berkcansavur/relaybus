import {
  requireCatalogEntry,
  catalogLabel,
  selectCatalogEntries,
  type Catalog,
  type CatalogEntry,
  type CatalogEventSelection,
  type CatalogEvents,
} from './catalog.js';
import { ConfigurationError } from './errors.js';
import type { DomainEvent, EventEnvelope } from './event.js';
import type { PublishOneOptions, PublishOptions } from './relay-bus.js';

/** The publishing half of a bus, which a {@link CatalogPublisher} delegates to. */
export interface PublishTarget<TTransaction> {
  publish<TPayload>(
    event: DomainEvent<TPayload>,
    options?: PublishOneOptions<TTransaction>,
  ): Promise<EventEnvelope<TPayload>>;
  publishAll(
    events: readonly DomainEvent[],
    options?: PublishOptions<TTransaction>,
  ): Promise<EventEnvelope[]>;
}

/**
 * A view of a bus that publishes only the events of one catalog, or only the
 * events a service produces from it. Obtain it with `bus.forCatalog(catalog)`
 * or `bus.forCatalog(catalog, { events: ['OrderPlaced'] })`. Publishing an
 * event outside the view fails to compile; a mistyped event that gets past the
 * compiler (a cast, plain JavaScript, or a class with a structurally identical
 * payload) is rejected with a {@link ConfigurationError} before anything is stored.
 *
 * ```ts
 * const orders = bus.forCatalog(OrderEvents);
 * await orders.publish(new OrderPlaced({ orderId }));
 * await orders.publish(new UserRegistered({ userId })); // compile error
 * ```
 */
export class CatalogPublisher<
  TCatalog extends Catalog,
  TTransaction = unknown,
  TName extends keyof TCatalog = keyof TCatalog,
> {
  readonly #catalog: TCatalog;
  readonly #target: PublishTarget<TTransaction>;
  readonly #entries: readonly CatalogEntry[];
  /**
   * Whether the view publishes the event named `eventName`. Its parameter
   * makes a view of some events unassignable to a view of more events, which
   * the bivariant method parameters alone would allow.
   */
  readonly #publishes: (eventName: TName) => boolean;

  /**
   * Throws {@link ConfigurationError} when `catalog` was not created with
   * `defineCatalog` or `options.events` names no event, an unknown event or
   * the same event twice.
   */
  constructor(
    catalog: TCatalog,
    target: PublishTarget<TTransaction>,
    options: CatalogEventSelection<TName> = {},
  ) {
    this.#entries = selectCatalogEntries(catalog, options.events);
    const names = new Set<PropertyKey>(this.#entries.map((entry) => entry.name));
    this.#publishes = (eventName) => names.has(eventName);
    this.#catalog = catalog;
    this.#target = target;
  }

  /** Same as `bus.publish`, limited to the events of the view. */
  async publish<TEvent extends CatalogEvents<TCatalog, TName>>(
    event: TEvent,
    options?: PublishOneOptions<TTransaction>,
  ): Promise<EventEnvelope<TEvent['payload']>> {
    this.#assertPublishable(event);
    return this.#target.publish(event, options);
  }

  /** Same as `bus.publishAll`, limited to the events of the view. Nothing is stored unless every event belongs to it. */
  async publishAll(
    events: readonly CatalogEvents<TCatalog, TName>[],
    options?: PublishOptions<TTransaction>,
  ): Promise<EventEnvelope[]> {
    for (const event of events) {
      this.#assertPublishable(event);
    }
    return this.#target.publishAll(events, options);
  }

  #assertPublishable(event: DomainEvent): void {
    const { name } = requireCatalogEntry(this.#catalog, event);
    if (!this.#publishes(name as TName)) {
      throw new ConfigurationError(
        `Event type '${event.type}' is not one of the events this view of ${catalogLabel(
          this.#catalog,
        )} publishes (${this.#entries.map((entry) => entry.name).join(', ')})`,
      );
    }
  }
}
