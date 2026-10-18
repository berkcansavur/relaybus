import { assertInCatalog, type Catalog, type CatalogEvents } from './catalog.js';
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
 * A view of a bus that publishes only the events of one catalog. Obtain it
 * with `bus.forCatalog(catalog)`. Publishing an event of another class fails
 * to compile; a mistyped event that gets past the compiler (a cast, plain
 * JavaScript, or a class with a structurally identical payload) is rejected
 * with a {@link ConfigurationError} before anything is stored.
 *
 * ```ts
 * const orders = bus.forCatalog(OrderEvents);
 * await orders.publish(new OrderPlaced({ orderId }));
 * await orders.publish(new UserRegistered({ userId })); // compile error
 * ```
 */
export class CatalogPublisher<TCatalog extends Catalog, TTransaction = unknown> {
  readonly #catalog: TCatalog;
  readonly #target: PublishTarget<TTransaction>;

  constructor(catalog: TCatalog, target: PublishTarget<TTransaction>) {
    this.#catalog = catalog;
    this.#target = target;
  }

  /** Same as `bus.publish`, limited to the events of the catalog. */
  async publish<TEvent extends CatalogEvents<TCatalog>>(
    event: TEvent,
    options?: PublishOneOptions<TTransaction>,
  ): Promise<EventEnvelope<TEvent['payload']>> {
    assertInCatalog(this.#catalog, event);
    return this.#target.publish(event, options);
  }

  /** Same as `bus.publishAll`, limited to the events of the catalog. Nothing is stored unless every event belongs to it. */
  async publishAll(
    events: readonly CatalogEvents<TCatalog>[],
    options?: PublishOptions<TTransaction>,
  ): Promise<EventEnvelope[]> {
    for (const event of events) {
      assertInCatalog(this.#catalog, event);
    }
    return this.#target.publishAll(events, options);
  }
}
