import 'reflect-metadata';
import { describeCatalog, type Catalog, type SubscribeOptions } from '@relaybus/core';

/**
 * Options of {@link EventConsumer}: the core {@link SubscribeOptions} applied to
 * every handler of the class, except `id`, which is always
 * `${ClassName}.on${EventName}`.
 */
export type EventConsumerOptions = Omit<SubscribeOptions, 'id'>;

/** What one `@EventConsumer` decorator recorded on a class. */
export interface EventConsumerMetadata {
  readonly catalog: Catalog;
  readonly options: EventConsumerOptions;
}

const EVENT_CONSUMER_METADATA_KEY = Symbol.for('relaybus.nestjs.event-consumer');

/**
 * Subscribes a provider class to the events of a catalog: every
 * `on${EventName}` method it defines is registered when the module initialises,
 * under the stable id `${ClassName}.on${EventName}`, with `this` bound to the
 * provider instance. The class does not have to handle every event, but adding
 * `implements HandlersOf<typeof Catalog>` makes the compiler enforce it, and
 * `catalogs: [Catalog]` in the module options makes the application fail to
 * boot if no provider handles an event.
 *
 * Works next to `@Subscribe` in the same class (give a method that carries both
 * a distinct `@Subscribe` id) and follows the same rules: the provider must be
 * a singleton. A class that defines none of the catalog's handler methods
 * fails the boot, which catches misspelled names.
 *
 * ```ts
 * @Injectable()
 * @EventConsumer(OrderEvents, { transactional: true })
 * export class ShippingHandlers implements HandlersOf<typeof OrderEvents> {
 *   onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
 *   onOrderShipped(event: EventOf<typeof OrderShipped>) {}
 * }
 * ```
 */
export function EventConsumer(
  catalog: Catalog,
  options: EventConsumerOptions = {},
): ClassDecorator {
  describeCatalog(catalog);
  return (target) => {
    const existing =
      (Reflect.getOwnMetadata(EVENT_CONSUMER_METADATA_KEY, target) as
        EventConsumerMetadata[] | undefined) ?? [];
    Reflect.defineMetadata(
      EVENT_CONSUMER_METADATA_KEY,
      [...existing, { catalog, options }],
      target,
    );
  };
}

/**
 * Reads the `@EventConsumer` declarations of a class, in declaration order. The
 * prototype chain of constructors is searched, so a subclass inherits the
 * declarations of the nearest ancestor that declares them.
 */
export function readEventConsumerMetadata(target: object): readonly EventConsumerMetadata[] {
  return (
    (Reflect.getMetadata(EVENT_CONSUMER_METADATA_KEY, target) as
      EventConsumerMetadata[] | undefined) ?? []
  );
}
