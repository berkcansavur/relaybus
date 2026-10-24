import 'reflect-metadata';
import {
  ConfigurationError,
  consume,
  describeCatalog,
  type Catalog,
  type CatalogEntry,
  type CatalogEventSelection,
  type EventOf,
  type HandlerContext,
  type SubscribeOptions,
} from '@relaybus/core';

/**
 * Options of {@link EventConsumer}: the core {@link SubscribeOptions} applied to
 * every handler of the class, except `id`, which is always
 * `${ClassName}.on${EventName}`, and optionally the events the class consumes.
 */
export type EventConsumerOptions<
  TCatalog extends Catalog = Catalog,
  TName extends keyof TCatalog = keyof TCatalog,
> = Omit<SubscribeOptions, 'id'> & CatalogEventSelection<TName>;

/** What one `@EventConsumer` decorator recorded on a class. */
export interface EventConsumerMetadata {
  readonly catalog: Catalog;
  /** Subscribe options applied to every handler of the class. */
  readonly options: Omit<SubscribeOptions, 'id'>;
  /**
   * Entries named by `options.events`, whose methods are all required;
   * undefined when the class consumes whichever handler methods it defines.
   */
  readonly selectedEntries: readonly CatalogEntry[] | undefined;
}

/**
 * A class whose instances have a handler method for each of the named catalog
 * events. The event parameter is checked strictly; the context is typed
 * `HandlerContext<never>` so a method may declare any transaction type.
 */
export type EventConsumerClass<
  TCatalog extends Catalog,
  TName extends keyof TCatalog,
> = abstract new (...args: never[]) => {
  readonly [TKey in TName & string as `on${TKey}`]: (
    event: EventOf<TCatalog[TKey]>,
    context: HandlerContext<never>,
  ) => unknown;
};

const EVENT_CONSUMER_METADATA_KEY = Symbol.for('relaybus.nestjs.event-consumer');

/**
 * Subscribes a provider class to the events of a catalog: its
 * `on${EventName}` methods are registered when the module initialises, under
 * the stable id `${ClassName}.on${EventName}`, with `this` bound to the
 * provider instance.
 *
 * - Without `events`, every handler method the class defines is registered; a
 *   class that defines none fails the boot, which catches misspelled names.
 *   `implements HandlersOf<typeof Catalog>` makes the compiler require all of them.
 * - With `events`, exactly the methods of those events are registered (methods
 *   of other events are ignored), and a class missing one of them fails to
 *   compile and fails the boot. This is how a service consumes its part of a
 *   catalog shared between services.
 *
 * `catalogs: [consume(Catalog, { events })]` in the module options makes the
 * application fail to boot if no provider handles a consumed event. Works next
 * to `@Subscribe` in the same class (give a method that carries both a
 * distinct `@Subscribe` id). The provider must be a singleton. Declarations
 * are inherited; a subclass that declares the same catalog again replaces the
 * inherited declaration of that catalog, and declaring one catalog twice on
 * the same class throws a {@link ConfigurationError}.
 *
 * ```ts
 * @Injectable()
 * @EventConsumer(OrderEvents, { events: ['OrderPlaced'], transactional: true })
 * export class BillingHandlers {
 *   onOrderPlaced(event: EventOf<typeof OrderPlaced>) {}
 * }
 * ```
 */
export function EventConsumer<TCatalog extends Catalog>(
  catalog: TCatalog,
  options?: Omit<SubscribeOptions, 'id'> & { readonly events?: undefined },
): ClassDecorator;
export function EventConsumer<TCatalog extends Catalog, const TName extends keyof TCatalog>(
  catalog: TCatalog,
  options: Omit<SubscribeOptions, 'id'> & { readonly events: readonly TName[] },
): (target: EventConsumerClass<TCatalog, TName>) => void;
/**
 * With options whose `events` are not statically known (a variable typed
 * {@link EventConsumerOptions}), the selection is checked when the module
 * initialises instead of at compile time.
 */
export function EventConsumer<TCatalog extends Catalog>(
  catalog: TCatalog,
  options: EventConsumerOptions<TCatalog>,
): ClassDecorator;
export function EventConsumer(
  catalog: Catalog,
  {
    events,
    ...options
  }: Omit<SubscribeOptions, 'id'> & {
    readonly events?: readonly PropertyKey[] | undefined;
  } = {},
): ClassDecorator {
  const { name } = describeCatalog(catalog);
  const selectedEntries =
    events === undefined
      ? undefined
      : consume(catalog, { events: events as readonly (keyof Catalog)[] }).entries;
  return (target) => {
    const existing =
      (Reflect.getOwnMetadata(EVENT_CONSUMER_METADATA_KEY, target) as
        EventConsumerMetadata[] | undefined) ?? [];
    if (existing.some((metadata) => metadata.catalog === catalog)) {
      throw new ConfigurationError(
        `@EventConsumer(${name ?? 'catalog'}) is declared twice on ${target.name}; ` +
          `merge the declarations into one`,
      );
    }
    Reflect.defineMetadata(
      EVENT_CONSUMER_METADATA_KEY,
      [...existing, { catalog, options, selectedEntries }],
      target,
    );
  };
}

/**
 * Reads the `@EventConsumer` declarations of a class and of its ancestors,
 * ancestors first, each in declaration order. When a class and an ancestor
 * both declare a catalog, the declaration nearest to the class wins.
 */
export function readEventConsumerMetadata(target: object): readonly EventConsumerMetadata[] {
  const lineage: object[] = [];
  for (let current = target; current !== Function.prototype;) {
    lineage.unshift(current);
    current = Object.getPrototypeOf(current) as object;
  }
  const byCatalog = new Map<Catalog, EventConsumerMetadata>();
  for (const type of lineage) {
    const own =
      (Reflect.getOwnMetadata(EVENT_CONSUMER_METADATA_KEY, type) as
        EventConsumerMetadata[] | undefined) ?? [];
    for (const metadata of own) {
      byCatalog.set(metadata.catalog, metadata);
    }
  }
  return [...byCatalog.values()];
}
