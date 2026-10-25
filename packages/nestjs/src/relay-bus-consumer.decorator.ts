import 'reflect-metadata';
import type { EventClass, EventEnvelope, HandlerContext, SubscribeOptions } from '@relaybus/core';

/**
 * Options of {@link RelayBusConsumer}: the core {@link SubscribeOptions}, except that
 * `id` is optional and defaults to `ClassName.methodName`.
 */
export type RelayBusConsumerOptions = Omit<SubscribeOptions, 'id'> & {
  /**
   * Stable id of the handler. Deliveries are recorded under it, so renaming it
   * (or the class or method it is derived from) orphans pending deliveries.
   * Default: `${ClassName}.${methodName}`. Required when one method carries
   * several `@RelayBusConsumer` decorators.
   */
  readonly id?: string;
};

/** What one `@RelayBusConsumer` decorator recorded on a method. */
export interface RelayBusConsumerMetadata {
  readonly event: EventClass<unknown> | string;
  readonly options: RelayBusConsumerOptions;
}

/** @deprecated Renamed to {@link RelayBusConsumerOptions}; this alias is removed in 1.0.0. */
export type SubscribeDecoratorOptions = RelayBusConsumerOptions;

/** @deprecated Renamed to {@link RelayBusConsumerMetadata}; this alias is removed in 1.0.0. */
export type SubscribeMetadata = RelayBusConsumerMetadata;

const SUBSCRIBE_METADATA_KEY = Symbol.for('relaybus.nestjs.subscribe');

type HandlerMethod<TPayload> = (
  event: EventEnvelope<TPayload>,
  context: HandlerContext<never>,
) => unknown;

type HandlerDecorator<TPayload> = <TMethod extends HandlerMethod<TPayload>>(
  target: object,
  propertyKey: string | symbol,
  descriptor: TypedPropertyDescriptor<TMethod>,
) => void;

/**
 * Subscribes a provider method to an event class or type. The method is
 * registered with `bus.subscribe` when the module initialises, with `this`
 * bound to the provider instance, and receives `(envelope, context)`.
 *
 * The provider must be a singleton (default scope) and belong to any module of
 * the application. The decorator name differs from `@nestjs/event-emitter`'s
 * `@OnEvent` so both can be used in one application.
 *
 * ```ts
 * @Injectable()
 * export class ShippingHandlers {
 *   @RelayBusConsumer(OrderPlaced, { transactional: true })
 *   async onOrderPlaced(event: EventEnvelope<{ orderId: string }>, context: HandlerContext) {}
 * }
 * ```
 */
export function RelayBusConsumer<TPayload>(
  event: EventClass<TPayload>,
  options?: RelayBusConsumerOptions,
): HandlerDecorator<TPayload>;
export function RelayBusConsumer(
  eventType: string,
  options?: RelayBusConsumerOptions,
): HandlerDecorator<never>;
export function RelayBusConsumer(
  event: EventClass<unknown> | string,
  options: RelayBusConsumerOptions = {},
): (target: object, propertyKey: string | symbol, descriptor: PropertyDescriptor) => void {
  return (target, propertyKey) => {
    // Keyed by (prototype, property) rather than stored on the method function, so a
    // decorator that later replaces the method does not drop the subscription.
    const existing =
      (Reflect.getOwnMetadata(SUBSCRIBE_METADATA_KEY, target, propertyKey) as
        RelayBusConsumerMetadata[] | undefined) ?? [];
    Reflect.defineMetadata(
      SUBSCRIBE_METADATA_KEY,
      [...existing, { event, options }],
      target,
      propertyKey,
    );
  };
}

/**
 * @deprecated Renamed to {@link RelayBusConsumer}, which behaves identically; this alias is
 * removed in 1.0.0.
 */
export const Subscribe: typeof RelayBusConsumer = RelayBusConsumer;

/**
 * Reads the `@RelayBusConsumer` declarations of a method, in declaration order. The
 * prototype chain is searched, so a subclass inherits the declarations of the
 * nearest ancestor that declares them.
 */
export function readConsumerMetadata(
  prototype: object,
  propertyKey: string | symbol,
): readonly RelayBusConsumerMetadata[] {
  return (
    (Reflect.getMetadata(SUBSCRIBE_METADATA_KEY, prototype, propertyKey) as
      RelayBusConsumerMetadata[] | undefined) ?? []
  );
}
