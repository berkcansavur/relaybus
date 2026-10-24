import 'reflect-metadata';
import type { EventClass, EventEnvelope, HandlerContext, SubscribeOptions } from '@relaybus/core';

/**
 * Options of {@link RelayBusHandler}: the core {@link SubscribeOptions}, except that
 * `id` is optional and defaults to `ClassName.methodName`.
 */
export type RelayBusHandlerOptions = Omit<SubscribeOptions, 'id'> & {
  /**
   * Stable id of the handler. Deliveries are recorded under it, so renaming it
   * (or the class or method it is derived from) orphans pending deliveries.
   * Default: `${ClassName}.${methodName}`. Required when one method carries
   * several `@RelayBusHandler` decorators.
   */
  readonly id?: string;
};

/** What one `@RelayBusHandler` decorator recorded on a method. */
export interface RelayBusHandlerMetadata {
  readonly event: EventClass<unknown> | string;
  readonly options: RelayBusHandlerOptions;
}

/** @deprecated Renamed to {@link RelayBusHandlerOptions}; this alias is removed in 1.0.0. */
export type SubscribeDecoratorOptions = RelayBusHandlerOptions;

/** @deprecated Renamed to {@link RelayBusHandlerMetadata}; this alias is removed in 1.0.0. */
export type SubscribeMetadata = RelayBusHandlerMetadata;

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
 *   @RelayBusHandler(OrderPlaced, { transactional: true })
 *   async onOrderPlaced(event: EventEnvelope<{ orderId: string }>, context: HandlerContext) {}
 * }
 * ```
 */
export function RelayBusHandler<TPayload>(
  event: EventClass<TPayload>,
  options?: RelayBusHandlerOptions,
): HandlerDecorator<TPayload>;
export function RelayBusHandler(
  eventType: string,
  options?: RelayBusHandlerOptions,
): HandlerDecorator<never>;
export function RelayBusHandler(
  event: EventClass<unknown> | string,
  options: RelayBusHandlerOptions = {},
): (target: object, propertyKey: string | symbol, descriptor: PropertyDescriptor) => void {
  return (target, propertyKey) => {
    // Keyed by (prototype, property) rather than stored on the method function, so a
    // decorator that later replaces the method does not drop the subscription.
    const existing =
      (Reflect.getOwnMetadata(SUBSCRIBE_METADATA_KEY, target, propertyKey) as
        RelayBusHandlerMetadata[] | undefined) ?? [];
    Reflect.defineMetadata(
      SUBSCRIBE_METADATA_KEY,
      [...existing, { event, options }],
      target,
      propertyKey,
    );
  };
}

/**
 * @deprecated Renamed to {@link RelayBusHandler}, which behaves identically; this alias is
 * removed in 1.0.0.
 */
export const Subscribe: typeof RelayBusHandler = RelayBusHandler;

/**
 * Reads the `@RelayBusHandler` declarations of a method, in declaration order. The
 * prototype chain is searched, so a subclass inherits the declarations of the
 * nearest ancestor that declares them.
 */
export function readHandlerMetadata(
  prototype: object,
  propertyKey: string | symbol,
): readonly RelayBusHandlerMetadata[] {
  return (
    (Reflect.getMetadata(SUBSCRIBE_METADATA_KEY, prototype, propertyKey) as
      RelayBusHandlerMetadata[] | undefined) ?? []
  );
}
