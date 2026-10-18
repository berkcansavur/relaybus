import 'reflect-metadata';
import type { EventClass, EventEnvelope, HandlerContext, SubscribeOptions } from '@relaybus/core';

/**
 * Options of {@link Subscribe}: the core {@link SubscribeOptions}, except that
 * `id` is optional and defaults to `ClassName.methodName`.
 */
export type SubscribeDecoratorOptions = Omit<SubscribeOptions, 'id'> & {
  /**
   * Stable id of the handler. Deliveries are recorded under it, so renaming it
   * (or the class or method it is derived from) orphans pending deliveries.
   * Default: `${ClassName}.${methodName}`. Required when one method carries
   * several `@Subscribe` decorators.
   */
  readonly id?: string;
};

/** What one `@Subscribe` decorator recorded on a method. */
export interface SubscribeMetadata {
  readonly event: EventClass<unknown> | string;
  readonly options: SubscribeDecoratorOptions;
}

const SUBSCRIBE_METADATA_KEY = Symbol.for('relaybus.nestjs.subscribe');

type HandlerMethod<TPayload> = (
  event: EventEnvelope<TPayload>,
  context: HandlerContext<never>,
) => unknown;

type SubscribeDecorator<TPayload> = <TMethod extends HandlerMethod<TPayload>>(
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
 *   @Subscribe(OrderPlaced, { transactional: true })
 *   async onOrderPlaced(event: EventEnvelope<{ orderId: string }>, context: HandlerContext) {}
 * }
 * ```
 */
export function Subscribe<TPayload>(
  event: EventClass<TPayload>,
  options?: SubscribeDecoratorOptions,
): SubscribeDecorator<TPayload>;
export function Subscribe(
  eventType: string,
  options?: SubscribeDecoratorOptions,
): SubscribeDecorator<never>;
export function Subscribe(
  event: EventClass<unknown> | string,
  options: SubscribeDecoratorOptions = {},
): (target: object, propertyKey: string | symbol, descriptor: PropertyDescriptor) => void {
  return (target, propertyKey) => {
    // Keyed by (prototype, property) rather than stored on the method function, so a
    // decorator that later replaces the method does not drop the subscription.
    const existing =
      (Reflect.getOwnMetadata(SUBSCRIBE_METADATA_KEY, target, propertyKey) as
        SubscribeMetadata[] | undefined) ?? [];
    Reflect.defineMetadata(
      SUBSCRIBE_METADATA_KEY,
      [...existing, { event, options }],
      target,
      propertyKey,
    );
  };
}

/**
 * Reads the `@Subscribe` declarations of a method, in declaration order. The
 * prototype chain is searched, so a subclass inherits the declarations of the
 * nearest ancestor that declares them.
 */
export function readSubscribeMetadata(
  prototype: object,
  propertyKey: string | symbol,
): readonly SubscribeMetadata[] {
  return (
    (Reflect.getMetadata(SUBSCRIBE_METADATA_KEY, prototype, propertyKey) as
      SubscribeMetadata[] | undefined) ?? []
  );
}
