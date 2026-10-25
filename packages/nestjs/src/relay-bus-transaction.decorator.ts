import 'reflect-metadata';
import { resolveBus } from './relay-bus-registry.js';

/** The compile error a `@RelayBusTransaction()` on a method that does not return a Promise reads. */
export type RelayBusTransactionNeedsAsyncMethod =
  '@RelayBusTransaction() needs a method that returns a Promise: make the method async';

/**
 * Accepts the descriptor of a method that returns a Promise; any other method fails to compile
 * with {@link RelayBusTransactionNeedsAsyncMethod} in the message.
 */
export type AsyncMethodDescriptor<TMethod> = TypedPropertyDescriptor<TMethod> &
  (TMethod extends (...args: never[]) => PromiseLike<unknown>
    ? unknown
    : RelayBusTransactionNeedsAsyncMethod);

/** What `@RelayBusTransaction()` returns: a method decorator for async methods only. */
export type RelayBusTransactionDecorator = <TMethod extends (...args: never[]) => unknown>(
  target: object,
  propertyKey: string | symbol,
  descriptor: AsyncMethodDescriptor<TMethod>,
) => void;

type AsyncMethod = (this: unknown, ...args: unknown[]) => Promise<unknown>;

/**
 * Runs the method in a relaybus transaction: `bus.publish(...)` inside it, and in everything it
 * awaits, joins the transaction without passing `{ transaction }`, and a repository reads it with
 * `bus.currentTransaction()` or {@link RelayBusTransactionHost}. The transaction commits when the
 * method resolves and rolls back when it rejects; it is held to the bus `transactionTimeout`.
 *
 * - The method itself is replaced, so `this.other()` calls of decorated methods are transactional
 *   too, unlike interceptor-based decorators.
 * - A decorated method called while a transaction is active (another decorated method,
 *   `bus.transaction`, a transactional handler, or the `transactionResolver`, for example
 *   `@nestjs-cls/transactional`) joins it instead of opening a second one.
 * - The bus is the one of the application's `RelayBusModule`; the class does not inject it.
 * - Only methods that return a Promise compile.
 *
 * ```ts
 * @Injectable()
 * export class OrderService {
 *   @RelayBusTransaction()
 *   async place(orderId: string): Promise<void> {
 *     await this.orders.insert(orderId);
 *     await this.bus.publish(new OrderPlaced({ orderId }));
 *   }
 * }
 * ```
 */
export function RelayBusTransaction(): RelayBusTransactionDecorator {
  // eslint-disable-next-line relaybus/max-two-params -- TypeScript calls a method decorator with three arguments
  return (target, propertyKey, descriptor) => {
    const original = descriptor.value as unknown as AsyncMethod;
    const label = `${target.constructor.name}.${String(propertyKey)}`;
    const transactional: AsyncMethod = async function (this: unknown, ...args: unknown[]) {
      const bus = resolveBus(this, label);
      if (bus.currentTransaction() !== undefined) {
        return original.apply(this, args);
      }
      return bus.transaction(() => original.apply(this, args));
    };
    Object.defineProperty(transactional, 'name', { value: original.name });
    // Decorators applied before this one (route, guard, interceptor metadata) keep their metadata.
    for (const key of Reflect.getOwnMetadataKeys(original)) {
      Reflect.defineMetadata(key, Reflect.getOwnMetadata(key, original), transactional);
    }
    descriptor.value = transactional as never;
  };
}
