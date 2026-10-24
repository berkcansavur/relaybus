/**
 * Compile-time contract of the key option of `defineEvent`. Enforced by `tsc -p tsconfig.json`:
 * every `@ts-expect-error` line must produce a compile error, otherwise tsc reports an unused
 * directive. Nothing here publishes.
 */
import { describe, expectTypeOf, it } from 'vitest';
import { defineEvent, type EventClass, type StandardSchemaV1 } from '../src/index.js';

declare const orderSchema: StandardSchemaV1<unknown, { orderId: string; total: number }>;

describe('defineEvent key types', () => {
  it('types the key function from the schema output or the payload type argument', () => {
    const compileOnly = (): void => {
      const FromSchema = defineEvent('order.placed.v1', {
        schema: orderSchema,
        key: (payload) => payload.orderId,
      });
      const FromTypeArgument = defineEvent<{ orderId: string }>('order.shipped.v1', {
        key: (payload) => payload.orderId,
      });
      expectTypeOf(FromSchema).toEqualTypeOf<EventClass<{ orderId: string; total: number }>>();
      expectTypeOf(FromTypeArgument).toEqualTypeOf<EventClass<{ orderId: string }>>();
    };
    void compileOnly;
  });

  it('rejects a key that names a missing field or is not a string', () => {
    const compileOnly = (): void => {
      defineEvent('order.placed.v1', {
        schema: orderSchema,
        // @ts-expect-error: the payload has orderId, not orderID
        key: (payload) => String(payload.orderID),
      });
      defineEvent<{ orderId: string }>('order.shipped.v1', {
        // @ts-expect-error: the payload has no customerId
        key: (payload) => String(payload.customerId),
      });
      // @ts-expect-error: a key is a string, total is a number
      defineEvent('order.placed.v1', {
        schema: orderSchema,
        key: (payload) => payload.total,
      });
    };
    void compileOnly;
  });
});
