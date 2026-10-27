/**
 * Compile-time contract of `@RelayBusTransaction()`. Enforced by `tsc -p tsconfig.json`: every
 * `@ts-expect-error` line must produce a compile error, otherwise tsc reports an unused directive.
 * The classes are declared inside a function that never runs, so no decorator executes here.
 */
import { describe, expectTypeOf, it } from 'vitest';
import {
  RelayBusTransaction,
  type AsyncMethodDescriptor,
  type RelayBusTransactionNeedsAsyncMethod,
} from '../src/index.js';

describe('@RelayBusTransaction types', () => {
  it('compiles on methods that return a Promise only', () => {
    const compileOnly = (): void => {
      class Service {
        @RelayBusTransaction()
        async place(orderId: string): Promise<string> {
          return orderId;
        }

        @RelayBusTransaction()
        settle(): Promise<void> {
          return Promise.resolve();
        }

        @RelayBusTransaction({ isolationLevel: 'serializable', maxAttempts: 5 })
        reserve(): Promise<void> {
          return Promise.resolve();
        }

        // @ts-expect-error: only the levels relaybus knows
        @RelayBusTransaction({ isolationLevel: 'snapshot' })
        snapshot(): Promise<void> {
          return Promise.resolve();
        }

        // @ts-expect-error: @RelayBusTransaction() needs a method that returns a Promise
        @RelayBusTransaction()
        count(): number {
          return 1;
        }

        // @ts-expect-error: a method returning void is not transactional either
        @RelayBusTransaction()
        forget(): void {}

        // @ts-expect-error: a property is not a method
        @RelayBusTransaction()
        readonly name = 'service';
      }
      void Service;
    };
    void compileOnly;
  });

  it('names the reason in the descriptor type a sync method fails against', () => {
    expectTypeOf<AsyncMethodDescriptor<() => number>>().toEqualTypeOf<
      TypedPropertyDescriptor<() => number> & RelayBusTransactionNeedsAsyncMethod
    >();
    expectTypeOf<RelayBusTransactionNeedsAsyncMethod>().toEqualTypeOf<'@RelayBusTransaction() needs a method that returns a Promise: make the method async'>();
  });
});
