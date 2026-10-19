/**
 * Compile-time contract of `@EventConsumer` with selected events. Enforced by
 * `tsc -p tsconfig.json`: every `@ts-expect-error` line must produce a compile
 * error, otherwise tsc reports an unused directive. The classes are declared
 * inside a function that never runs, so no decorator executes here.
 */
import { Injectable } from '@nestjs/common';
import {
  consume,
  defineCatalog,
  defineEvent,
  produce,
  type EventOf,
  type HandlerContext,
  type InMemoryTransaction,
} from '@relaybus/core';
import { describe, expectTypeOf, it } from 'vitest';
import { EventConsumer, RelayBusModule, type EventConsumerOptions } from '../src/index.js';
import { OrderPlaced } from './helpers.js';

class OrderShipped extends defineEvent<{ orderId: string; carrier: string }>('order.shipped.v1') {}

const OrderEvents = defineCatalog({ OrderPlaced, OrderShipped }, { name: 'OrderEvents' });

describe('@EventConsumer types', () => {
  it('requires the handler methods of the selected events', () => {
    const compileOnly = (): void => {
      @Injectable()
      @EventConsumer(OrderEvents, { events: ['OrderPlaced'] })
      class BillingHandlers {
        onOrderPlaced(event: EventOf<typeof OrderPlaced>): void {
          void event.payload.orderId;
        }
      }

      @Injectable()
      // @ts-expect-error: consumer missing handler, onOrderShipped is required by events
      @EventConsumer(OrderEvents, { events: ['OrderPlaced', 'OrderShipped'] })
      class MissingShipped {
        onOrderPlaced(): void {}
      }

      @Injectable()
      // @ts-expect-error: unknown event name, 'OrderRefunded' is not an event of OrderEvents
      @EventConsumer(OrderEvents, { events: ['OrderRefunded'] })
      class UnknownEvent {
        onOrderRefunded(): void {}
      }

      @Injectable()
      @EventConsumer(OrderEvents, { events: ['OrderPlaced'] })
      class TransactionalHandlers {
        onOrderPlaced(
          event: EventOf<typeof OrderPlaced>,
          context: HandlerContext<InMemoryTransaction>,
        ): void {
          void [event, context.transaction];
        }
      }

      @Injectable()
      // @ts-expect-error: onOrderShipped receives the OrderShipped envelope, not a number
      @EventConsumer(OrderEvents, { events: ['OrderShipped'] })
      class WrongPayload {
        onOrderShipped(event: number): void {
          void event;
        }
      }

      @Injectable()
      // @ts-expect-error: a narrower payload is rejected, the OrderShipped envelope has no 'note'
      @EventConsumer(OrderEvents, { events: ['OrderShipped'] })
      class NarrowerPayload {
        onOrderShipped(event: EventOf<typeof OrderShipped> & { payload: { note: string } }): void {
          void event.payload.note;
        }
      }

      const selection = { events: ['OrderPlaced'] as const, transactional: true };
      @Injectable()
      // @ts-expect-error: options from a variable are checked too, onOrderPlaced is missing
      @EventConsumer(OrderEvents, selection)
      class SelectionFromVariable {
        onOrderShipped(): void {}
      }

      // A variable typed EventConsumerOptions is checked when the module initialises.
      const typedOptions: EventConsumerOptions<typeof OrderEvents> = { transactional: true };
      @Injectable()
      @EventConsumer(OrderEvents, typedOptions)
      class TypedOptions {
        onOrderPlaced(): void {}
      }

      // Without events any subset of methods compiles, as before.
      @Injectable()
      @EventConsumer(OrderEvents)
      class AnySubset {
        onOrderShipped(): void {}
      }

      void [
        BillingHandlers,
        MissingShipped,
        UnknownEvent,
        TransactionalHandlers,
        WrongPayload,
        NarrowerPayload,
        SelectionFromVariable,
        TypedOptions,
        AnySubset,
      ];
    };
    expectTypeOf(compileOnly).toBeFunction();
  });

  it('accepts consume and produce declarations as module catalogs', () => {
    const compileOnly = (): void => {
      RelayBusModule.forRoot({
        store: undefined as never,
        catalogs: [
          consume(OrderEvents, { events: ['OrderPlaced'] }),
          produce(OrderEvents, { events: ['OrderShipped'] }),
          OrderEvents,
        ],
      });
    };
    expectTypeOf(compileOnly).toBeFunction();
  });
});
