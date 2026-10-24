/**
 * Compile-time contract of `@RelayBusConsumer`. Enforced by `tsc -p tsconfig.json`: every
 * `@ts-expect-error` line must produce a compile error, otherwise tsc reports an unused directive.
 * The classes are declared inside a function that never runs, so no decorator executes here.
 */
import { defineEvent, type EventEnvelope, type HandlerContext } from '@relaybus/core';
import { describe, it } from 'vitest';
import { defineCatalog } from '@relaybus/core';
import {
  EventConsumer,
  RelayBusCatalogConsumer,
  RelayBusConsumer,
  Subscribe,
} from '../src/index.js';
import { OrderPlaced } from './helpers.js';

class UserRegistered extends defineEvent<{ userId: number }>('user.registered.v1') {}

describe('@RelayBusConsumer types', () => {
  it('checks the event parameter against the event payload', () => {
    const compileOnly = (): void => {
      class Handlers {
        // Free method names, events of any catalog in one class.
        @RelayBusConsumer(OrderPlaced)
        async shipOrder(event: EventEnvelope<{ orderId: string }>): Promise<void> {
          void event.payload.orderId;
        }

        @RelayBusConsumer(UserRegistered, { id: 'welcome-mail' })
        sendWelcome(event: EventEnvelope<{ userId: number }>, context: HandlerContext): void {
          void [event.payload.userId, context.attempt];
        }

        @RelayBusConsumer(OrderPlaced, { id: 'audit' })
        audit(): void {}

        // @ts-expect-error: OrderPlaced carries { orderId: string }, not { userId: number }
        @RelayBusConsumer(OrderPlaced, { id: 'wrong' })
        wrongPayload(event: EventEnvelope<{ userId: number }>): void {
          void event;
        }

        // @ts-expect-error: the first parameter is the envelope, not the payload
        @RelayBusConsumer(OrderPlaced, { id: 'payload' })
        payloadOnly(payload: { orderId: string }): void {
          void payload;
        }

        // @ts-expect-error: the deprecated alias checks the parameter the same way
        @Subscribe(UserRegistered, { id: 'alias' })
        aliasWrongPayload(event: EventEnvelope<{ orderId: string }>): void {
          void event;
        }
      }
      void Handlers;
    };
    void compileOnly;
  });

  it('rejects a decorator in the wrong place or with the wrong argument', () => {
    const OrderEvents = defineCatalog({ OrderPlaced }, { name: 'OrderEvents' });
    const compileOnly = (): void => {
      // @ts-expect-error: @RelayBusConsumer decorates methods, not classes
      @RelayBusConsumer(OrderPlaced)
      class ConsumerOnClass {}

      // @ts-expect-error: the deprecated @Subscribe decorates methods only too
      @Subscribe(OrderPlaced)
      class SubscribeOnClass {}

      class CatalogOnMethod {
        // @ts-expect-error: @RelayBusCatalogConsumer decorates classes, not methods
        @RelayBusCatalogConsumer(OrderEvents)
        onOrderPlaced(): void {}

        // @ts-expect-error: the deprecated @EventConsumer decorates classes only too
        @EventConsumer(OrderEvents)
        onOrderShipped(): void {}

        // @ts-expect-error: @RelayBusConsumer takes an event class or type, not a catalog
        @RelayBusConsumer(OrderEvents)
        onCatalog(): void {}
      }

      // @ts-expect-error: @RelayBusCatalogConsumer takes a catalog, not an event class
      @RelayBusCatalogConsumer(OrderPlaced)
      class EventOnCatalogConsumer {}

      void [ConsumerOnClass, SubscribeOnClass, CatalogOnMethod, EventOnCatalogConsumer];
    };
    void compileOnly;
  });
});
