/**
 * Compile-time contract of `@RelayBusHandler`. Enforced by `tsc -p tsconfig.json`: every
 * `@ts-expect-error` line must produce a compile error, otherwise tsc reports an unused directive.
 * The classes are declared inside a function that never runs, so no decorator executes here.
 */
import { defineEvent, type EventEnvelope, type HandlerContext } from '@relaybus/core';
import { describe, it } from 'vitest';
import { RelayBusHandler, Subscribe } from '../src/index.js';
import { OrderPlaced } from './helpers.js';

class UserRegistered extends defineEvent<{ userId: number }>('user.registered.v1') {}

describe('@RelayBusHandler types', () => {
  it('checks the event parameter against the event payload', () => {
    const compileOnly = (): void => {
      class Handlers {
        // Free method names, events of any catalog in one class.
        @RelayBusHandler(OrderPlaced)
        async shipOrder(event: EventEnvelope<{ orderId: string }>): Promise<void> {
          void event.payload.orderId;
        }

        @RelayBusHandler(UserRegistered, { id: 'welcome-mail' })
        sendWelcome(event: EventEnvelope<{ userId: number }>, context: HandlerContext): void {
          void [event.payload.userId, context.attempt];
        }

        @RelayBusHandler(OrderPlaced, { id: 'audit' })
        audit(): void {}

        // @ts-expect-error: OrderPlaced carries { orderId: string }, not { userId: number }
        @RelayBusHandler(OrderPlaced, { id: 'wrong' })
        wrongPayload(event: EventEnvelope<{ userId: number }>): void {
          void event;
        }

        // @ts-expect-error: the first parameter is the envelope, not the payload
        @RelayBusHandler(OrderPlaced, { id: 'payload' })
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
});
