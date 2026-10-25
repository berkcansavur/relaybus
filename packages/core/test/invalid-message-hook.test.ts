import { describe, expect, it, vi } from 'vitest';
import {
  type Transport,
  type TransportMessageHandler,
  type TransportSubscriptionHooks,
} from '@relaybus/core';
import { createTestBus } from './helpers.js';

/** A transport that records what the bus passed to `subscribe`. */
function recordingTransport(overrides: Partial<Transport> = {}): {
  transport: Transport;
  subscribed: () => { onMessage: TransportMessageHandler; hooks?: TransportSubscriptionHooks };
} {
  let captured:
    { onMessage: TransportMessageHandler; hooks?: TransportSubscriptionHooks } | undefined;
  const transport: Transport = {
    name: 'broker',
    publish: async () => undefined,
    subscribe: async (_destinations, onMessage, ...rest) => {
      captured = rest[0] === undefined ? { onMessage } : { onMessage, hooks: rest[0] };
    },
    close: async () => undefined,
    ...overrides,
  };
  return { transport, subscribed: () => captured! };
}

describe('invalid message hook', () => {
  it('hands the transport a way to report undecodable messages and notifies observers', async () => {
    const { transport, subscribed } = recordingTransport({ system: 'kafka' });
    const { bus, observer } = createTestBus({
      transports: [transport],
      subscriptions: [{ transport: 'broker', destinations: ['orders'] }],
    });
    await bus.start();
    subscribed().hooks!.onInvalidMessage({
      destination: 'orders',
      error: new TypeError('value is not JSON'),
    });
    await bus.stop();
    expect(observer.onInvalidMessage).toHaveBeenCalledExactlyOnceWith({
      transport: 'broker',
      system: 'kafka',
      destination: 'orders',
      reason: 'value is not JSON',
      error: expect.objectContaining({ name: 'TypeError', message: 'value is not JSON' }),
    });
  });

  it('names the system after the transport when it declares none', async () => {
    const { transport, subscribed } = recordingTransport();
    const { bus, observer } = createTestBus({
      transports: [transport],
      subscriptions: [{ transport: 'broker', destinations: ['orders'] }],
    });
    await bus.start();
    subscribed().hooks!.onInvalidMessage({ destination: 'orders', error: 'plain text' });
    await bus.stop();
    expect(observer.onInvalidMessage).toHaveBeenCalledWith(
      expect.objectContaining({ system: 'broker', destination: 'orders' }),
    );
  });

  it('passes no hooks when no observer implements the hook', async () => {
    const { transport, subscribed } = recordingTransport();
    const { bus } = createTestBus({
      transports: [transport],
      subscriptions: [{ transport: 'broker', destinations: ['orders'] }],
      observers: [{ onReceiveFinished: vi.fn() }],
    });
    await bus.start();
    await bus.stop();
    expect(subscribed().hooks).toBeUndefined();
  });
});
