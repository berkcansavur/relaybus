import { describe, expect, it } from 'vitest';
import { ConfigurationError, defineEvent, type EventEnvelope } from '@relaybus/core';
import { InMemoryTransport, createEnvelope } from '@relaybus/testing';
import { OrderPlaced, OrderShipped, createTestBus } from './helpers.js';

class OrderCancelled extends defineEvent<{ orderId: string }>('order.cancelled.v1') {}

const RETRY = { initialDelay: 100, jitter: 'none' } as const;

function createOrderRoutes() {
  const transport = new InMemoryTransport('broker');
  const test = createTestBus({
    transactionMode: 'optional',
    transports: [transport],
    routes: [
      { event: OrderPlaced, transport: 'broker', destination: 'orders', retry: RETRY },
      { event: OrderCancelled, transport: 'broker', destination: 'orders', retry: RETRY },
      { event: OrderShipped, transport: 'broker', destination: 'orders', retry: RETRY },
    ],
  });
  return { ...test, transport };
}

function sent(transport: InMemoryTransport): string[] {
  return transport.published.map(({ envelope }) => `${envelope.type}:${envelope.key ?? '-'}`);
}

describe('routes sharing a destination', () => {
  it('sends the events of one key in publish order when the first send fails', async () => {
    const { bus, clock, transport } = createOrderRoutes();
    transport.failNextPublishes(1);
    await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' });
    await bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' });

    await bus.drain();
    expect(sent(transport)).toEqual([]);
    clock.advance(100);
    await bus.drain();

    expect(sent(transport)).toEqual(['order.placed.v1:o-1', 'order.cancelled.v1:o-1']);
  });

  it('keeps other keys and unkeyed events moving while one key retries', async () => {
    const { bus, transport } = createOrderRoutes();
    transport.failNextPublishes(1);
    await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' });
    await bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' });
    await bus.drain();
    await bus.publish(new OrderPlaced({ orderId: '2' }), { key: 'o-2' });
    await bus.publish(new OrderShipped({ orderId: '3' }));

    await bus.drain();

    expect(sent(transport).sort()).toEqual(['order.placed.v1:o-2', 'order.shipped.v1:-']);
  });

  it('registers one route per destination, listed once per event type', () => {
    const { bus } = createOrderRoutes();
    expect(bus.handlers().map(({ id, eventType }) => `${id} ${eventType}`)).toEqual([
      'relaybus.transport:broker:orders order.placed.v1',
      'relaybus.transport:broker:orders order.cancelled.v1',
      'relaybus.transport:broker:orders order.shipped.v1',
    ]);
  });

  it('rejects routes to one destination whose retry or timeout differ', () => {
    const transports = [new InMemoryTransport('broker')];
    expect(() =>
      createTestBus({
        transports,
        routes: [
          { event: OrderPlaced, transport: 'broker', destination: 'orders' },
          { event: OrderCancelled, transport: 'broker', destination: 'orders', retry: RETRY },
        ],
      }),
    ).toThrow(ConfigurationError);
    expect(() =>
      createTestBus({
        transports,
        routes: [
          { event: OrderPlaced, transport: 'broker', destination: 'orders', timeout: '30s' },
          { event: OrderCancelled, transport: 'broker', destination: 'orders', timeout: '5s' },
        ],
      }),
    ).toThrow(/Routes to 'orders' of transport 'broker' must share retry and timeout/);
  });

  it('accepts equal options however they are written, and rejects a repeated route', () => {
    const transports = [new InMemoryTransport('broker')];
    expect(() =>
      createTestBus({
        transports,
        routes: [
          { event: OrderPlaced, transport: 'broker', destination: 'orders', timeout: '30s' },
          { event: OrderCancelled, transport: 'broker', destination: 'orders' },
        ],
      }),
    ).not.toThrow();
    expect(() =>
      createTestBus({
        transports,
        routes: [
          { event: OrderPlaced, transport: 'broker', destination: 'orders' },
          { event: 'order.placed.v1', transport: 'broker', destination: 'orders' },
        ],
      }),
    ).toThrow(/order.placed.v1 is routed to 'orders' of transport 'broker' twice/);
  });

  it('rejects two destinations whose route ids would collide', () => {
    expect(() =>
      createTestBus({
        transports: [new InMemoryTransport('broker'), new InMemoryTransport('broker:region')],
        routes: [
          { event: OrderPlaced, transport: 'broker', destination: 'region:orders' },
          { event: OrderCancelled, transport: 'broker:region', destination: 'orders' },
        ],
      }),
    ).toThrow(/would share the handler id 'relaybus.transport:broker:region:orders'/);
  });

  it('still sends deliveries stored under the 0.2.0 per-type route id', async () => {
    const { bus, store, clock, transport } = createOrderRoutes();
    const envelope: EventEnvelope = createEnvelope({ id: 'legacy', type: 'order.cancelled.v1' });
    await store.transaction((transaction) =>
      store.append(
        transaction,
        [
          {
            event: envelope,
            deliveries: [
              {
                id: 'legacy-delivery',
                handler: 'relaybus.transport:broker:orders:order.cancelled.v1',
                availableAt: clock.now(),
              },
            ],
          },
        ],
        clock.now(),
      ),
    );

    await bus.drain();

    expect(transport.published).toEqual([{ destination: 'orders', envelope }]);
    expect(await bus.listDeliveries()).toEqual([
      expect.objectContaining({
        status: 'completed',
        handler: 'relaybus.transport:broker:orders:order.cancelled.v1',
      }),
    ]);
  });
});
