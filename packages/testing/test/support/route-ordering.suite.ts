/**
 * Per-key order of routes that share a destination, run against a real store
 * by the PostgreSQL and MongoDB integration tests. Every route to one
 * destination is one handler, so a retrying event holds back later events of
 * its key whatever their types, while other keys keep moving.
 */
import {
  RelayBus,
  defineEvent,
  type EventEnvelope,
  type EventStore,
  type RelayBusLogger,
} from '@relaybus/core';
import { FakeClock, InMemoryTransport, createEnvelope } from '@relaybus/testing';
import { afterEach, describe, expect, it } from 'vitest';

const silentLogger: RelayBusLogger = { log: () => {}, warn: () => {}, error: () => {} };

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderCancelled extends defineEvent<{ orderId: string }>('order.cancelled.v1') {}

const RETRY = { initialDelay: 100, jitter: 'none' } as const;

/** What a store test provides: a fresh, isolated store, and how to drop it afterwards. */
export interface RouteOrderingStoreFixture {
  /** Shown in the suite title, for example `PostgreSQL`. */
  readonly name: string;
  createStore(): Promise<EventStore>;
  cleanup(): Promise<void>;
}

async function createBus(fixture: RouteOrderingStoreFixture) {
  const transport = new InMemoryTransport('broker');
  const clock = new FakeClock();
  const bus = new RelayBus({
    store: await fixture.createStore(),
    clock,
    transactionMode: 'optional',
    transports: [transport],
    routes: [
      { event: OrderPlaced, transport: 'broker', destination: 'orders', retry: RETRY },
      { event: OrderCancelled, transport: 'broker', destination: 'orders', retry: RETRY },
    ],
    dispatcher: { enabled: false },
    logger: silentLogger,
  });
  await bus.start();
  return { bus, clock, transport };
}

function sent(transport: InMemoryTransport): string[] {
  return transport.published.map(({ envelope }) => `${envelope.type}:${envelope.key ?? '-'}`);
}

export function describeRouteOrdering(fixture: RouteOrderingStoreFixture): void {
  describe(`routes sharing a destination on ${fixture.name}`, () => {
    const buses: RelayBus[] = [];

    afterEach(async () => {
      await Promise.all(buses.splice(0).map((bus) => bus.stop()));
      await fixture.cleanup();
    });

    async function start() {
      const created = await createBus(fixture);
      buses.push(created.bus);
      return created;
    }

    it('sends the cancellation after the placement whose first send failed', async () => {
      const { bus, clock, transport } = await start();
      transport.failNextPublishes(1);
      await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' });
      await bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' });

      await bus.drain();
      expect(sent(transport)).toEqual([]);
      clock.advance(100);
      await bus.drain();

      expect(sent(transport)).toEqual(['order.placed.v1:o-1', 'order.cancelled.v1:o-1']);
    });

    it('lets other keys and unkeyed events pass a retrying key', async () => {
      const { bus, transport } = await start();
      transport.failNextPublishes(1);
      await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'o-1' });
      await bus.publish(new OrderCancelled({ orderId: '1' }), { key: 'o-1' });
      await bus.drain();
      await bus.publish(new OrderPlaced({ orderId: '2' }), { key: 'o-2' });
      await bus.publish(new OrderCancelled({ orderId: '3' }));

      await bus.drain();

      expect(sent(transport).sort()).toEqual(['order.cancelled.v1:-', 'order.placed.v1:o-2']);
    });

    it('still sends a delivery stored under the 0.2.0 per-type route id', async () => {
      const { bus, clock, transport } = await start();
      const envelope: EventEnvelope = createEnvelope({ type: 'order.cancelled.v1', key: 'o-9' });
      await bus.store.transaction((transaction) =>
        bus.store.append(
          transaction,
          [
            {
              event: envelope,
              deliveries: [
                {
                  id: `${envelope.id}-legacy`,
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

      expect(transport.published.map((message) => message.envelope.id)).toEqual([envelope.id]);
      expect(await bus.statistics()).toMatchObject({ pending: 0, dead: 0 });
    });
  });
}
