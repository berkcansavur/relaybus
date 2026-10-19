/**
 * The multi-service scenario of a shared catalog, run against a real broker by
 * the Kafka and RabbitMQ integration tests. Two independent buses play two
 * services with their own stores (PostgreSQL and MongoDB) and their own
 * transports; they share only the catalog module and the broker.
 */
import { consume, produce, type EventEnvelope, type Transport } from '@relaybus/core';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  createOrdersService,
  createShippingService,
  eventually,
  OrderEvents,
  startServiceDatabases,
  type Destinations,
  type OrdersService,
  type ServiceBus,
  type ServiceDatabases,
  type ShippingService,
} from './catalog-services.js';

/** What a broker test provides to the shared scenario. */
export interface BrokerFixture {
  /** Shown in the suite title, for example `Kafka`. */
  readonly name: string;
  start(): Promise<void>;
  stop(): Promise<void>;
  /** Fresh destinations no other test uses, created on the broker when it needs that. */
  createDestinations(): Promise<Destinations>;
  /** A transport of one service, consuming as `consumerGroup` when it subscribes. */
  createTransport(consumerGroup: string): Transport;
  /** Publishes an envelope again, as a broker redelivery or a foreign producer would. */
  replay(envelope: EventEnvelope, destination: string): Promise<void>;
}

/**
 * Handler calls without retried attempts. A transactional handler on MongoDB
 * runs again when its transaction hits a transient error and is retried, so a
 * call can appear twice in a row; only the committed attempt took effect.
 */
function committedCalls(calls: readonly string[]): string[] {
  return calls.filter((call, index) => call !== calls[index - 1]);
}

/** Waits until `handler` has completed `count` deliveries: the handler's work is committed. */
async function completedDeliveries(bus: ServiceBus, handler: string, count: number): Promise<void> {
  await eventually(
    async () => (await bus.listDeliveries({ handler, status: 'completed' })).length === count,
    `${count} completed deliveries of ${handler}`,
  );
}

export function describeSharedCatalogServices(broker: BrokerFixture): void {
  describe(`two services sharing one catalog over ${broker.name}`, () => {
    let databases: ServiceDatabases;
    const running: { stop(): Promise<void> }[] = [];
    const schemas: OrdersService[] = [];

    // One container at a time: starting three at once on a loaded machine made
    // the broker miss its startup log deadline.
    beforeAll(async () => {
      databases = await startServiceDatabases();
      await broker.start();
    });

    afterEach(async () => {
      await Promise.all(running.splice(0).map((bus) => bus.stop()));
      await Promise.all(schemas.splice(0).map((service) => service.drop()));
    });

    afterAll(async () => {
      await Promise.all([databases.stop(), broker.stop()]);
    });

    async function startServices(): Promise<{
      orders: OrdersService;
      shipping: ShippingService;
      destinations: Destinations;
    }> {
      const destinations = await broker.createDestinations();
      const shipping = createShippingService(databases, {
        transport: broker.createTransport(`shipping-${destinations.orders}`),
        destinations,
      });
      const orders = createOrdersService(databases, {
        transport: broker.createTransport(`orders-${destinations.orders}`),
        destinations,
      });
      schemas.push(orders);
      running.push(shipping.bus, orders.bus);
      await shipping.bus.start();
      await orders.bus.start();
      await orders.setupOrdersTable();
      return { orders, shipping, destinations };
    }

    it('carries events both ways while each service consumes its own part of the catalog', async () => {
      const { orders, shipping } = await startServices();
      const placements = [
        ['o-1', 'alice'],
        ['o-2', 'bob'],
        ['o-3', 'alice'],
        ['o-4', 'bob'],
        ['o-5', 'alice'],
      ] as const;

      for (const [orderId, customer] of placements) {
        await orders.placeOrder(orderId, 10, { customer });
      }
      await completedDeliveries(shipping.bus, 'shipping.onOrderPlaced', 5);
      await orders.cancelOrder('o-2');
      for (const carrier of ['ups', 'dhl', 'fedex']) {
        await shipping.shipOrder('o-1', carrier);
      }
      await completedDeliveries(orders.bus, 'orders.onOrderShipped', 3);
      await completedDeliveries(shipping.bus, 'shipping.onOrderCancelled', 1);

      for (const customer of ['alice', 'bob']) {
        const expected = placements
          .filter((placement) => placement[1] === customer)
          .map(([orderId]) => `placed:${orderId}:10`);
        const actual = committedCalls(
          shipping.record.handled.filter((entry) => expected.includes(entry)),
        );
        expect(actual).toEqual(expected);
      }
      expect(orders.record.handled).toEqual([
        'shipped:o-1:ups',
        'shipped:o-1:dhl',
        'shipped:o-1:fedex',
      ]);
      expect(await shipping.shipmentStatuses()).toEqual({
        'o-1': 'pending',
        'o-2': 'cancelled',
        'o-3': 'pending',
        'o-4': 'pending',
        'o-5': 'pending',
      });
      expect(orders.bus.handlers().filter((handler) => !handler.forwardsToTransport)).toEqual([
        expect.objectContaining({ id: 'orders.onOrderShipped', eventType: 'order.shipped.v1' }),
      ]);
      expect(
        shipping.bus
          .handlers()
          .filter((handler) => !handler.forwardsToTransport)
          .map((handler) => handler.id),
      ).toEqual(['shipping.onOrderPlaced', 'shipping.onOrderCancelled']);
    });

    it('publishes nothing when the producing transaction rolls back', async () => {
      const { orders, shipping } = await startServices();

      await expect(orders.placeOrder('rolled-back', 5, { fail: true })).rejects.toThrow(
        'checkout failed after publishing',
      );
      await orders.placeOrder('committed', 7);
      await completedDeliveries(shipping.bus, 'shipping.onOrderPlaced', 1);

      expect(committedCalls(shipping.record.handled)).toEqual(['placed:committed:7']);
      expect(await orders.storedOrders()).toEqual(['committed']);
    });

    it('stores a redelivered event once and runs its handler once', async () => {
      const { orders, shipping, destinations } = await startServices();
      await orders.placeOrder('o-dup', 3);
      await completedDeliveries(shipping.bus, 'shipping.onOrderPlaced', 1);
      await eventually(
        async () =>
          (await orders.bus.listDeliveries({ status: 'completed', eventType: 'order.placed.v1' }))
            .length === 1,
        'the route to the broker to complete',
      );
      const [sent] = await orders.bus.listDeliveries({ status: 'completed' });

      await broker.replay(sent!.event, destinations.orders);
      await eventually(
        () => shipping.record.received.includes('duplicate'),
        'the redelivery to be recognised',
      );

      expect(shipping.record.received).toEqual(['accepted', 'duplicate']);
      expect(committedCalls(shipping.record.handled)).toEqual(['placed:o-dup:3']);
      expect(
        (await shipping.bus.listDeliveries({})).map((delivery) => [
          delivery.handler,
          delivery.status,
        ]),
      ).toEqual([['shipping.onOrderPlaced', 'completed']]);
      expect(await shipping.shipmentStatuses()).toEqual({ 'o-dup': 'pending' });
    });

    it('validates a received payload against the schema of the catalog event', async () => {
      const { orders, shipping, destinations } = await startServices();
      await orders.placeOrder('o-valid', 1);
      await completedDeliveries(shipping.bus, 'shipping.onOrderPlaced', 1);
      await eventually(
        async () =>
          (await orders.bus.listDeliveries({ status: 'completed', eventType: 'order.placed.v1' }))
            .length === 1,
        'the route to the broker to complete',
      );
      const [sent] = await orders.bus.listDeliveries({ status: 'completed' });

      await broker.replay(
        { ...sent!.event, id: `${sent!.event.id}-malformed`, payload: { orderId: '', total: 1 } },
        destinations.orders,
      );
      await eventually(
        async () => (await shipping.bus.listDeliveries({ status: 'dead' })).length === 1,
        'the malformed event to be dead-lettered',
      );

      const [dead] = await shipping.bus.listDeliveries({ status: 'dead' });
      expect(dead).toMatchObject({
        handler: 'shipping.onOrderPlaced',
        lastError: {
          message:
            "Payload of 'order.placed.v1' failed validation: orderId must be a non-empty string and total a number",
        },
      });
      expect(committedCalls(shipping.record.handled)).toEqual(['placed:o-valid:1']);
    });

    it('refuses to boot a consumer without handlers or a producer without routes', async () => {
      const destinations = await broker.createDestinations();
      const consumerWithoutHandlers = createShippingService(databases, {
        transport: broker.createTransport('shipping-unbootable'),
        destinations,
        withoutHandlers: true,
      });
      const producerWithoutRoutes = createOrdersService(databases, {
        transport: broker.createTransport('orders-unbootable'),
        destinations,
        withoutRoutes: true,
        catalogs: [
          produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }),
          consume(OrderEvents, { events: ['OrderShipped'] }),
        ],
      });

      await expect(consumerWithoutHandlers.bus.start()).rejects.toThrow(
        [
          'Catalog events this service does not cover:',
          '  - OrderEvents.OrderPlaced (order.placed.v1): consumed here, needs handler method onOrderPlaced',
          '  - OrderEvents.OrderCancelled (order.cancelled.v1): consumed here, needs handler method onOrderCancelled',
        ].join('\n'),
      );
      await expect(producerWithoutRoutes.bus.start()).rejects.toThrow(
        [
          'Catalog events this service does not cover:',
          '  - OrderEvents.OrderPlaced (order.placed.v1): produced here, needs a route to a transport',
          '  - OrderEvents.OrderCancelled (order.cancelled.v1): produced here, needs a route to a transport',
        ].join('\n'),
      );
      expect(consumerWithoutHandlers.bus.isRunning).toBe(false);
      expect(producerWithoutRoutes.bus.isRunning).toBe(false);
    });
  });
}
