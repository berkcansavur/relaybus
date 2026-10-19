import type { StartedRabbitMQContainer } from '@testcontainers/rabbitmq';
import { describeSharedCatalogServices } from '../../testing/test/support/catalog-services.suite.js';
import { silentLogger } from '../../testing/test/support/catalog-services.js';
import { RabbitMqTransport } from '../src/index.js';
import { startBroker, unique } from './support/broker.js';

let broker: StartedRabbitMQContainer;
/** Every test gets its own exchange, so tests never see each other's events. */
let exchange = '';

function createTransport(consumerGroup?: string): RabbitMqTransport {
  return new RabbitMqTransport({
    name: 'rabbit',
    url: broker.getAmqpUrl(),
    exchange,
    // Per-key order across the broker needs ordered consumption (see the README, Ordering).
    ordered: true,
    ...(consumerGroup === undefined ? {} : { consumerGroup }),
    reconnect: { initialDelay: '100ms', maxDelay: '500ms' },
    logger: silentLogger,
  });
}

describeSharedCatalogServices({
  name: 'RabbitMQ',
  async start() {
    broker = await startBroker();
  },
  async stop() {
    await broker.stop();
  },
  async createDestinations() {
    exchange = unique('events');
    return { orders: unique('orders'), shipments: unique('shipments') };
  },
  createTransport: (consumerGroup) => createTransport(consumerGroup),
  async replay(envelope, destination) {
    const transport = createTransport();
    try {
      await transport.connect();
      await transport.publish(envelope, destination);
    } finally {
      await transport.close();
    }
  },
});
