import type { AmqpMessage } from '../../src/amqp-types.js';
import type { RabbitMqTransportOptions } from '../../src/options.js';
import { RabbitMqTransport } from '../../src/rabbitmq-transport.js';
import { FakeConnection, createLogger } from '../fakes.js';

export interface TransportSetup {
  readonly transport: RabbitMqTransport;
  readonly connections: FakeConnection[];
  readonly logger: ReturnType<typeof createLogger>;
}

export function createTransport(options: Partial<RabbitMqTransportOptions> = {}): TransportSetup {
  const connections: FakeConnection[] = [];
  const logger = createLogger();
  const transport = new RabbitMqTransport({
    connect: async () => {
      const connection = new FakeConnection();
      connections.push(connection);
      return connection;
    },
    consumerGroup: 'billing',
    reconnect: { initialDelay: 5, maxDelay: 5, jitter: 'none' },
    logger,
    random: () => 0.5,
    ...options,
  });
  return { transport, connections, logger };
}

export const returned = (messageId: string, routingKey = 'orders'): AmqpMessage => ({
  content: Buffer.from('{}'),
  fields: { routingKey },
  properties: { messageId },
});
