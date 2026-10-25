import { describe, expect, it } from 'vitest';
import { InMemoryEventStore, RelayBus, defineEvent, type ErrorInfo } from '@relaybus/core';
import { FakeClock } from '@relaybus/testing';
import { UnroutableMessageError } from '../src/errors.js';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import { FakeConnection, createLogger } from './fakes.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

async function createBus() {
  const connections: FakeConnection[] = [];
  const transport = new RabbitMqTransport({
    connect: async () => {
      const connection = new FakeConnection();
      connections.push(connection);
      return connection;
    },
    logger: createLogger(),
    random: () => 0.5,
  });
  const deadErrors: ErrorInfo[] = [];
  const bus = new RelayBus({
    store: new InMemoryEventStore(),
    clock: new FakeClock(),
    transactionMode: 'optional',
    logger: createLogger(),
    random: () => 0.5,
    transports: [transport],
    routes: [{ event: OrderPlaced, transport: transport.name, retry: { attempts: 1 } }],
    observers: [{ onDeliveryDead: ({ error }) => deadErrors.push(error) }],
  });
  await transport.connect();
  return { bus, transport, publisher: connections[0]!.publisher, deadErrors };
}

describe('outbox dead letters of RabbitMQ publish failures', () => {
  it('records the specific error class, not the RelayBusError base', async () => {
    const { bus, transport, publisher, deadErrors } = await createBus();
    publisher.publishBehaviour = (callback, routingKey) => {
      const { messageId } = publisher.published.at(-1)!.options;
      publisher.emit('return', {
        content: Buffer.from('{}'),
        fields: { routingKey },
        properties: { messageId },
      });
      callback(null);
    };
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    const [record] = await bus.listDeliveries({ status: 'dead' });
    expect(record!.lastError).toMatchObject({
      name: UnroutableMessageError.name,
      code: 'RELAYBUS_RABBITMQ_UNROUTABLE_MESSAGE',
    });
    expect(record!.lastError!.name).not.toBe('RelayBusError');
    expect(deadErrors).toEqual([
      expect.objectContaining({
        name: 'UnroutableMessageError',
        code: 'RELAYBUS_RABBITMQ_UNROUTABLE_MESSAGE',
      }),
    ]);
    await transport.close();
  });
});
