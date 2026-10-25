import { describe, expect, it } from 'vitest';
import {
  AmqpLimitExceededError,
  RabbitMqPublishRejectedError,
  RabbitMqUnavailableError,
  UnroutableMessageError,
  UnserializablePayloadError,
} from '../src/errors.js';
import type { RabbitMqTransportOptions } from '../src/options.js';
import { createEnvelope, waitFor } from './fakes.js';
import { createTransport, returned } from './support/transport-setup.js';

describe('RabbitMqTransport publish', () => {
  async function connected(options: Partial<RabbitMqTransportOptions> = {}) {
    const context = createTransport(options);
    await context.transport.connect();
    return { ...context, publisher: context.connections[0]!.publisher };
  }

  it('resolves after the broker confirm, publishing persistent and mandatory', async () => {
    const { transport, publisher } = await connected({ exchange: 'events' });
    const envelope = createEnvelope();
    let confirm!: (error: Error | null) => void;
    publisher.publishBehaviour = (callback) => {
      confirm = callback;
    };
    let resolved = false;
    const publishing = transport.publish(envelope, 'order.placed.v1').then(() => {
      resolved = true;
    });
    await waitFor(() => publisher.published.length === 1);
    expect(resolved).toBe(false);
    confirm(null);
    await publishing;
    expect(publisher.published[0]!.routingKey).toBe('order.placed.v1');
    expect(publisher.published[0]!.options).toMatchObject({
      persistent: true,
      mandatory: true,
      messageId: envelope.id,
    });
    await transport.close();
  });

  it('rejects while disconnected and after close, never dropping silently', async () => {
    const { transport } = createTransport();
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      /Not connected to the broker \| transport=rabbitmq destination=orders/,
    );
    await transport.connect();
    await transport.close();
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
  });

  it('rejects when the broker nacks', async () => {
    const { transport, publisher } = await connected();
    publisher.publishBehaviour = (callback) => callback(new Error('message nacked'));
    const error = await transport.publish(createEnvelope(), 'orders').catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RabbitMqPublishRejectedError);
    expect((error as Error).message).toContain('Broker nacked the message');
    await transport.close();
  });

  it('rejects when the channel closes before the confirm', async () => {
    const { transport, publisher } = await connected();
    publisher.publishBehaviour = (callback) => callback(new Error('channel closed'));
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
    await transport.close();
  });

  it('rejects when publishing throws synchronously', async () => {
    const { transport, publisher } = await connected();
    publisher.publishBehaviour = () => {
      throw new Error('Channel closed');
    };
    const error = await transport.publish(createEnvelope(), 'orders').catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RabbitMqUnavailableError);
    expect(((error as Error).cause as Error).message).toBe('Channel closed');
    await transport.close();
  });

  it('rejects, without retrying, an event whose payload cannot be serialized', async () => {
    const { transport, publisher } = await connected();
    await expect(transport.publish(createEnvelope({ payload: 1n }), 'orders')).rejects.toThrow(
      UnserializablePayloadError,
    );
    await expect(
      transport.publish(createEnvelope({ payload: undefined }), 'orders'),
    ).rejects.toThrow(UnserializablePayloadError);
    expect(publisher.published).toHaveLength(0);
    await transport.close();
  });

  it('rejects, without retrying, a destination or id beyond the AMQP short string limit', async () => {
    const { transport, publisher } = await connected();
    await expect(transport.publish(createEnvelope(), 'k'.repeat(256))).rejects.toThrow(
      AmqpLimitExceededError,
    );
    await expect(
      transport.publish(createEnvelope({ id: 'i'.repeat(256) }), 'orders'),
    ).rejects.toThrow(AmqpLimitExceededError);
    expect(publisher.published).toHaveLength(0);
    await transport.close();
  });

  it('rejects when no confirm arrives within the timeout', async () => {
    const { transport, publisher } = await connected({ publishTimeout: 20 });
    publisher.publishBehaviour = () => undefined;
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      /No broker confirm within 20ms/,
    );
    await transport.close();
  });

  it('rejects an unroutable message even though the broker acked it', async () => {
    const { transport, publisher } = await connected();
    const envelope = createEnvelope();
    publisher.publishBehaviour = (callback) => {
      publisher.emit('return', returned(envelope.id));
      callback(null);
    };
    await expect(transport.publish(envelope, 'orders')).rejects.toThrow(UnroutableMessageError);
    await transport.close();
  });

  it('matches returns to publishes by event id and destination', async () => {
    const { transport, publisher } = await connected();
    const envelope = createEnvelope();
    let returnFor: string | undefined;
    publisher.publishBehaviour = (callback, routingKey) => {
      if (routingKey === returnFor) {
        publisher.emit('return', returned(envelope.id, routingKey));
        publisher.emit('return', returned(envelope.id, routingKey));
      }
      callback(null);
    };
    returnFor = 'unbound';
    await expect(transport.publish(envelope, 'unbound')).rejects.toThrow(UnroutableMessageError);
    await expect(transport.publish(envelope, 'unbound')).rejects.toThrow(UnroutableMessageError);
    returnFor = undefined;
    await transport.publish(envelope, 'orders');
    await transport.close();
  });

  it('lets unroutable messages through when allowed, without the mandatory flag', async () => {
    const { transport, publisher } = await connected({ allowUnroutable: true });
    await transport.publish(createEnvelope(), 'orders');
    expect(publisher.published[0]!.options.mandatory).toBe(false);
    await transport.close();
  });

  it('logs publisher channel errors', async () => {
    const { transport, publisher, logger } = await connected();
    publisher.emit('error', new Error('NOT_FOUND'));
    expect(logger.warn).toHaveBeenCalledWith(
      '[RabbitMqTransport](establishChannels) Publisher channel error | transport=rabbitmq error=NOT_FOUND',
    );
    await transport.close();
  });
});
