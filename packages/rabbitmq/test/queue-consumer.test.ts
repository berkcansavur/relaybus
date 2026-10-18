import { beforeEach, describe, expect, it, vi } from 'vitest';
import { InvalidEnvelopeError, createRetryPolicy, type EventEnvelope } from '@relaybus/core';
import { QueueConsumer } from '../src/queue-consumer.js';
import {
  FakeChannel,
  createDelivery,
  createEnvelope,
  createLogger,
  deferred,
  waitFor,
} from './fakes.js';

const RETRY = createRetryPolicy({ attempts: 3, initialDelay: 5, maxDelay: 5, jitter: 'none' });

function setup(
  onMessage: (envelope: EventEnvelope) => Promise<void> = async () => undefined,
  overrides: {
    handlerRetry?: typeof RETRY;
    shutdownTimeoutMs?: number;
    deliveryLimit?: number;
  } = {},
) {
  const logger = createLogger();
  const onConsumerLost = vi.fn();
  const handler = vi.fn(onMessage);
  const consumer = new QueueConsumer({
    queue: 'relaybus.billing',
    prefetch: 7,
    deliveryLimit: overrides.deliveryLimit ?? 10,
    handlerRetry: overrides.handlerRetry ?? RETRY,
    shutdownTimeoutMs: overrides.shutdownTimeoutMs ?? 1_000,
    onMessage: handler,
    onConsumerLost,
    logger,
    random: () => 0.5,
  });
  return { consumer, handler, logger, onConsumerLost, channel: new FakeChannel() };
}

describe('QueueConsumer', () => {
  let message: ReturnType<typeof createDelivery>;
  beforeEach(() => {
    message = createDelivery();
  });

  it('consumes with manual acknowledgements and the configured prefetch', async () => {
    const { consumer, channel } = setup();
    await consumer.attach(channel);
    expect(channel.prefetch).toHaveBeenCalledWith(7);
    expect(channel.consume).toHaveBeenCalledWith('relaybus.billing', expect.any(Function), {
      noAck: false,
    });
  });

  it('acknowledges only after the handler resolved, handing it the decoded envelope', async () => {
    const gate = deferred();
    const { consumer, channel, handler } = setup(() => gate.promise);
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => handler.mock.calls.length === 1);
    expect(channel.ack).not.toHaveBeenCalled();
    expect(handler.mock.calls[0]![0]).toEqual(createEnvelope());
    gate.resolve();
    await waitFor(() => channel.ack.mock.calls.length === 1);
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
  });

  it('retries a failing handler on the same delivery before acknowledging', async () => {
    let calls = 0;
    const { consumer, channel, logger } = setup(async () => {
      calls += 1;
      if (calls < 3) {
        throw new Error('database down');
      }
    });
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => channel.ack.mock.calls.length === 1);
    expect(calls).toBe(3);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        '[QueueConsumer](process) Handler failed, retrying | queue=relaybus.billing',
      ),
    );
  });

  it('requeues the message once the local attempts are spent', async () => {
    const { consumer, channel, handler, logger } = setup(async () => {
      throw new Error('always fails');
    });
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => channel.nack.mock.calls.length === 1);
    expect(handler).toHaveBeenCalledTimes(3);
    expect(channel.nack).toHaveBeenCalledWith(message, false, true);
    expect(channel.ack).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('Handler attempts spent'));
  });

  it.each([
    ['x-acquired-count', { 'x-acquired-count': 2 }],
    ['x-delivery-count', { 'x-delivery-count': 2 }],
    ['both counters', { 'x-delivery-count': 1, 'x-acquired-count': 2 }],
  ])('dead-letters instead of requeueing on the last delivery (%s)', async (_label, headers) => {
    const { consumer, channel, logger } = setup(
      async () => {
        throw new Error('always fails');
      },
      { deliveryLimit: 3 },
    );
    await consumer.attach(channel);
    channel.deliver!({
      ...message,
      properties: { ...message.properties, headers: { ...message.properties.headers, ...headers } },
    });
    await waitFor(() => channel.nack.mock.calls.length === 1);
    expect(channel.nack).toHaveBeenCalledWith(expect.anything(), false, false);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('deliveries=3 deliveryLimit=3'),
    );
  });

  it('dead-letters immediately when the error must not be retried', async () => {
    const { consumer, channel, handler, logger } = setup(async () => {
      throw new InvalidEnvelopeError('invalid occurredAt');
    });
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => channel.nack.mock.calls.length === 1);
    expect(handler).toHaveBeenCalledTimes(1);
    expect(channel.nack).toHaveBeenCalledWith(message, false, false);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Dead-lettering message the handler cannot process'),
    );
  });

  it('dead-letters a malformed message without calling the handler', async () => {
    const { consumer, channel, handler, logger } = setup();
    await consumer.attach(channel);
    channel.deliver!({ ...message, content: Buffer.from('not json') });
    await waitFor(() => channel.nack.mock.calls.length === 1);
    expect(handler).not.toHaveBeenCalled();
    expect(channel.nack.mock.calls[0]![1]).toBe(false);
    expect(channel.nack.mock.calls[0]![2]).toBe(false);
    expect(logger.error).toHaveBeenCalledWith(
      '[QueueConsumer](process) Dead-lettering malformed message | queue=relaybus.billing routingKey=order.placed.v1 error=Message body is not valid JSON',
    );
  });

  it('logs and carries on when the channel can no longer acknowledge', async () => {
    const { consumer, channel, logger } = setup();
    channel.ack.mockImplementation(() => {
      throw new Error('Channel closed');
    });
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => logger.warn.mock.calls.length === 1);
    expect(logger.warn).toHaveBeenCalledWith(
      '[QueueConsumer](settle) Could not ack message, the broker will redeliver it | queue=relaybus.billing error=Channel closed',
    );
  });

  it('reports a consumer cancelled by the broker', async () => {
    const { consumer, channel, onConsumerLost } = setup();
    await consumer.attach(channel);
    channel.deliver!(null);
    expect(onConsumerLost).toHaveBeenCalledWith('consumer cancelled by the broker');
  });

  it('reports its channel closing, unless it detached or stopped first', async () => {
    const { consumer, channel, onConsumerLost, logger } = setup();
    await consumer.attach(channel);
    channel.emit('error', new Error('PRECONDITION_FAILED'));
    expect(logger.warn).toHaveBeenCalledWith(
      '[QueueConsumer](attach) Channel error | queue=relaybus.billing error=PRECONDITION_FAILED',
    );
    channel.emit('close');
    expect(onConsumerLost).toHaveBeenCalledWith('consumer channel closed');

    const second = setup();
    await second.consumer.attach(second.channel);
    second.consumer.detach();
    second.channel.emit('close');
    expect(second.onConsumerLost).not.toHaveBeenCalled();
  });

  it('stops: cancels the consumer, waits for running handlers, closes the channel', async () => {
    const gate = deferred();
    const { consumer, channel } = setup(() => gate.promise);
    await consumer.attach(channel);
    channel.deliver!(message);
    const stopping = consumer.stop();
    await waitFor(() => channel.cancel.mock.calls.length === 1);
    expect(channel.cancel).toHaveBeenCalledWith('consumer-1');
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(channel.close).not.toHaveBeenCalled();
    gate.resolve();
    await stopping;
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.close).toHaveBeenCalledTimes(1);
  });

  it('requeues messages that are waiting between attempts when it stops', async () => {
    const slowRetry = createRetryPolicy({ attempts: 3, initialDelay: 60_000, jitter: 'none' });
    const { consumer, channel, handler } = setup(
      async () => {
        throw new Error('fails');
      },
      { handlerRetry: slowRetry },
    );
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => handler.mock.calls.length === 1);
    await consumer.stop();
    expect(channel.nack).toHaveBeenCalledWith(message, false, true);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('gives up waiting for a hung handler after the shutdown timeout', async () => {
    const { consumer, channel, handler } = setup(() => new Promise(() => undefined), {
      shutdownTimeoutMs: 30,
    });
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => handler.mock.calls.length === 1);
    await consumer.stop();
    expect(channel.close).toHaveBeenCalledTimes(1);
    expect(channel.ack).not.toHaveBeenCalled();
  });

  it('survives a failing cancel and a failing channel close', async () => {
    const { consumer, channel, logger } = setup();
    channel.cancel.mockRejectedValue(new Error('channel is closing'));
    channel.close.mockRejectedValue(new Error('already closed'));
    await consumer.attach(channel);
    await consumer.stop();
    expect(logger.warn).toHaveBeenCalledWith(
      '[QueueConsumer](stop) Could not cancel consumer | queue=relaybus.billing error=channel is closing',
    );
  });

  it('stops without a channel', async () => {
    const { consumer } = setup();
    await consumer.stop();
  });
});
