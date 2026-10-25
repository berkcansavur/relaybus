import { beforeEach, describe, expect, it } from 'vitest';
import { createRetryPolicy } from '@relaybus/core';
import { createDelivery, createEnvelope, deferred, waitFor } from './fakes.js';
import { setup, failing } from './support/queue-consumer-setup.js';

describe('QueueConsumer', () => {
  let message: ReturnType<typeof createDelivery>;
  beforeEach(() => {
    message = createDelivery();
  });

  it('consumes with manual acknowledgements and the configured prefetch', async () => {
    const { consumer, channel } = setup();
    expect(consumer.isActive()).toBe(false);
    await consumer.attach(channel);
    expect(consumer.isActive()).toBe(true);
    expect(channel.prefetch).toHaveBeenCalledWith(7);
    expect(channel.consume).toHaveBeenCalledWith('relaybus.billing', expect.any(Function), {
      noAck: false,
    });
  });

  it('acknowledges only after the handler resolved, handing it the decoded envelope', async () => {
    const gate = deferred();
    const { consumer, channel, handler, republish } = setup(() => gate.promise);
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => handler.mock.calls.length === 1);
    expect(channel.ack).not.toHaveBeenCalled();
    expect(handler.mock.calls[0]![0]).toEqual(createEnvelope());
    gate.resolve();
    await waitFor(() => channel.ack.mock.calls.length === 1);
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
    expect(republish).not.toHaveBeenCalled();
  });

  it('retries a failing handler on the same delivery before acknowledging', async () => {
    let calls = 0;
    const { consumer, channel, logger, republish } = setup(async () => {
      calls += 1;
      if (calls < 3) {
        throw new Error('database down');
      }
    });
    await consumer.attach(channel);
    channel.deliver!(message);
    await waitFor(() => channel.ack.mock.calls.length === 1);
    expect(calls).toBe(3);
    expect(republish).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        '[QueueConsumer](afterFailure) Handler failed, retrying | queue=relaybus.billing',
      ),
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

  it('reports a consumer cancelled by the broker and stops counting as active', async () => {
    const { consumer, channel, onConsumerLost } = setup();
    await consumer.attach(channel);
    channel.deliver!(null);
    expect(onConsumerLost).toHaveBeenCalledWith('consumer cancelled by the broker');
    expect(consumer.isActive()).toBe(false);
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
    expect(consumer.isActive()).toBe(false);

    const second = setup();
    await second.consumer.attach(second.channel);
    second.consumer.detach();
    second.channel.emit('close');
    expect(second.onConsumerLost).not.toHaveBeenCalled();
  });

  it('fails to attach when the channel closed while the consumer was being registered', async () => {
    const { consumer, channel, onConsumerLost } = setup();
    channel.consume.mockImplementationOnce(async () => {
      channel.emit('close');
      return { consumerTag: 'consumer-1' };
    });
    await expect(consumer.attach(channel)).rejects.toThrow(
      /Consumer channel closed while the consumer was being set up/,
    );
    expect(consumer.isActive()).toBe(false);
    expect(onConsumerLost).not.toHaveBeenCalled();
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
    const { consumer, channel, handler } = setup(failing, { handlerRetry: slowRetry });
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
