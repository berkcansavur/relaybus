import { beforeEach, describe, expect, it, vi } from 'vitest';
import { InvalidEnvelopeError, createRetryPolicy, type EventEnvelope } from '@relaybus/core';
import type { AmqpMessage } from '../src/amqp-types.js';
import { QueueConsumer, type ConfirmedRepublish } from '../src/queue-consumer.js';
import { decodeEnvelope } from '../src/wire-format.js';
import {
  DEAD_LETTER_ERROR_CODE_HEADER,
  DEAD_LETTER_ERROR_NAME_HEADER,
  DEAD_LETTER_REASON_HEADER,
  FIRST_FAILURE_HEADER,
  ORIGINAL_ROUTING_KEY_HEADER,
  REDELIVERIES_HEADER,
} from '../src/redelivery.js';
import {
  FakeChannel,
  createDelivery,
  createEnvelope,
  createLogger,
  deferred,
  waitFor,
} from './fakes.js';

const RETRY = createRetryPolicy({ attempts: 3, initialDelay: 5, maxDelay: 5, jitter: 'none' });
const REDELIVERY = createRetryPolicy(
  { attempts: Infinity, initialDelay: 100, maxDelay: 400, factor: 2, jitter: 'none' },
  { ...RETRY, attempts: Infinity },
);

interface SetupOverrides {
  handlerRetry?: typeof RETRY;
  redelivery?: typeof REDELIVERY;
  retryLadder?: readonly number[];
  redeliveryMaxAgeMs?: number;
  ordered?: boolean;
  shutdownTimeoutMs?: number;
  republish?: ConfirmedRepublish;
  decode?: (message: AmqpMessage) => EventEnvelope;
}

function setup(
  onMessage: (envelope: EventEnvelope) => Promise<void> = async () => undefined,
  overrides: SetupOverrides = {},
) {
  const logger = createLogger();
  const onConsumerLost = vi.fn();
  const handler = vi.fn(onMessage);
  const republish = vi.fn<ConfirmedRepublish>(overrides.republish ?? (async () => undefined));
  const consumer = new QueueConsumer({
    queue: 'relaybus.billing',
    prefetch: 7,
    handlerRetry: overrides.handlerRetry ?? RETRY,
    redelivery: overrides.redelivery ?? REDELIVERY,
    retryLadder: overrides.retryLadder ?? [100, 200, 400],
    redeliveryMaxAgeMs: overrides.redeliveryMaxAgeMs ?? 10_000,
    ordered: overrides.ordered ?? false,
    deadLetterExchange: 'relaybus.dead',
    shutdownTimeoutMs: overrides.shutdownTimeoutMs ?? 1_000,
    onMessage: handler,
    decode: overrides.decode ?? decodeEnvelope,
    onConsumerLost,
    republish,
    logger,
    random: () => 0.5,
  });
  return { consumer, handler, logger, onConsumerLost, republish, channel: new FakeChannel() };
}

const failing = async (): Promise<void> => {
  throw new Error('database down');
};

function withHeaders(message: AmqpMessage, headers: Record<string, unknown>): AmqpMessage {
  return {
    ...message,
    properties: { ...message.properties, headers: { ...message.properties.headers, ...headers } },
  };
}

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

  describe('delayed redelivery', () => {
    it('parks a copy in the retry queue once the local attempts are spent, and acknowledges only after the broker confirmed it', async () => {
      const confirmed = deferred();
      const { consumer, channel, handler, republish, logger } = setup(failing, {
        republish: () => confirmed.promise,
      });
      await consumer.attach(channel);
      const before = Date.now();
      channel.deliver!(
        withHeaders(message, {
          'x-delivery-count': 4,
          'x-acquired-count': 4,
          [DEAD_LETTER_REASON_HEADER]: 'stale',
          [DEAD_LETTER_ERROR_NAME_HEADER]: 'StaleError',
          [DEAD_LETTER_ERROR_CODE_HEADER]: 'STALE',
        }),
      );
      await waitFor(() => republish.mock.calls.length === 1);
      expect(handler).toHaveBeenCalledTimes(3);
      expect(channel.ack).not.toHaveBeenCalled();
      expect(channel.nack).not.toHaveBeenCalled();

      const [{ exchange, routingKey }, { content, options }] = republish.mock.calls[0]!;
      expect(exchange).toBe('');
      expect(routingKey).toBe('relaybus.billing.retry.100');
      expect(content).toBe(message.content);
      expect(options).toMatchObject({
        contentType: 'application/json',
        persistent: true,
        messageId: createEnvelope().id,
        type: 'order.placed.v1',
        appId: 'orders-service',
      });
      expect(options.headers).toMatchObject({
        'cloudEvents:id': createEnvelope().id,
        'relaybus:traceparent': '00-abc-def-01',
        [REDELIVERIES_HEADER]: '1',
        [ORIGINAL_ROUTING_KEY_HEADER]: 'order.placed.v1',
      });
      expect(options.headers).not.toHaveProperty('x-delivery-count');
      expect(options.headers).not.toHaveProperty('x-acquired-count');
      expect(options.headers).not.toHaveProperty(DEAD_LETTER_REASON_HEADER);
      expect(options.headers).not.toHaveProperty(DEAD_LETTER_ERROR_NAME_HEADER);
      expect(options.headers).not.toHaveProperty(DEAD_LETTER_ERROR_CODE_HEADER);
      expect(Number(options.headers[FIRST_FAILURE_HEADER])).toBeGreaterThanOrEqual(before);
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Handler attempts spent, retrying later'),
      );

      confirmed.resolve();
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(channel.ack).toHaveBeenCalledWith(
        expect.objectContaining({ content: message.content }),
      );
    });

    it('grows the delay with every redelivery, up to the largest retry queue, and keeps the first failure time', async () => {
      const firstFailureAt = Date.now() - 1_000;
      const { consumer, channel, republish } = setup(failing);
      await consumer.attach(channel);
      channel.deliver!(
        withHeaders(message, {
          [REDELIVERIES_HEADER]: 1,
          [FIRST_FAILURE_HEADER]: String(firstFailureAt),
        }),
      );
      channel.deliver!(
        withHeaders(message, {
          [REDELIVERIES_HEADER]: '7',
          [FIRST_FAILURE_HEADER]: String(firstFailureAt),
        }),
      );
      await waitFor(() => republish.mock.calls.length === 2);
      const byQueue = Object.fromEntries(
        republish.mock.calls.map(([{ routingKey }, { options }]) => [routingKey, options.headers]),
      );
      expect(byQueue['relaybus.billing.retry.200']).toMatchObject({
        [REDELIVERIES_HEADER]: '2',
        [FIRST_FAILURE_HEADER]: String(firstFailureAt),
      });
      expect(byQueue['relaybus.billing.retry.400']).toMatchObject({
        [REDELIVERIES_HEADER]: '8',
      });
    });

    it('treats unreadable redelivery state as a first failure', async () => {
      const { consumer, channel, republish } = setup(failing);
      await consumer.attach(channel);
      channel.deliver!(
        withHeaders(message, { [REDELIVERIES_HEADER]: 'many', [FIRST_FAILURE_HEADER]: -5 }),
      );
      await waitFor(() => republish.mock.calls.length === 1);
      expect(republish.mock.calls[0]![0].routingKey).toBe('relaybus.billing.retry.100');
      expect(republish.mock.calls[0]![1].options.headers[REDELIVERIES_HEADER]).toBe('1');
    });

    it('requeues the original, not acknowledging it, when the copy cannot be confirmed', async () => {
      const { consumer, channel, logger } = setup(failing, {
        republish: async () => {
          throw new Error('nacked');
        },
      });
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => channel.nack.mock.calls.length === 1);
      expect(channel.nack).toHaveBeenCalledWith(message, false, true);
      expect(channel.ack).not.toHaveBeenCalled();
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Could not park the message in the retry queue, requeueing it'),
      );
    });

    it('does not wait out the fallback delay when it stops', async () => {
      const { consumer, channel, republish } = setup(failing, {
        retryLadder: [600_000],
        redeliveryMaxAgeMs: 3_600_000,
        republish: async () => {
          throw new Error('nacked');
        },
      });
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => republish.mock.calls.length === 1);
      await consumer.stop();
      expect(channel.nack).toHaveBeenCalledWith(message, false, true);
    });
  });

  describe('dead-lettering', () => {
    it('dead-letters through a confirmed copy without retry state once the message failed for too long', async () => {
      const { consumer, channel, logger, republish } = setup(failing, {
        redeliveryMaxAgeMs: 5_000,
      });
      await consumer.attach(channel);
      channel.deliver!(
        withHeaders(message, {
          [REDELIVERIES_HEADER]: 12,
          [FIRST_FAILURE_HEADER]: String(Date.now() - 60_000),
        }),
      );
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(republish).toHaveBeenCalledTimes(1);
      const [{ exchange, routingKey }, { options }] = republish.mock.calls[0]!;
      expect(exchange).toBe('relaybus.dead');
      expect(routingKey).toBe('relaybus.billing');
      expect(options.headers[DEAD_LETTER_REASON_HEADER]).toBe('redelivery window exhausted');
      expect(options.headers[DEAD_LETTER_ERROR_NAME_HEADER]).toBe('Error');
      expect(options.headers).not.toHaveProperty(DEAD_LETTER_ERROR_CODE_HEADER);
      expect(options.headers).not.toHaveProperty(REDELIVERIES_HEADER);
      expect(options.headers).not.toHaveProperty(FIRST_FAILURE_HEADER);
      expect(options.headers['cloudEvents:id']).toBe(createEnvelope().id);
      expect(channel.nack).not.toHaveBeenCalled();
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('Dead-lettering message that kept failing'),
      );
    });

    it('dead-letters immediately when the error must not be retried', async () => {
      const { consumer, channel, handler, logger, republish } = setup(async () => {
        throw new InvalidEnvelopeError('invalid occurredAt');
      });
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(handler).toHaveBeenCalledTimes(1);
      expect(republish.mock.calls[0]![0].exchange).toBe('relaybus.dead');
      expect(republish.mock.calls[0]![1].options.headers[DEAD_LETTER_REASON_HEADER]).toBe(
        'non-retryable handler error',
      );
      expect(republish.mock.calls[0]![1].options.headers).toMatchObject({
        [DEAD_LETTER_ERROR_NAME_HEADER]: 'InvalidEnvelopeError',
        [DEAD_LETTER_ERROR_CODE_HEADER]: 'RELAYBUS_INVALID_ENVELOPE',
      });
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringMatching(
          /error=invalid occurredAt errorName=InvalidEnvelopeError code=RELAYBUS_INVALID_ENVELOPE$/,
        ),
      );
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('Dead-lettering message the handler cannot process'),
      );
    });

    it('dead-letters a malformed message without calling the handler, keeping its bytes', async () => {
      const { consumer, channel, handler, logger, republish } = setup();
      await consumer.attach(channel);
      const malformed = { ...message, content: Buffer.from('not json') };
      channel.deliver!(malformed);
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(handler).not.toHaveBeenCalled();
      expect(republish.mock.calls[0]![1].content).toBe(malformed.content);
      expect(republish.mock.calls[0]![1].options.headers[DEAD_LETTER_REASON_HEADER]).toBe(
        'malformed message',
      );
      expect(logger.error).toHaveBeenCalledWith(
        '[QueueConsumer](decodeOrDeadLetter) Dead-lettering malformed message | queue=relaybus.billing routingKey=order.placed.v1 error=Message body is not valid JSON errorName=MalformedMessageError code=RELAYBUS_RABBITMQ_MALFORMED_MESSAGE',
      );
      expect(republish.mock.calls[0]![1].options.headers).toMatchObject({
        [DEAD_LETTER_ERROR_NAME_HEADER]: 'MalformedMessageError',
        [DEAD_LETTER_ERROR_CODE_HEADER]: 'RELAYBUS_RABBITMQ_MALFORMED_MESSAGE',
      });
    });

    it('dead-letters a message whose decoder throws any error, and keeps consuming', async () => {
      const decode = vi.fn((input: AmqpMessage): EventEnvelope => {
        if (input.content.toString() === 'garbage') {
          throw new TypeError('cannot read properties of undefined');
        }
        return createEnvelope();
      });
      const { consumer, channel, handler, logger, republish } = setup(undefined, { decode });
      await consumer.attach(channel);
      channel.deliver!({ ...message, content: Buffer.from('garbage') });
      channel.deliver!(message);
      await waitFor(() => channel.ack.mock.calls.length === 2);
      expect(handler).toHaveBeenCalledTimes(1);
      expect(republish).toHaveBeenCalledTimes(1);
      expect(republish.mock.calls[0]![1].options.headers[DEAD_LETTER_REASON_HEADER]).toBe(
        'malformed message',
      );
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('error=cannot read properties of undefined errorName=TypeError'),
      );
      expect(republish.mock.calls[0]![1].options.headers[DEAD_LETTER_ERROR_NAME_HEADER]).toBe(
        'TypeError',
      );
      expect(republish.mock.calls[0]![1].options.headers).not.toHaveProperty(
        DEAD_LETTER_ERROR_CODE_HEADER,
      );
    });

    it('hands the decoder a retry copy with its original routing key and keeps that key on the next copy', async () => {
      const decode = vi.fn((_input: AmqpMessage) => createEnvelope());
      const { consumer, channel, republish } = setup(
        async () => {
          throw new Error('db down');
        },
        { decode },
      );
      await consumer.attach(channel);
      const retryCopy = withHeaders(
        { ...message, fields: { routingKey: 'relaybus.billing' } },
        { [ORIGINAL_ROUTING_KEY_HEADER]: 'orders.placed', [REDELIVERIES_HEADER]: '1' },
      );
      channel.deliver!(retryCopy);
      await waitFor(() => republish.mock.calls.length === 1);
      expect(decode.mock.calls[0]![0].fields.routingKey).toBe('orders.placed');
      expect(republish.mock.calls[0]![1].options.headers[ORIGINAL_ROUTING_KEY_HEADER]).toBe(
        'orders.placed',
      );
    });

    it('processes a message without headers, as a custom decoder may accept one', async () => {
      const { consumer, channel, handler } = setup(undefined, { decode: () => createEnvelope() });
      await consumer.attach(channel);
      channel.deliver!({ content: Buffer.from('{}'), fields: { routingKey: 'x' }, properties: {} });
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('dead-letters a message without any properties', async () => {
      const { consumer, channel, republish } = setup();
      await consumer.attach(channel);
      channel.deliver!({ content: Buffer.from('x'), fields: { routingKey: 'x' }, properties: {} });
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(republish.mock.calls[0]![1].options).toEqual({
        contentType: 'application/json',
        persistent: true,
        headers: {
          [DEAD_LETTER_REASON_HEADER]: 'malformed message',
          [DEAD_LETTER_ERROR_NAME_HEADER]: 'MalformedMessageError',
          [DEAD_LETTER_ERROR_CODE_HEADER]: 'RELAYBUS_RABBITMQ_MALFORMED_MESSAGE',
        },
      });
    });

    it('lets the broker dead-letter the message when the copy cannot be confirmed', async () => {
      const { consumer, channel, logger } = setup(
        async () => {
          throw new InvalidEnvelopeError('bad');
        },
        {
          republish: async () => {
            throw new Error('channel closed');
          },
        },
      );
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => channel.nack.mock.calls.length === 1);
      expect(channel.nack).toHaveBeenCalledWith(message, false, false);
      expect(channel.ack).not.toHaveBeenCalled();
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Could not publish to the dead-letter exchange'),
      );
    });
  });

  describe('ordered mode', () => {
    const orderedRedelivery = createRetryPolicy(
      { attempts: Infinity, initialDelay: 5, maxDelay: 5, jitter: 'none' },
      { ...RETRY, attempts: Infinity },
    );

    it('retries in place, beyond the local attempts, and never moves the message', async () => {
      let calls = 0;
      const { consumer, channel, republish } = setup(
        async () => {
          calls += 1;
          if (calls < 8) {
            throw new Error('database down');
          }
        },
        { ordered: true, redelivery: orderedRedelivery, retryLadder: [] },
      );
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(calls).toBe(8);
      expect(republish).not.toHaveBeenCalled();
      expect(channel.nack).not.toHaveBeenCalled();
    });

    it('requeues a message waiting between in-place retries when it stops', async () => {
      const { consumer, channel, handler } = setup(failing, {
        ordered: true,
        handlerRetry: createRetryPolicy({ attempts: 1 }),
        redelivery: createRetryPolicy(
          { attempts: Infinity, initialDelay: 600_000, jitter: 'none' },
          { ...RETRY, attempts: Infinity },
        ),
        redeliveryMaxAgeMs: 3_600_000,
        retryLadder: [],
      });
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => handler.mock.calls.length === 1);
      await consumer.stop();
      expect(channel.nack).toHaveBeenCalledWith(message, false, true);
    });

    it('dead-letters after the retry window like the unordered mode', async () => {
      const { consumer, channel, republish } = setup(failing, {
        ordered: true,
        redelivery: orderedRedelivery,
        redeliveryMaxAgeMs: 40,
        retryLadder: [],
      });
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(republish).toHaveBeenCalledTimes(1);
      expect(republish.mock.calls[0]![0].exchange).toBe('relaybus.dead');
    });
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
