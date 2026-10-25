import { beforeEach, describe, expect, it, vi } from 'vitest';
import { InvalidEnvelopeError, type EventEnvelope } from '@relaybus/core';
import type { AmqpMessage } from '../src/amqp-types.js';
import {
  DEAD_LETTER_ERROR_CODE_HEADER,
  DEAD_LETTER_ERROR_NAME_HEADER,
  DEAD_LETTER_REASON_HEADER,
  FIRST_FAILURE_HEADER,
  ORIGINAL_ROUTING_KEY_HEADER,
  REDELIVERIES_HEADER,
} from '../src/redelivery.js';
import { createDelivery, createEnvelope, waitFor } from './fakes.js';
import { setup, failing, withHeaders } from './support/queue-consumer-setup.js';

describe('QueueConsumer', () => {
  let message: ReturnType<typeof createDelivery>;
  beforeEach(() => {
    message = createDelivery();
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

    it('reports a malformed message to the bus once, with its routing key and the decode error', async () => {
      const onInvalidMessage = vi.fn();
      const { consumer, channel, handler } = setup(undefined, { onInvalidMessage });
      await consumer.attach(channel);
      channel.deliver!({ ...message, content: Buffer.from('not json') });
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(onInvalidMessage).toHaveBeenCalledExactlyOnceWith({
        destination: 'order.placed.v1',
        error: expect.objectContaining({ name: 'MalformedMessageError' }),
      });
      expect(handler).not.toHaveBeenCalled();
    });

    it('does not report a message the handler rejects as non-retryable', async () => {
      const onInvalidMessage = vi.fn();
      const { consumer, channel } = setup(
        async () => {
          throw new InvalidEnvelopeError('bus says no');
        },
        { onInvalidMessage },
      );
      await consumer.attach(channel);
      channel.deliver!(message);
      await waitFor(() => channel.ack.mock.calls.length === 1);
      expect(onInvalidMessage).not.toHaveBeenCalled();
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
});
