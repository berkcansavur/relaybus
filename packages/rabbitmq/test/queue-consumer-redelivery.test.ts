import { beforeEach, describe, expect, it } from 'vitest';
import {
  DEAD_LETTER_ERROR_CODE_HEADER,
  DEAD_LETTER_ERROR_NAME_HEADER,
  DEAD_LETTER_REASON_HEADER,
  FIRST_FAILURE_HEADER,
  ORIGINAL_ROUTING_KEY_HEADER,
  REDELIVERIES_HEADER,
} from '../src/redelivery.js';
import { createDelivery, createEnvelope, deferred, waitFor } from './fakes.js';
import { setup, failing, withHeaders } from './support/queue-consumer-setup.js';

describe('QueueConsumer', () => {
  let message: ReturnType<typeof createDelivery>;
  beforeEach(() => {
    message = createDelivery();
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
});
