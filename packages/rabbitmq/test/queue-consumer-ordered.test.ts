import { beforeEach, describe, expect, it } from 'vitest';
import { createRetryPolicy } from '@relaybus/core';
import { createDelivery, waitFor } from './fakes.js';
import { RETRY, setup, failing } from './support/queue-consumer-setup.js';

describe('QueueConsumer', () => {
  let message: ReturnType<typeof createDelivery>;
  beforeEach(() => {
    message = createDelivery();
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
});
