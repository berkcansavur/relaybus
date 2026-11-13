import { describe, expect, it } from 'vitest';
import { createLoggingObserver, defaultLoggingFormat } from '@relaybus/core';
import { OrderPlaced, createLogger, createTestBus } from './helpers.js';

function loggingBus(observer: ReturnType<typeof createLoggingObserver>) {
  const test = createTestBus({
    observers: [observer],
    transactionMode: 'optional',
    retry: { attempts: 2, initialDelay: 1, factor: 1, jitter: 'none' },
  });
  return test;
}

describe('createLoggingObserver', () => {
  it('logs retries as warnings and dead letters as errors by default, nothing else', async () => {
    const logger = createLogger();
    const { bus, clock } = loggingBus(createLoggingObserver({ logger }));
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new Error('broker down');
      },
      { id: 'h' },
    );
    await bus.publish(new OrderPlaced({ orderId: 'a' }));

    await bus.runOnce();
    clock.advance(10);
    await bus.runOnce();

    expect(logger.log).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledTimes(1);
    expect(logger.warn.mock.calls[0]![0]).toMatch(
      /^\[relaybus\]\(onDeliveryRetryScheduled\) Delivery failed, retry scheduled \| handler=h \| eventId=\S+ \| eventType=order\.placed\.v1 \| attempt=1 \| availableAt=\S+ \| error=broker down$/,
    );
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error.mock.calls[0]![0]).toMatch(
      /^\[relaybus\]\(onDeliveryDead\) Delivery dead-lettered \| handler=h \| eventId=\S+ \| eventType=order\.placed\.v1 \| attempt=2 \| error=broker down$/,
    );
  });

  it('logs one line per committed event and per successful delivery when asked to', async () => {
    const logger = createLogger();
    const { bus } = loggingBus(
      createLoggingObserver({ logger, events: ['committed', 'succeeded'] }),
    );
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });

    await bus.transaction(async () => {
      await bus.publishAll([new OrderPlaced({ orderId: 'a' }), new OrderPlaced({ orderId: 'b' })]);
      expect(logger.log).not.toHaveBeenCalled();
    });
    expect(logger.log).toHaveBeenCalledTimes(2);
    expect(logger.log.mock.calls[0]![0]).toMatch(
      /^\[relaybus\]\(onPublishCommitted\) Event committed \| eventId=\S+ \| eventType=order\.placed\.v1 \| correlationId=\S+$/,
    );

    await bus.runOnce();

    expect(logger.log).toHaveBeenCalledTimes(4);
    expect(logger.log.mock.calls[2]![0]).toMatch(
      /^\[relaybus\]\(onDeliverySucceeded\) Delivery succeeded \| handler=h \| .* \| durationMs=\d+$/,
    );
  });

  it('uses a caller format per event, and skips the line it returns undefined for', async () => {
    const logger = createLogger();
    const { bus } = loggingBus(
      createLoggingObserver({
        logger,
        events: ['committed'],
        format: {
          committed: (eventEnvelope) =>
            (eventEnvelope.payload as { orderId: string }).orderId === 'skip'
              ? undefined
              : `written ${eventEnvelope.type}`,
        },
      }),
    );
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });

    await bus.publish(new OrderPlaced({ orderId: 'keep' }));
    await bus.publish(new OrderPlaced({ orderId: 'skip' }));

    expect(logger.log.mock.calls).toEqual([['written order.placed.v1']]);
  });

  it('implements only the hooks of the events it logs', () => {
    const observer = createLoggingObserver({ logger: createLogger(), events: ['dead'] });

    expect(Object.keys(observer)).toEqual(['onDeliveryDead']);
  });

  it('exposes the default format so a caller can extend it', () => {
    expect(Object.keys(defaultLoggingFormat).sort()).toEqual([
      'committed',
      'dead',
      'retry',
      'succeeded',
    ]);
  });
});
