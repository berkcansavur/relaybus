import { describe, expect, it } from 'vitest';
import { OrderPlaced, createTestBus, waitFor } from './helpers.js';

/** Shutdown has a deadline: nothing it waits for may hold it longer than the shutdown timeout. */
describe('shutdown deadline', () => {
  it('does not wait for a store that never answers the relinquish call', async () => {
    const { bus, store } = createTestBus({
      dispatcher: { pollInterval: 5, shutdownTimeout: 20 },
    });
    Object.assign(store, { relinquish: () => new Promise<void>(() => undefined) });
    await bus.start();
    const started = Date.now();
    await bus.stop();
    expect(Date.now() - started).toBeLessThan(2_000);
  }, 10_000);

  it('writes the completions still waiting for the batch window as soon as shutdown begins', async () => {
    const { bus } = createTestBus({
      transactionMode: 'optional',
      dispatcher: {
        pollInterval: 5,
        shutdownTimeout: 20,
        concurrency: 2,
        batchSize: 2,
        completionBatchWindow: 700,
      },
    });
    let slowRunning = false;
    bus.subscribe(
      OrderPlaced,
      async (event, context) => {
        if (event.payload.orderId === 'slow') {
          slowRunning = true;
          await new Promise<void>((resolve) =>
            context.signal.addEventListener('abort', () => resolve()),
          );
        }
      },
      { id: 'h' },
    );
    await bus.start();
    await bus.publish(new OrderPlaced({ orderId: 'slow' }));
    await waitFor(() => slowRunning, 3_000);
    await bus.publish(new OrderPlaced({ orderId: 'fast' }));
    // The fast delivery finished and waits for company in the batch window while the slow one still runs.
    await new Promise((resolve) => setTimeout(resolve, 100));
    const started = Date.now();
    await bus.stop();
    expect(Date.now() - started).toBeLessThan(400);
  }, 10_000);
});
