import { InMemoryEventStore, RelayBus } from '@relaybus/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RelayBusHealth } from '../src/index.js';
import { OrderPlaced, createApplication, manualOptions, relayBusRoot } from './helpers.js';

afterEach(() => {
  vi.restoreAllMocks();
});

describe('RelayBusHealth', () => {
  it('reports up with statistics and zero lag when idle', async () => {
    const moduleRef = await createApplication({ imports: [relayBusRoot()] });
    await moduleRef.init();

    const report = await moduleRef.get(RelayBusHealth).check();

    expect(report).toEqual({
      status: 'up',
      running: true,
      handlers: 0,
      statistics: { pending: 0, processing: 0, completed: 0, dead: 0, oldestDueAt: null },
      lagMilliseconds: 0,
    });
    await moduleRef.close();
  });

  it('reports the lag of the oldest due delivery', async () => {
    const store = new InMemoryEventStore();
    const moduleRef = await createApplication({ imports: [relayBusRoot(manualOptions(store))] });
    await moduleRef.init();
    const bus = moduleRef.get(RelayBus);
    bus.subscribe(OrderPlaced, () => undefined, { id: 'late' });
    await bus.transaction((transaction) =>
      bus.publish(new OrderPlaced({ orderId: 'o' }), { transaction }),
    );

    const report = await moduleRef.get(RelayBusHealth).check();

    expect(report.statistics?.pending).toBe(1);
    expect(report.lagMilliseconds).toBeGreaterThanOrEqual(0);
    expect(report.handlers).toBe(1);
    await moduleRef.close();
  });

  it('reports down before start and after stop', async () => {
    const moduleRef = await createApplication({ imports: [relayBusRoot()] });
    const health = moduleRef.get(RelayBusHealth);

    expect(await health.check()).toEqual({ status: 'down', running: false, handlers: 0 });

    await moduleRef.init();
    await moduleRef.close();

    expect((await health.check()).status).toBe('down');
  });

  it('reports down with the error when the store fails', async () => {
    const store = new InMemoryEventStore();
    const moduleRef = await createApplication({ imports: [relayBusRoot(manualOptions(store))] });
    await moduleRef.init();
    vi.spyOn(store, 'statistics').mockRejectedValueOnce(new Error('connection lost'));

    const report = await moduleRef.get(RelayBusHealth).check();

    expect(report).toEqual({
      status: 'down',
      running: true,
      handlers: 0,
      error: 'connection lost',
    });
    vi.spyOn(store, 'statistics').mockRejectedValueOnce('plain failure');
    expect((await moduleRef.get(RelayBusHealth).check()).error).toBe('plain failure');
    await moduleRef.close();
  });
});
