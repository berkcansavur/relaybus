import { InMemoryEventStore, RelayBus, type Transport } from '@relaybus/core';
import { FakeClock } from '@relaybus/testing';
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
      ready: true,
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

    expect(await health.check()).toEqual({
      status: 'down',
      running: false,
      ready: false,
      handlers: 0,
    });

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
      ready: true,
      handlers: 0,
      error: 'connection lost',
    });
    vi.spyOn(store, 'statistics').mockRejectedValueOnce('plain failure');
    expect((await moduleRef.get(RelayBusHealth).check()).error).toBe('plain failure');
    await moduleRef.close();
  });

  it('reports down while a transport is not ready and up again once it is', async () => {
    let ready = true;
    const transport: Transport = {
      name: 'broker',
      publish: () => Promise.resolve(),
      isReady: () => ready,
      close: () => Promise.resolve(),
    };
    const moduleRef = await createApplication({
      imports: [relayBusRoot({ ...manualOptions(), transports: [transport] })],
    });
    await moduleRef.init();
    const health = moduleRef.get(RelayBusHealth);

    expect(await health.check()).toMatchObject({ status: 'up', running: true, ready: true });

    ready = false;
    expect(await health.check()).toMatchObject({
      status: 'down',
      running: true,
      ready: false,
      lagMilliseconds: 0,
    });

    ready = true;
    expect((await health.check()).status).toBe('up');
    await moduleRef.close();
  });

  it('measures the lag with the clock of the module options', async () => {
    const clock = new FakeClock();
    const moduleRef = await createApplication({
      imports: [relayBusRoot({ ...manualOptions(), clock })],
    });
    await moduleRef.init();
    const bus = moduleRef.get(RelayBus);
    bus.subscribe(OrderPlaced, () => undefined, { id: 'late' });
    await bus.transaction((transaction) =>
      bus.publish(new OrderPlaced({ orderId: 'o' }), { transaction }),
    );

    clock.advance(7_000);

    expect((await moduleRef.get(RelayBusHealth).check()).lagMilliseconds).toBe(7_000);
    await moduleRef.close();
  });
});
