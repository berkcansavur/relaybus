import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  InMemoryEventStore,
  consume,
  defineCatalog,
  type StopResult,
} from '@relaybus/core';
import { InMemoryTransport } from '@relaybus/testing';
import { OrderPlaced, createTestBus, deferred } from './helpers.js';

// Characterisation of the RelayBus lifecycle state machine (idle, starting, running, stopping,
// stopped), written before the 0.3.1 readability refactor and kept unchanged through it.

/** What a call answers: the resolved value, or the class and message of the rejection. */
async function outcome(work: () => Promise<unknown>): Promise<string> {
  try {
    const value = await work();
    return `resolved ${JSON.stringify(value === undefined ? null : summary(value))}`;
  } catch (error) {
    return `rejected ${(error as Error).name}: ${(error as Error).message}`;
  }
}

function summary(value: unknown): unknown {
  if (typeof value === 'object' && value !== null && 'graceful' in value) {
    const { graceful, abandonedDeliveries } = value as StopResult;
    return { graceful, abandonedDeliveries };
  }
  return value;
}

describe('RelayBus state machine (characterisation)', () => {
  it('idle: start runs, stop leaves it startable', async () => {
    const { bus } = createTestBus({ dispatcher: { enabled: false } });
    expect([bus.isRunning, bus.isReady()]).toEqual([false, false]);
    expect(await outcome(() => bus.stop())).toBe(
      'resolved {"graceful":true,"abandonedDeliveries":0}',
    );
    expect(await outcome(() => bus.start())).toBe('resolved null');
    expect([bus.isRunning, bus.isReady()]).toEqual([true, true]);
    expect(await outcome(() => bus.stop())).toBe(
      'resolved {"graceful":true,"abandonedDeliveries":0}',
    );
    expect([bus.isRunning, bus.isReady()]).toEqual([false, false]);
  });

  it('running: a second start is refused, stop twice shares the first result', async () => {
    const { bus } = createTestBus({ dispatcher: { enabled: false } });
    await bus.start();
    expect(await outcome(() => bus.start())).toBe(
      'rejected LifecycleError: Cannot start a relaybus that is running',
    );
    const first = bus.stop();
    const second = bus.stop();
    expect(await second).toBe(await first);
    expect(await outcome(() => bus.stop())).toBe(
      'resolved {"graceful":true,"abandonedDeliveries":0}',
    );
  });

  it('stopped: start is refused for good', async () => {
    const { bus } = createTestBus({ dispatcher: { enabled: false } });
    await bus.start();
    await bus.stop();
    expect(await outcome(() => bus.start())).toBe(
      'rejected LifecycleError: A stopped relaybus cannot be started again; create a new instance',
    );
  });

  it('starting: a second start is refused and stop waits for the start', async () => {
    const setup = deferred();
    const store = new InMemoryEventStore();
    Object.assign(store, { setup: vi.fn(() => setup.promise) });
    const { bus } = createTestBus({ store, dispatcher: { enabled: false } });
    const starting = bus.start();
    expect(await outcome(() => bus.start())).toBe(
      'rejected LifecycleError: Cannot start a relaybus that is starting',
    );
    const stopping = bus.stop();
    setup.resolve();
    await starting;
    expect(summary(await stopping)).toEqual({ graceful: true, abandonedDeliveries: 0 });
    expect(bus.isRunning).toBe(false);
  });

  it('starting past the stop deadline: the start rejects once it resumes', async () => {
    const setup = deferred();
    const store = new InMemoryEventStore();
    Object.assign(store, { setup: vi.fn(() => setup.promise) });
    const transport = new InMemoryTransport('broker');
    const { bus, clock } = createTestBus({ store, transports: [transport] });
    const starting = outcome(() => bus.start());
    const stopping = bus.stop({ timeout: 10 });
    await new Promise((resolve) => setImmediate(resolve));
    clock.advance(10);
    const result = await stopping;
    expect(summary(result)).toEqual({ graceful: false, abandonedDeliveries: 0 });
    expect(transport.closed).toBe(true);
    setup.resolve();
    expect(await starting).toBe(
      'rejected LifecycleError: relaybus was stopped before it finished starting',
    );
  });

  it('a failed start leaves it stopped, with every transport closed', async () => {
    const store = new InMemoryEventStore();
    Object.assign(store, { setup: vi.fn().mockRejectedValue(new Error('migrations failed')) });
    const transport = new InMemoryTransport('broker');
    const { bus } = createTestBus({ store, transports: [transport] });
    expect(await outcome(() => bus.start())).toBe('rejected Error: migrations failed');
    expect(transport.closed).toBe(true);
    expect(await outcome(() => bus.stop())).toBe(
      'resolved {"graceful":true,"abandonedDeliveries":0}',
    );
    expect(await outcome(() => bus.start())).toBe(
      'rejected LifecycleError: A stopped relaybus cannot be started again; create a new instance',
    );
  });

  it('an uncovered catalog fails start before any side effect and leaves it idle', async () => {
    const { bus } = createTestBus({ dispatcher: { enabled: false } });
    bus.registerCatalog(consume(defineCatalog({ OrderPlaced })));
    await expect(bus.start()).rejects.toBeInstanceOf(ConfigurationError);
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing.onOrderPlaced' });
    expect(await outcome(() => bus.start())).toBe('resolved null');
    await bus.stop();
  });
});
