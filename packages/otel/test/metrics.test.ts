import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  InMemoryEventStore,
  NonRetryableError,
  RelayBus,
  defineEvent,
  type ClaimedDelivery,
  type InMemoryTransaction,
} from '@relaybus/core';
import { FakeClock, InMemoryTransport } from '@relaybus/testing';
import { createTelemetry, type Point, type Telemetry } from './support.js';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
class OrderShipped extends defineEvent<{ orderId: string }>('order.shipped.v1') {}

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let telemetry: Telemetry;

afterEach(async () => {
  await telemetry.shutdown();
});

function createBus(
  options: Partial<ConstructorParameters<typeof RelayBus<InMemoryTransaction>>[0]> = {},
): { bus: RelayBus<InMemoryTransaction>; clock: FakeClock; store: InMemoryEventStore } {
  telemetry = createTelemetry();
  const clock = (options.clock as FakeClock | undefined) ?? new FakeClock();
  const store = (options.store as InMemoryEventStore | undefined) ?? new InMemoryEventStore();
  const bus = new RelayBus<InMemoryTransaction>({
    store,
    clock,
    logger: silent,
    transactionMode: 'optional',
    dispatcher: { enabled: false, completionBatchWindow: 0 },
    retry: { attempts: 2, initialDelay: 0, jitter: 'none' },
    observers: [telemetry.otel.observer],
    tracer: telemetry.otel.tracer,
    ...options,
  });
  return { bus, clock, store };
}

async function points(name: string): Promise<Point[]> {
  return (await telemetry.collect()).get(name)?.points ?? [];
}

describe('messaging metrics', () => {
  it('records publish, claim, process and end-to-end latency with semantic attributes', async () => {
    // One handler at a time, so the fake clock gives each run its own 250 ms.
    const { bus, clock } = createBus({
      dispatcher: { enabled: false, completionBatchWindow: 0, concurrency: 1 },
    });
    bus.subscribe(
      OrderPlaced,
      async () => {
        clock.advance(250);
      },
      { id: 'billing' },
    );
    await bus.publishAll([new OrderPlaced({ orderId: '1' }), new OrderPlaced({ orderId: '2' })]);
    clock.advance(1_500);
    await bus.drain();

    const metrics = await telemetry.collect();
    expect(metrics.get('relaybus.events.published')!.points).toEqual([
      {
        attributes: {
          'messaging.system': 'relaybus',
          'messaging.destination.name': 'order.placed.v1',
        },
        value: 2,
      },
    ]);
    expect(metrics.get('messaging.process.duration')!.points).toEqual([
      {
        attributes: {
          'messaging.system': 'relaybus',
          'messaging.destination.name': 'order.placed.v1',
          'messaging.consumer.group.name': 'billing',
          'messaging.operation.name': 'process',
          'messaging.operation.type': 'process',
        },
        value: { count: 2, sum: 0.5 },
      },
    ]);
    expect(metrics.get('messaging.process.duration')!.metric.descriptor.unit).toBe('s');
    // The second delivery waited for the first handler too.
    expect(metrics.get('relaybus.end_to_end.latency')!.points).toEqual([
      {
        attributes: {
          'messaging.system': 'relaybus',
          'messaging.destination.name': 'order.placed.v1',
          'messaging.consumer.group.name': 'billing',
          'relaybus.delivery.kind': 'handler',
        },
        value: { count: 2, sum: 1.5 + 1.75 },
      },
    ]);
    const operations = metrics.get('messaging.client.operation.duration')!.points;
    expect(operations.map((point) => point.attributes['messaging.operation.name']).sort()).toEqual([
      'claim',
      'publish',
    ]);
    expect(
      operations.find((point) => point.attributes['messaging.operation.name'] === 'publish'),
    ).toMatchObject({
      attributes: { 'messaging.destination.name': 'order.placed.v1' },
      value: { count: 1, sum: 0 },
    });
    expect(metrics.get('relaybus.claim.deliveries')!.points[0]!.value).toMatchObject({ sum: 2 });
    expect(metrics.get('relaybus.claim.lanes')!.points[0]!.value).toMatchObject({ sum: 2 });
  });

  it('records sends and receives by messaging system and destination', async () => {
    const transport = Object.assign(new InMemoryTransport('broker'), { system: 'kafka' });
    const { bus } = createBus({
      transports: [transport],
      routes: [
        {
          event: OrderPlaced,
          transport: 'broker',
          destination: 'orders',
          retry: { initialDelay: 0, jitter: 'none' },
        },
      ],
      subscriptions: [{ transport: 'broker', destinations: ['shipments'] }],
    });
    bus.subscribe(OrderShipped, async () => undefined, { id: 'tracking' });
    await bus.start();
    transport.failNextPublishes(1);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.drain();
    await bus.drain();
    const shipped = {
      id: 'evt-1',
      type: 'order.shipped.v1',
      source: 'shipping',
      key: null,
      payload: { orderId: '1' },
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    };
    await transport.deliver('shipments', shipped);
    await transport.deliver('shipments', shipped);
    await bus.stop();

    const metrics = await telemetry.collect();
    const send = {
      'messaging.system': 'kafka',
      'messaging.destination.name': 'orders',
      'messaging.operation.name': 'send',
      'messaging.operation.type': 'send',
    };
    expect(metrics.get('messaging.client.sent.messages')!.points).toEqual([
      { attributes: { ...send, 'error.type': 'Error' }, value: 1 },
      { attributes: send, value: 1 },
    ]);
    const receive = {
      'messaging.system': 'kafka',
      'messaging.destination.name': 'shipments',
      'messaging.operation.name': 'receive',
      'messaging.operation.type': 'receive',
    };
    expect(metrics.get('messaging.client.consumed.messages')!.points).toEqual([
      { attributes: { ...receive, 'relaybus.receive.outcome': 'accepted' }, value: 1 },
      { attributes: { ...receive, 'relaybus.receive.outcome': 'duplicate' }, value: 1 },
    ]);
    expect(metrics.get('relaybus.end_to_end.latency')!.points).toContainEqual(
      expect.objectContaining({
        attributes: expect.objectContaining({ 'relaybus.delivery.kind': 'route' }),
      }),
    );
  });
});

describe('delivery metrics', () => {
  it('counts retries and dead letters with the error type', async () => {
    const { bus } = createBus();
    bus.subscribe(
      OrderPlaced,
      async () => {
        throw new Error('payment provider down');
      },
      { id: 'billing' },
    );
    bus.subscribe(
      OrderShipped,
      async () => {
        throw new NonRetryableError('bad address');
      },
      { id: 'shipping' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.publish(new OrderShipped({ orderId: '1' }));
    await bus.drain();

    const attributes = (handler: string, type: string, error: string): Record<string, string> => ({
      'messaging.system': 'relaybus',
      'messaging.destination.name': type,
      'messaging.consumer.group.name': handler,
      'error.type': error,
    });
    expect(await points('relaybus.deliveries.retried')).toEqual([
      { attributes: attributes('billing', 'order.placed.v1', 'Error'), value: 1 },
    ]);
    const dead = await points('relaybus.deliveries.dead_lettered');
    expect(dead).toHaveLength(2);
    expect(dead).toContainEqual({
      attributes: attributes('billing', 'order.placed.v1', 'Error'),
      value: 1,
    });
    expect(dead).toContainEqual({
      attributes: attributes('shipping', 'order.shipped.v1', 'RELAYBUS_NON_RETRYABLE'),
      value: 1,
    });
    expect(await points('messaging.process.duration')).toContainEqual(
      expect.objectContaining({
        attributes: expect.objectContaining({ 'error.type': 'RELAYBUS_NON_RETRYABLE' }),
      }),
    );
  });

  it('counts lost leases and reported failures', async () => {
    const store = new InMemoryEventStore();
    vi.spyOn(store, 'complete').mockResolvedValue(false);
    const { bus } = createBus({ store });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    vi.spyOn(store, 'purge').mockRejectedValue(new Error('db down'));
    await expect(bus.purgeExpired()).rejects.toThrow('db down');

    const metrics = await telemetry.collect();
    expect(metrics.get('relaybus.lease.lost')!.points).toEqual([
      {
        attributes: {
          'messaging.system': 'relaybus',
          'messaging.destination.name': 'order.placed.v1',
          'messaging.consumer.group.name': 'billing',
        },
        value: 1,
      },
    ]);
    expect(metrics.get('relaybus.failures')!.points).toEqual([
      { attributes: { 'relaybus.failure.stage': 'purge', 'error.type': 'Error' }, value: 1 },
    ]);
  });

  it('reports the backlog gauges from the last reading only', async () => {
    const clock = new FakeClock('2026-01-01T00:00:00.000Z', { controlTimers: true });
    const { bus } = createBus({ clock, backlogInterval: '10s' });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.start();
    expect(await points('relaybus.deliveries.pending')).toEqual([]);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.publish(new OrderPlaced({ orderId: '2' }));
    clock.advance(10_000);
    await vi.waitFor(async () =>
      expect(await points('relaybus.deliveries.pending')).toEqual([{ attributes: {}, value: 2 }]),
    );
    expect(await points('relaybus.deliveries.oldest_age')).toEqual([{ attributes: {}, value: 10 }]);
    await bus.stop();
  });
});

describe('observer edge cases', () => {
  it('leaves out the destination of mixed publishes and of receives without a transport', async () => {
    telemetry = createTelemetry();
    const observer = telemetry.otel.observer;
    observer.onPublishFinished!({
      eventTypes: ['a.v1', 'b.v1'],
      durationMs: 5,
      outcome: 'error',
      error: {
        name: 'TransactionRequiredError',
        message: 'no',
        code: 'RELAYBUS_TRANSACTION_REQUIRED',
      },
    });
    observer.onReceiveFinished!({
      transport: undefined,
      system: undefined,
      destination: undefined,
      eventType: undefined,
      source: undefined,
      traceContext: undefined,
      outcome: 'invalid',
      durationMs: 1,
      error: { name: 'InvalidEnvelopeError', message: 'no', code: 'RELAYBUS_INVALID_ENVELOPE' },
    });
    observer.onClaimFinished!({
      handlers: ['billing'],
      requested: 10,
      claimed: 0,
      lanes: 0,
      durationMs: 2,
      outcome: 'error',
      error: { name: 'Error', message: 'db down' },
    });
    observer.onHandlerFinished!({
      delivery: {} as ClaimedDelivery,
      kind: 'route',
      durationMs: 1,
      outcome: 'success',
    });
    const metrics = await telemetry.collect();
    expect(metrics.get('messaging.client.operation.duration')!.points).toEqual([
      {
        attributes: {
          'messaging.system': 'relaybus',
          'messaging.operation.name': 'publish',
          'messaging.operation.type': 'create',
          'error.type': 'RELAYBUS_TRANSACTION_REQUIRED',
        },
        value: { count: 1, sum: 0.005 },
      },
      {
        attributes: {
          'messaging.system': 'relaybus',
          'messaging.operation.name': 'receive',
          'messaging.operation.type': 'receive',
          'error.type': 'RELAYBUS_INVALID_ENVELOPE',
        },
        value: { count: 1, sum: 0.001 },
      },
      {
        attributes: {
          'messaging.system': 'relaybus',
          'messaging.operation.name': 'claim',
          'messaging.operation.type': 'receive',
          'error.type': 'Error',
        },
        value: { count: 1, sum: 0.002 },
      },
    ]);
    expect(metrics.has('relaybus.claim.deliveries')).toBe(false);
    expect(metrics.has('messaging.process.duration')).toBe(false);
  });
});
