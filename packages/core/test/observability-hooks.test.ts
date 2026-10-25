import { describe, expect, it, vi } from 'vitest';
import {
  InMemoryEventStore,
  InvalidEnvelopeError,
  type ClaimRequest,
  type ClaimedDelivery,
  type EventEnvelope,
  type EventStoreStatistics,
  type RelayBusObserver,
  type RelayBusTracer,
} from '@relaybus/core';
import { FakeClock, InMemoryTransport } from '@relaybus/testing';
import { OrderPlaced, OrderShipped, createTestBus, deferred } from './helpers.js';

const TRACEPARENT = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';

function remoteEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: '0190a000-0000-7000-8000-0000000000aa',
    type: 'order.placed.v1',
    source: 'orders',
    key: 'order-1',
    payload: { orderId: '1' },
    headers: { traceparent: TRACEPARENT },
    correlationId: null,
    causationId: null,
    occurredAt: new Date('2025-12-31T23:59:59.000Z'),
    ...overrides,
  };
}

function orderIdOf(envelope: EventEnvelope): string {
  return (envelope.payload as { orderId: string }).orderId;
}

/** A tracer that records what it wraps and hands out fixed trace headers. */
function recordingTracer(): Required<RelayBusTracer> & { calls: string[] } {
  const calls: string[] = [];
  return {
    calls,
    publish: async (input, run) => {
      calls.push(`publish ${input.eventTypes.join(',')}`);
      return run({ traceparent: TRACEPARENT, tracestate: 'vendor=publish' });
    },
    send: async (input, run) => {
      calls.push(`send ${input.transport}/${input.system}/${input.destination}`);
      return run({ traceparent: TRACEPARENT.replace('b7ad', 'cccc') });
    },
    process: async (input, run) => {
      calls.push(`process ${input.delivery.handler}`);
      return run();
    },
  };
}

describe('publish hooks', () => {
  it('reports the event types, duration and outcome of every publish call', async () => {
    const { bus, observer, clock } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    bus.subscribe(OrderShipped, async () => undefined, { id: 'shipping' });
    await bus.publishAll([new OrderPlaced({ orderId: '1' }), new OrderShipped({ orderId: '1' })]);
    expect(observer.onPublishFinished).toHaveBeenCalledWith({
      eventTypes: ['order.placed.v1', 'order.shipped.v1'],
      durationMs: 0,
      outcome: 'success',
    });
    clock.advance(1);
    await expect(bus.publish({ type: 42 } as never)).rejects.toThrow('defineEvent');
    expect(observer.onPublishFinished).toHaveBeenLastCalledWith({
      eventTypes: ['42'],
      durationMs: 0,
      outcome: 'error',
      error: expect.objectContaining({ code: 'RELAYBUS_CONFIGURATION' }),
    });
  });

  it('writes the tracer headers into every event, keeping headers passed explicitly', async () => {
    const tracer = recordingTracer();
    const { bus, store } = createTestBus({ transactionMode: 'optional', tracer });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }), { headers: { tenant: 't-1' } });
    await bus.publish(new OrderPlaced({ orderId: '2' }), { headers: { traceparent: 'mine' } });
    const headers = (await store.listDeliveries({}))
      .sort((left, right) => orderIdOf(left.event).localeCompare(orderIdOf(right.event)))
      .map((record) => record.event.headers);
    expect(headers).toEqual([
      { traceparent: TRACEPARENT, tracestate: 'vendor=publish', tenant: 't-1' },
      { traceparent: 'mine', tracestate: 'vendor=publish' },
    ]);
    expect(tracer.calls).toEqual(['publish order.placed.v1', 'publish order.placed.v1']);
  });

  it('publishes untouched through a tracer without a publish hook', async () => {
    const { bus, store, observer } = createTestBus({
      transactionMode: 'optional',
      tracer: {},
      observers: [],
    });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }), { headers: { tenant: 't-1' } });
    expect((await store.listDeliveries({}))[0]!.event.headers).toEqual({ tenant: 't-1' });
    expect(observer.onPublishFinished).not.toHaveBeenCalled();
  });
});

describe('claim hooks', () => {
  it('reports each claim with its size, lanes and duration', async () => {
    const { bus, observer } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }), { key: 'a' });
    await bus.publish(new OrderPlaced({ orderId: '2' }), { key: 'b' });
    await bus.runOnce();
    expect(observer.onClaimFinished).toHaveBeenCalledWith({
      handlers: ['billing'],
      requested: 10,
      claimed: 2,
      lanes: 2,
      durationMs: 0,
      outcome: 'success',
    });
  });

  it('reports a failed claim and lets the error through', async () => {
    const store = new InMemoryEventStore();
    vi.spyOn(store, 'claim').mockRejectedValue(new Error('db down'));
    const { bus, observer } = createTestBus({ store });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await expect(bus.runOnce()).rejects.toThrow('db down');
    expect(observer.onClaimFinished).toHaveBeenCalledWith(
      expect.objectContaining({ claimed: 0, lanes: 0, outcome: 'error' }),
    );
  });

  it('claims without timing when only the handler hooks are observed', async () => {
    const onHandlerStarted = vi.fn();
    const { bus } = createTestBus({
      transactionMode: 'optional',
      observers: [{ onHandlerStarted }],
    });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    expect(await bus.runOnce()).toBe(1);
    expect(onHandlerStarted).toHaveBeenCalledTimes(1);
  });
});

describe('handler hooks', () => {
  it('reports start with end-to-end latency and success with duration', async () => {
    const { bus, observer, clock } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(
      OrderPlaced,
      async () => {
        clock.advance(7);
      },
      { id: 'billing' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    clock.advance(40);
    await bus.runOnce();
    const delivery = expect.objectContaining({ handler: 'billing', attempt: 1 });
    expect(observer.onHandlerStarted).toHaveBeenCalledWith({
      delivery,
      kind: 'handler',
      endToEndLatencyMs: 40,
    });
    expect(observer.onHandlerFinished).toHaveBeenCalledWith({
      delivery,
      kind: 'handler',
      durationMs: 7,
      outcome: 'success',
    });
  });

  it('reports a failing handler with its error code', async () => {
    const { bus, observer } = createTestBus({ transactionMode: 'optional', handlerTimeout: '1s' });
    bus.subscribe(
      OrderPlaced,
      async () => {
        throw new Error('boom');
      },
      { id: 'billing' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(observer.onHandlerFinished).toHaveBeenCalledWith(
      expect.objectContaining({
        outcome: 'error',
        error: expect.objectContaining({ message: 'boom' }),
      }),
    );
    expect(observer.onDeliveryRetryScheduled).toHaveBeenCalledTimes(1);
  });

  it('wraps local handlers in the process hook and routes in the send hook', async () => {
    const tracer = recordingTracer();
    const transport = new InMemoryTransport('broker');
    const { bus } = createTestBus({
      transactionMode: 'optional',
      tracer,
      transports: [transport],
      routes: [{ event: OrderPlaced, transport: 'broker', destination: 'orders' }],
    });
    const seen = vi.fn();
    bus.subscribe(OrderPlaced, seen, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.drain();
    // Both deliveries run in one batch, in either order.
    expect(tracer.calls[0]).toBe('publish order.placed.v1');
    expect(tracer.calls.slice(1).sort()).toEqual(['process billing', 'send broker/broker/orders']);
    // The stored event keeps the publish context; the wire copy carries the send context.
    expect(seen.mock.calls[0]![0].headers.traceparent).toBe(TRACEPARENT);
    expect(transport.published[0]!.envelope.headers).toEqual({
      traceparent: TRACEPARENT.replace('b7ad', 'cccc'),
      tracestate: 'vendor=publish',
    });
  });

  it('runs handlers through a process-only tracer without other observers', async () => {
    let processed = 0;
    const tracer: RelayBusTracer = {
      process: (_input, run) => {
        processed += 1;
        return run();
      },
    };
    const { bus } = createTestBus({
      transactionMode: 'optional',
      tracer,
      observers: [],
    });
    const handled = vi.fn();
    bus.subscribe(OrderPlaced, handled, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(processed).toBe(1);
    expect(handled).toHaveBeenCalledTimes(1);
  });

  it('passes a handler failure through a tracer whose runs are not observed', async () => {
    const onClaimFinished = vi.fn();
    const { bus, store } = createTestBus({
      transactionMode: 'optional',
      tracer: { process: async (_input, run) => run() },
      observers: [{ onClaimFinished }],
    });
    bus.subscribe(
      OrderPlaced,
      () => {
        throw new Error('boom');
      },
      { id: 'billing' },
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect((await store.listDeliveries({}))[0]!.lastError?.message).toBe('boom');
    expect(onClaimFinished).toHaveBeenCalledTimes(1);
  });
});

describe('transport hooks', () => {
  it('reports each send with its system, destination, duration and outcome', async () => {
    const transport = new InMemoryTransport('broker');
    const kafka = Object.assign(new InMemoryTransport('events'), { system: 'kafka' });
    const { bus, observer } = createTestBus({
      transactionMode: 'optional',
      transports: [transport, kafka],
      routes: [
        { event: OrderPlaced, transport: 'broker', destination: 'orders' },
        { event: OrderPlaced, transport: 'events', destination: 'orders-topic' },
      ],
    });
    transport.failNextPublishes(1);
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.drain();
    expect(observer.onTransportSent).toHaveBeenCalledWith(
      expect.objectContaining({
        transport: 'broker',
        system: 'broker',
        destination: 'orders',
        outcome: 'error',
        error: expect.objectContaining({ message: 'InMemoryTransport refused publish to orders' }),
      }),
    );
    expect(observer.onTransportSent).toHaveBeenCalledWith(
      expect.objectContaining({
        transport: 'events',
        system: 'kafka',
        destination: 'orders-topic',
        durationMs: 0,
        outcome: 'success',
      }),
    );
  });

  it('sends through a send-only tracer without reporting', async () => {
    const transport = new InMemoryTransport('broker');
    let sent = 0;
    const tracer: RelayBusTracer = {
      send: (_input, run) => {
        sent += 1;
        return run({});
      },
    };
    const { bus } = createTestBus({
      transactionMode: 'optional',
      tracer,
      observers: [],
      transports: [transport],
      routes: [{ event: OrderPlaced, transport: 'broker' }],
    });
    await bus.publish(new OrderPlaced({ orderId: '1' }), { headers: { tenant: 't' } });
    await bus.drain();
    expect(sent).toBe(1);
    expect(transport.published[0]!.envelope.headers).toEqual({ tenant: 't' });
  });

  it('sends directly when only other hooks are observed', async () => {
    const transport = new InMemoryTransport('broker');
    const publish = vi.spyOn(transport, 'publish');
    const { bus } = createTestBus({
      transactionMode: 'optional',
      observers: [{ onReceiveFinished: vi.fn() }],
      transports: [transport],
      routes: [{ event: OrderPlaced, transport: 'broker' }],
    });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.drain();
    expect(publish).toHaveBeenCalledWith(expect.anything(), 'order.placed.v1');
  });
});

describe('receive hooks', () => {
  it('reports the transport, destination, source and trace context of a received event', async () => {
    const transport = new InMemoryTransport('broker');
    const { bus, observer } = createTestBus({
      transports: [transport],
      subscriptions: [{ transport: 'broker', destinations: ['orders'] }],
    });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.start();
    await transport.deliver('orders', remoteEnvelope());
    await transport.deliver('orders', remoteEnvelope());
    await bus.stop();
    const received = {
      transport: 'broker',
      system: 'broker',
      destination: 'orders',
      eventType: 'order.placed.v1',
      source: 'orders',
      traceContext: { traceparent: TRACEPARENT },
      durationMs: 0,
    };
    expect(observer.onReceiveFinished).toHaveBeenNthCalledWith(1, {
      ...received,
      outcome: 'accepted',
    });
    expect(observer.onReceiveFinished).toHaveBeenNthCalledWith(2, {
      ...received,
      outcome: 'duplicate',
    });
  });

  it('reports events received directly as ignored, invalid or failed', async () => {
    const store = new InMemoryEventStore();
    const { bus, observer } = createTestBus({ store });
    expect(await bus.receive(remoteEnvelope({ type: 'unknown.v1' }))).toBe('ignored');
    await expect(
      bus.receive({ id: 'x', type: 7 } as unknown as EventEnvelope),
    ).rejects.toBeInstanceOf(InvalidEnvelopeError);
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    vi.spyOn(store, 'transaction').mockRejectedValueOnce(new Error('db down'));
    await expect(bus.receive(remoteEnvelope({ headers: {} }))).rejects.toThrow('db down');
    const calls = vi.mocked(observer.onReceiveFinished).mock.calls.map(([event]) => event);
    expect(calls.map((event) => [event.outcome, event.transport, event.destination])).toEqual([
      ['ignored', undefined, undefined],
      ['invalid', undefined, undefined],
      ['error', undefined, undefined],
    ]);
    expect(calls[1]).toMatchObject({
      eventType: undefined,
      source: undefined,
      traceContext: undefined,
      error: expect.objectContaining({ code: 'RELAYBUS_INVALID_ENVELOPE' }),
    });
    expect(calls[2]).toMatchObject({ traceContext: undefined, error: { message: 'db down' } });
  });

  it('receives without reporting when only other hooks are observed', async () => {
    const onBacklog = vi.fn();
    const { bus } = createTestBus({ observers: [{ onBacklog }] });
    expect(await bus.receive(remoteEnvelope())).toBe('ignored');
    expect(onBacklog).not.toHaveBeenCalled();
  });
});

describe('backlog hook', () => {
  function backlogBus(
    observers: readonly RelayBusObserver[],
    store = new InMemoryEventStore(),
  ): ReturnType<typeof createTestBus> & { clock: FakeClock } {
    const clock = new FakeClock('2026-01-01T00:00:00.000Z', { controlTimers: true });
    const test = createTestBus({
      store,
      clock,
      observers,
      backlogInterval: '10s',
      transactionMode: 'optional',
      dispatcher: { enabled: false },
    });
    return { ...test, clock };
  }

  it('reports pending deliveries and the age of the oldest due one every interval', async () => {
    const onBacklog = vi.fn();
    const { bus, clock } = backlogBus([{ onBacklog }]);
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.start();
    clock.advance(10_000);
    await vi.waitFor(() => expect(onBacklog).toHaveBeenCalledTimes(1));
    expect(onBacklog).toHaveBeenLastCalledWith(
      expect.objectContaining({ pending: 0, oldestPendingAgeMs: 0 }),
    );
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    clock.advance(10_000);
    await vi.waitFor(() => expect(onBacklog).toHaveBeenCalledTimes(2));
    expect(onBacklog).toHaveBeenLastCalledWith({
      pending: 1,
      oldestPendingAgeMs: 10_000,
      statistics: expect.objectContaining({ pending: 1 }),
    });
    await bus.stop();
    clock.advance(10_000);
    expect(onBacklog).toHaveBeenCalledTimes(2);
  });

  it('reports a failed reading as a backlog failure and never overlaps readings', async () => {
    const onBacklog = vi.fn();
    const onFailure = vi.fn();
    const store = new InMemoryEventStore();
    const reading = deferred<EventStoreStatistics>();
    const statistics = vi
      .spyOn(store, 'statistics')
      .mockReturnValueOnce(reading.promise)
      .mockRejectedValueOnce(new Error('db down'));
    const { bus, clock } = backlogBus([{ onBacklog, onFailure }], store);
    await bus.start();
    clock.advance(10_000);
    clock.advance(10_000);
    expect(statistics).toHaveBeenCalledTimes(1);
    reading.reject(new Error('db down'));
    await vi.waitFor(() =>
      expect(onFailure).toHaveBeenCalledWith({
        stage: 'backlog',
        error: expect.objectContaining({ message: 'db down' }),
      }),
    );
    clock.advance(10_000);
    await vi.waitFor(() => expect(onFailure).toHaveBeenCalledTimes(2));
    expect(onBacklog).not.toHaveBeenCalled();
    await bus.stop();
  });

  it('reads nothing when no observer wants the backlog', async () => {
    const store = new InMemoryEventStore();
    const statistics = vi.spyOn(store, 'statistics');
    for (const observers of [[], [{ onPublishFinished: vi.fn() }]]) {
      const { bus, clock } = backlogBus(observers, store);
      await bus.start();
      clock.advance(60_000);
      await bus.stop();
    }
    expect(statistics).not.toHaveBeenCalled();
  });

  it('rejects a zero interval', () => {
    expect(() => createTestBus({ backlogInterval: 0 })).toThrow(
      'backlogInterval must be greater than zero',
    );
  });
});

describe('observer discovery', () => {
  it('finds hooks on class instances and ignores hooks that are not functions', async () => {
    class MetricsObserver implements RelayBusObserver {
      readonly published: string[][] = [];
      onPublishFinished(event: { readonly eventTypes: readonly string[] }): void {
        this.published.push([...event.eventTypes]);
      }
    }
    const metrics = new MetricsObserver();
    const disabled = { onPublishFinished: undefined, onClaimFinished: 'nope' };
    const claim = vi.fn(async (_request: ClaimRequest): Promise<ClaimedDelivery[]> => []);
    const store = Object.assign(new InMemoryEventStore(), { claim });
    const { bus } = createTestBus({
      store,
      transactionMode: 'optional',
      observers: [metrics, disabled as unknown as RelayBusObserver],
    });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    await bus.runOnce();
    expect(metrics.published).toEqual([['order.placed.v1']]);
    expect(claim).toHaveBeenCalledTimes(1);
  });
});
