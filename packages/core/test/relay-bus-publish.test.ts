import { describe, expect, it, vi } from 'vitest';
import {
  ConfigurationError,
  EventValidationError,
  InMemoryEventStore,
  RelayBus,
  TransactionRequiredError,
  defineEvent,
  type EventEnvelope,
  type HandlerContext,
  type StandardSchemaV1,
  type Transport,
} from '@relaybus/core';
import { InMemoryTransport } from '@relaybus/testing';
import { OrderPlaced, OrderShipped, createTestBus } from './helpers.js';

const publishOnlyTransport: Transport = {
  name: 'publish-only',
  publish: async () => undefined,
  close: async () => undefined,
};

describe('RelayBus configuration', () => {
  const store = new InMemoryEventStore();

  it.each([
    [{ transactionMode: 'sometimes' }, /transactionMode must be/],
    [{ handlerTimeout: 0 }, /handlerTimeout must be greater than zero/],
    [{ dispatcher: { concurrency: 0 } }, /dispatcher.concurrency must be a positive integer/],
    [{ dispatcher: { batchSize: 1.5 } }, /dispatcher.batchSize must be a positive integer/],
    [{ dispatcher: { pollInterval: 0 } }, /dispatcher.pollInterval must be greater than zero/],
    [{ dispatcher: { leaseDuration: '0s' } }, /dispatcher.leaseDuration must be greater than zero/],
    [{ retention: { interval: 0 } }, /retention.interval must be greater than zero/],
    [
      { transports: [new InMemoryTransport('a'), new InMemoryTransport('a')] },
      /Transport name 'a' is used twice/,
    ],
    [{ routes: [{ event: OrderPlaced, transport: 'missing' }] }, /No transport named 'missing'/],
    [
      { subscriptions: [{ transport: 'missing', destinations: ['x'] }] },
      /No transport named 'missing'/,
    ],
    [
      {
        transports: [publishOnlyTransport],
        subscriptions: [{ transport: 'publish-only', destinations: ['x'] }],
      },
      /Transport 'publish-only' cannot subscribe/,
    ],
  ])('rejects %j', (options, message) => {
    expect(() => new RelayBus({ store, ...(options as object) })).toThrow(message);
  });

  it('exposes the store and running state', () => {
    const { bus, store: testStore } = createTestBus();
    expect(bus.store).toBe(testStore);
    expect(bus.isRunning).toBe(false);
  });

  it('validates subscriptions', () => {
    const { bus } = createTestBus();
    bus.subscribe(OrderPlaced, () => undefined, { id: 'orders.audit' });
    expect(() => bus.subscribe(OrderPlaced, () => undefined, { id: 'orders.audit' })).toThrow(
      /already registered/,
    );
    expect(() => bus.subscribe(OrderPlaced, () => undefined, { id: 'bad id' })).toThrow(
      ConfigurationError,
    );
    expect(() => bus.subscribe(OrderPlaced, () => undefined, { id: 'c', concurrency: 0 })).toThrow(
      /Concurrency of handler 'c' must be a positive integer/,
    );
    expect(() => bus.subscribe(OrderPlaced, () => undefined, { id: 't', timeout: 0 })).toThrow(
      /Timeout of handler 't' must be greater than zero/,
    );
    expect(() => bus.subscribe('not valid', () => undefined, { id: 's' })).toThrow(
      ConfigurationError,
    );
  });

  it('applies bus defaults and per-handler overrides', () => {
    const { bus } = createTestBus({ handlerTimeout: '5s', retry: { attempts: 4 } });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'default' });
    bus.subscribe(OrderPlaced, () => undefined, {
      id: 'custom',
      timeout: false,
      retry: { attempts: 2 },
      concurrency: 3,
      transactional: true,
    });
    bus.subscribe('order.placed.v1', () => undefined, { id: 'by-type', timeout: '1s' });
    const [byDefault, custom, byType] = bus.handlers();
    expect(byDefault).toMatchObject({ timeoutMs: 5_000, concurrency: null, transactional: false });
    expect(byDefault!.retry.attempts).toBe(4);
    expect(custom).toMatchObject({ timeoutMs: null, concurrency: 3, transactional: true });
    expect(custom!.retry.attempts).toBe(2);
    expect(byType).toMatchObject({ eventType: 'order.placed.v1', timeoutMs: 1_000 });
  });
});

describe('RelayBus.publish', () => {
  it('requires a transaction by default', async () => {
    const { bus } = createTestBus();
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await expect(bus.publish(new OrderPlaced({ orderId: '1' }))).rejects.toThrow(
      TransactionRequiredError,
    );
    expect(await bus.listDeliveries()).toEqual([]);
  });

  it('writes into an explicit transaction, committed with it', async () => {
    const { bus, store } = createTestBus();
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await expect(
      store.transaction(async (transaction) => {
        await bus.publish(new OrderPlaced({ orderId: '1' }), { transaction });
        throw new Error('business write failed');
      }),
    ).rejects.toThrow('business write failed');
    expect(await bus.listDeliveries()).toEqual([]);
    await store.transaction((transaction) =>
      bus.publish(new OrderPlaced({ orderId: '2' }), { transaction }),
    );
    expect(await bus.listDeliveries()).toHaveLength(1);
  });

  it('joins the transaction opened with bus.transaction', async () => {
    const { bus } = createTestBus();
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    const result = await bus.transaction(async () => {
      await bus.publish(new OrderPlaced({ orderId: '1' }));
      return 'done';
    });
    expect(result).toBe('done');
    expect(await bus.listDeliveries()).toHaveLength(1);
  });

  it('asks the transaction resolver', async () => {
    const store = new InMemoryEventStore();
    let active: Parameters<Parameters<typeof store.transaction>[0]>[0] | undefined;
    const { bus } = createTestBus({ store, transactionResolver: () => active });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await store.transaction(async (transaction) => {
      active = transaction;
      await bus.publish(new OrderPlaced({ orderId: '1' }));
      active = undefined;
    });
    expect(await bus.listDeliveries()).toHaveLength(1);
    await expect(bus.publish(new OrderPlaced({ orderId: '2' }))).rejects.toThrow(
      TransactionRequiredError,
    );
  });

  it('opens its own transaction in optional mode', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }));
    expect(await bus.listDeliveries()).toHaveLength(1);
  });

  it('builds the envelope', async () => {
    const { bus, clock, observer } = createTestBus({
      source: 'orders-service',
      transactionMode: 'optional',
    });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    const envelope = await bus.publish(new OrderPlaced({ orderId: '1' }), {
      key: 'order-1',
      headers: { traceparent: 'tp' },
      delay: '5m',
    });
    expect(envelope).toEqual({
      id: expect.stringMatching(/^[0-9a-f-]{36}$/),
      type: 'order.placed.v1',
      source: 'orders-service',
      key: 'order-1',
      payload: { orderId: '1' },
      headers: { traceparent: 'tp' },
      correlationId: envelope.id,
      causationId: null,
      occurredAt: clock.now(),
    });
    const [delivery] = await bus.listDeliveries();
    expect(delivery!.availableAt).toEqual(new Date(clock.now().getTime() + 300_000));
    expect(observer.onPublished).toHaveBeenCalledWith({ envelopes: [envelope], deliveries: 1 });
    const correlated = await bus.publish(new OrderPlaced({ orderId: '2' }), {
      correlationId: 'flow-9',
    });
    expect(correlated.correlationId).toBe('flow-9');
    expect(correlated.source).toBe('orders-service');
  });

  it('defaults the source', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    expect((await bus.publish(new OrderPlaced({ orderId: '1' }))).source).toBe('relaybus');
  });

  it('is idempotent on a caller-supplied id', async () => {
    const { bus, observer } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'h' });
    await bus.publish(new OrderPlaced({ orderId: '1' }), { id: 'order-1-placed' });
    await bus.publish(new OrderPlaced({ orderId: '1' }), { id: 'order-1-placed' });
    expect(await bus.listDeliveries()).toHaveLength(1);
    expect(observer.onPublished).toHaveBeenCalledTimes(1);
  });

  it('stores several events atomically and in order', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(OrderPlaced, () => undefined, { id: 'placed' });
    bus.subscribe(OrderShipped, () => undefined, { id: 'shipped' });
    const envelopes = await bus.publishAll(
      [new OrderPlaced({ orderId: '1' }), new OrderShipped({ orderId: '1' })],
      { key: 'order-1' },
    );
    expect(envelopes.map((envelope) => envelope.type)).toEqual([
      'order.placed.v1',
      'order.shipped.v1',
    ]);
    expect(await bus.listDeliveries()).toHaveLength(2);
    expect(await bus.publishAll([])).toEqual([]);
  });

  it('warns and stores nothing when an event has no handler or route', async () => {
    const { bus, logger, observer } = createTestBus();
    const envelope = await bus.publish(new OrderPlaced({ orderId: '1' }));
    expect(envelope.type).toBe('order.placed.v1');
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('[RelayBus](reportUnrouted) No handler or route for event type'),
    );
    expect(observer.onPublished).not.toHaveBeenCalled();
    expect(await bus.listDeliveries()).toEqual([]);
  });

  it('rejects values that are not defined events', async () => {
    const { bus } = createTestBus({ transactionMode: 'optional' });
    await expect(bus.publish({ type: 1, payload: {} } as never)).rejects.toThrow(
      /instance of a class created with defineEvent/,
    );
    await expect(bus.publish({ type: 'bad type', payload: {} })).rejects.toThrow(
      ConfigurationError,
    );
    await expect(bus.publish(new OrderPlaced({ orderId: '1' }), { key: '' })).rejects.toThrow(
      /key must not be empty/,
    );
    await expect(
      bus.publish(new OrderPlaced({ orderId: '1' }), { delay: '-1s' as never }),
    ).rejects.toThrow(/delay must look like/);
  });

  it('validates the payload against the event schema before storing', async () => {
    const schema: StandardSchemaV1<unknown, { amount: number }> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (value) =>
          typeof (value as { amount?: unknown }).amount === 'number'
            ? { value: { amount: (value as { amount: number }).amount } }
            : { issues: [{ message: 'amount must be a number', path: ['amount'] }] },
      },
    };
    class PaymentTaken extends defineEvent('payment.taken.v1', { schema }) {}
    const { bus } = createTestBus({ transactionMode: 'optional' });
    bus.subscribe(PaymentTaken, () => undefined, { id: 'h' });
    await expect(bus.publish(new PaymentTaken({ amount: 'ten' } as never))).rejects.toThrow(
      EventValidationError,
    );
    const stored = await bus.publish(new PaymentTaken({ amount: 10, extra: true } as never));
    expect(stored.payload).toEqual({ amount: 10 });
  });
});

describe('RelayBus handler invocation', () => {
  it('runs every handler after commit with the envelope and context', async () => {
    const { bus, observer } = createTestBus({ transactionMode: 'optional' });
    const calls: Array<{ event: EventEnvelope; context: HandlerContext }> = [];
    bus.subscribe(OrderPlaced, (event, context) => void calls.push({ event, context }), {
      id: 'first',
      retry: { attempts: 3 },
    });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'second' });
    const envelope = await bus.publish(new OrderPlaced({ orderId: '1' }));
    expect(await bus.runOnce()).toBe(2);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.event).toEqual(envelope);
    expect(calls[0]!.context).toMatchObject({
      handler: 'first',
      attempt: 1,
      maxAttempts: 3,
      transaction: undefined,
      deliveryId: expect.any(String),
    });
    expect(calls[0]!.context.signal.aborted).toBe(false);
    expect(observer.onDeliveryStarted).toHaveBeenCalledTimes(2);
    expect(observer.onDeliverySucceeded).toHaveBeenCalledWith(
      expect.objectContaining({ durationMs: 0 }),
    );
    const records = await bus.listDeliveries();
    expect(records.every((record) => record.status === 'completed')).toBe(true);
    expect(await bus.runOnce()).toBe(0);
  });

  it('propagates correlation to events published while handling', async () => {
    const { bus } = createTestBus();
    let child: EventEnvelope | undefined;
    bus.subscribe(
      OrderPlaced,
      async (event) => {
        child = await bus.publish(new OrderShipped({ orderId: event.payload.orderId }));
      },
      { id: 'ship', transactional: true },
    );
    bus.subscribe(OrderShipped, () => undefined, { id: 'notify' });
    const parent = await bus.transaction(() =>
      bus.publish(new OrderPlaced({ orderId: '1' }), { correlationId: 'flow-1' }),
    );
    await bus.drain();
    expect(child).toMatchObject({ causationId: parent.id, correlationId: 'flow-1' });
    const notify = await bus.listDeliveries({ handler: 'notify' });
    expect(notify[0]!.status).toBe('completed');
  });

  it('uses the parent id as correlation when the parent has none', async () => {
    const { bus } = createTestBus();
    let child: EventEnvelope | undefined;
    bus.subscribe(
      OrderPlaced,
      async (event) => {
        child = await bus.publish(new OrderShipped({ orderId: event.payload.orderId }));
      },
      { id: 'ship', transactional: true },
    );
    bus.subscribe(OrderShipped, () => undefined, { id: 'notify' });
    const store = bus.store as InMemoryEventStore;
    const parentId = '0190a000-0000-7000-8000-000000000001';
    await bus.receive({
      id: parentId,
      type: 'order.placed.v1',
      source: 'remote',
      key: null,
      payload: { orderId: '1' },
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date(),
    });
    await bus.runOnce();
    expect(store).toBeDefined();
    expect(child).toMatchObject({ causationId: parentId, correlationId: parentId });
  });

  it('rolls back events published by a failing transactional handler', async () => {
    const { bus } = createTestBus({ retry: { attempts: 1 } });
    bus.subscribe(
      OrderPlaced,
      async (_event, context) => {
        expect(context.transaction).toBeDefined();
        await bus.publish(new OrderShipped({ orderId: '1' }));
        throw new Error('after publish');
      },
      { id: 'ship', transactional: true },
    );
    bus.subscribe(OrderShipped, () => undefined, { id: 'notify' });
    await bus.transaction(() => bus.publish(new OrderPlaced({ orderId: '1' })));
    await bus.runOnce();
    expect(await bus.listDeliveries({ handler: 'notify' })).toEqual([]);
    expect((await bus.listDeliveries({ handler: 'ship' }))[0]!.status).toBe('dead');
  });

  it('does not let a non-transactional handler publish without a transaction', async () => {
    const { bus } = createTestBus({ retry: { attempts: 1 } });
    bus.subscribe(OrderPlaced, () => bus.publish(new OrderShipped({ orderId: '1' })), {
      id: 'ship',
    });
    bus.subscribe(OrderShipped, () => undefined, { id: 'notify' });
    await bus.transaction(() => bus.publish(new OrderPlaced({ orderId: '1' })));
    await bus.runOnce();
    const [record] = await bus.listDeliveries({ handler: 'ship' });
    expect(record!.lastError).toMatchObject({ name: 'TransactionRequiredError' });
  });

  it('hands the schema output to handlers and dead-letters payloads that no longer validate', async () => {
    let strict = false;
    const schema: StandardSchemaV1<unknown, { orderId: string; parsed: true }> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (value) =>
          strict
            ? { issues: [{ message: 'schema changed' }] }
            : { value: { ...(value as { orderId: string }), parsed: true } },
      },
    };
    class Versioned extends defineEvent('versioned.v1', { schema }) {}
    const { bus, observer } = createTestBus({ transactionMode: 'optional' });
    const received = vi.fn();
    bus.subscribe(Versioned, (event) => received(event.payload), { id: 'h' });
    await bus.publish(new Versioned({ orderId: '1', parsed: true }));
    await bus.runOnce();
    expect(received).toHaveBeenCalledWith({ orderId: '1', parsed: true });
    await bus.publish(new Versioned({ orderId: '2', parsed: true }));
    strict = true;
    await bus.runOnce();
    expect(observer.onDeliveryDead).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ name: 'EventValidationError' }) }),
    );
  });
});
