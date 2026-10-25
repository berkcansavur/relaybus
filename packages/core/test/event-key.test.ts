import { describe, expect, it } from 'vitest';
import {
  ConfigurationError,
  defineCatalog,
  defineEvent,
  type StandardSchemaV1,
} from '@relaybus/core';
import { createTestBus } from './helpers.js';

const orderSchema: StandardSchemaV1<{ orderId: string; total: number }> = {
  '~standard': {
    version: 1,
    vendor: 'test',
    validate: (value) => ({ value: value as { orderId: string; total: number } }),
  },
};

class OrderPlaced extends defineEvent('order.placed.v1', {
  schema: orderSchema,
  key: (payload) => payload.orderId,
}) {}
class PaymentCaptured extends defineEvent<{ paymentId: string; orderId: string }>(
  'payment.captured.v1',
  { key: (payload) => payload.orderId },
) {}
class Unkeyed extends defineEvent<{ note: string }>('note.added.v1') {}
class BadKey extends defineEvent<{ id: string }>('bad.key.v1', { key: (payload) => payload.id }) {}

const OrderEvents = defineCatalog({ OrderPlaced, PaymentCaptured }, { name: 'OrderEvents' });

function keyedBus(): ReturnType<typeof createTestBus> {
  const testBus = createTestBus({ transactionMode: 'optional' });
  for (const event of [OrderPlaced, PaymentCaptured, Unkeyed, BadKey]) {
    testBus.bus.subscribe(event as typeof Unkeyed, () => undefined, { id: `h.${event.type}` });
  }
  return testBus;
}

describe('event keys from the event definition', () => {
  it('exposes the key function on the class and leaves unkeyed events without one', () => {
    expect(OrderPlaced.key?.({ orderId: 'o-1', total: 1 })).toBe('o-1');
    expect(Unkeyed.key).toBeUndefined();
  });

  it('keys an event from its payload when publish passes no key', async () => {
    const { bus } = keyedBus();
    const placed = await bus.publish(new OrderPlaced({ orderId: 'o-1', total: 5 }));
    const unkeyed = await bus.publish(new Unkeyed({ note: 'n' }));
    expect(placed.key).toBe('o-1');
    expect(unkeyed.key).toBeNull();
  });

  it('lets an explicit key override the definition, also for every event of publishAll', async () => {
    const { bus } = keyedBus();
    const explicit = await bus.publish(new OrderPlaced({ orderId: 'o-1', total: 5 }), {
      key: 'tenant-7',
    });
    const each = await bus.publishAll([
      new OrderPlaced({ orderId: 'o-2', total: 1 }),
      new PaymentCaptured({ paymentId: 'p-1', orderId: 'o-3' }),
      new Unkeyed({ note: 'n' }),
    ]);
    const shared = await bus.publishAll(
      [new OrderPlaced({ orderId: 'o-4', total: 1 }), new Unkeyed({ note: 'n' })],
      { key: 'batch' },
    );
    expect(explicit.key).toBe('tenant-7');
    expect(each.map((envelope) => envelope.key)).toEqual(['o-2', 'o-3', null]);
    expect(shared.map((envelope) => envelope.key)).toEqual(['batch', 'batch']);
  });

  it('keys events published through a catalog view', async () => {
    const { bus } = keyedBus();
    const orders = bus.forCatalog(OrderEvents);
    const envelope = await orders.publish(new PaymentCaptured({ paymentId: 'p', orderId: 'o-9' }));
    expect(envelope.key).toBe('o-9');
  });

  it.each([
    ['an empty string', ''],
    ['a value that is not a string', 42],
  ])('rejects a key function that returns %s', async (_label, id) => {
    const { bus } = keyedBus();
    const publishing = bus.publish(new BadKey({ id: id as string }));
    await expect(publishing).rejects.toThrow(ConfigurationError);
    await expect(bus.publish(new BadKey({ id: id as string }))).rejects.toThrow(
      "The key function of 'bad.key.v1' must return a non-empty string",
    );
  });
});
