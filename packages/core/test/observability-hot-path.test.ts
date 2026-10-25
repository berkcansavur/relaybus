import { describe, expect, it } from 'vitest';
import {
  InMemoryEventStore,
  RelayBus,
  type Clock,
  type EventEnvelope,
  type InMemoryTransaction,
  type Transport,
} from '@relaybus/core';
import { OrderPlaced, createLogger } from './helpers.js';

/** A clock that counts how often the bus reads the time: each read is a call on the hot path. */
function countingClock(): Clock & { reads: number } {
  const clock = {
    reads: 0,
    now(): Date {
      clock.reads += 1;
      return new Date('2026-01-01T00:00:00.000Z');
    },
  };
  return clock;
}

function recordingTransport(): Transport & { sent: EventEnvelope[] } {
  const sent: EventEnvelope[] = [];
  return {
    name: 'broker',
    sent,
    publish: async (envelope) => {
      sent.push(envelope);
    },
    close: async () => undefined,
  };
}

const EVENTS = 10;

/**
 * Design test: observability costs nothing when nobody observes. Without observers and a tracer,
 * publishing and dispatching read the clock exactly as often as before the hooks existed, and a
 * route sends the event with the headers it was published with.
 */
describe('hot path without observers', () => {
  it('reads the clock no more often than before the observability hooks', async () => {
    const clock = countingClock();
    const transport = recordingTransport();
    const store = new InMemoryEventStore();
    const bus = new RelayBus<InMemoryTransaction>({
      store,
      clock,
      logger: createLogger(),
      transactionMode: 'optional',
      transports: [transport],
      routes: [{ event: OrderPlaced, transport: 'broker' }],
    });
    bus.subscribe(OrderPlaced, async () => undefined, { id: 'billing' });

    for (let index = 0; index < EVENTS; index += 1) {
      await bus.publish(new OrderPlaced({ orderId: `o-${index}` }));
    }
    const afterPublish = clock.reads;
    await bus.drain();

    expect({ publish: afterPublish, dispatch: clock.reads - afterPublish }).toEqual({
      publish: EVENTS,
      // One claim read per claim round plus the reads of 2 * EVENTS deliveries (start, completion).
      dispatch: 45,
    });
    // Nothing traced: the events go out with the headers they were published with.
    expect(transport.sent).toHaveLength(EVENTS);
    expect(transport.sent.map((envelope) => envelope.headers)).toEqual(
      Array.from({ length: EVENTS }, () => ({})),
    );
  });

  it('receives without observation', async () => {
    const bus = new RelayBus({ store: new InMemoryEventStore(), logger: createLogger() });
    const envelope: EventEnvelope = {
      id: 'evt-1',
      type: 'order.placed.v1',
      source: 'orders',
      key: null,
      payload: {},
      headers: {},
      correlationId: null,
      causationId: null,
      occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    };
    expect(await bus.receive(envelope)).toBe('ignored');
  });
});
