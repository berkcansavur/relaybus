import type { EventEnvelope, Transport, TransportMessageHandler } from '@relaybus/core';

export interface PublishedMessage {
  readonly destination: string;
  readonly envelope: EventEnvelope;
}

/**
 * Transport that keeps messages in memory. `deliver` pushes a message to the
 * subscribers of its destination and, like a broker, redelivers it on failure
 * up to `maxRedeliveries` times.
 */
export class InMemoryTransport implements Transport {
  readonly name: string;
  readonly published: PublishedMessage[] = [];
  readonly #subscribers = new Map<string, TransportMessageHandler[]>();
  #failNextPublishes = 0;
  connected = false;
  closed = false;

  constructor(name = 'memory') {
    this.name = name;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  /** Makes the next `count` publishes reject, simulating a broker outage. */
  failNextPublishes(count: number): void {
    this.#failNextPublishes = count;
  }

  async publish(envelope: EventEnvelope, destination: string): Promise<void> {
    if (this.#failNextPublishes > 0) {
      this.#failNextPublishes -= 1;
      throw new Error(`InMemoryTransport refused publish to ${destination}`);
    }
    this.published.push({ destination, envelope });
  }

  async subscribe(
    destinations: readonly string[],
    onMessage: TransportMessageHandler,
  ): Promise<void> {
    for (const destination of destinations) {
      const handlers = this.#subscribers.get(destination) ?? [];
      handlers.push(onMessage);
      this.#subscribers.set(destination, handlers);
    }
  }

  /**
   * Delivers a message to every subscriber of `destination`, redelivering it to
   * a subscriber that rejects up to `maxRedeliveries` times. Resolves with the
   * total number of handler calls.
   */
  // eslint-disable-next-line relaybus/max-two-params -- public InMemoryTransport.deliver signature
  async deliver(
    destination: string,
    envelope: EventEnvelope,
    maxRedeliveries = 0,
  ): Promise<number> {
    let calls = 0;
    for (const handler of this.#subscribers.get(destination) ?? []) {
      calls += await deliverWithRedeliveries(handler, { envelope, destination, maxRedeliveries });
    }
    return calls;
  }

  async close(): Promise<void> {
    this.closed = true;
  }
}

/** Hands `envelope` to one handler, again after each failure up to `maxRedeliveries` times; returns the calls. */
async function deliverWithRedeliveries(
  handler: TransportMessageHandler,
  delivery: {
    readonly envelope: EventEnvelope;
    readonly destination: string;
    readonly maxRedeliveries: number;
  },
): Promise<number> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      await handler(delivery.envelope, { destination: delivery.destination });
      return attempt + 1;
    } catch (error) {
      if (attempt >= delivery.maxRedeliveries) {
        throw error;
      }
    }
  }
}
