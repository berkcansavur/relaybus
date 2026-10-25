import type { BusObservation, ReceiveOrigin } from '../bus-observation.js';
import type { Clock } from '../clock.js';
import type { AppendEntry, EventStore, NewDelivery } from '../event-store.js';
import type { EventEnvelope } from '../event.js';
import type { HandlerRegistration, HandlerRegistry } from '../handler-registry.js';
import type { Notifier } from '../notifier.js';
import { normalizeEnvelope } from '../received-envelope.js';
import type { ReceiveStatus } from '../relay-bus-options.js';

/** What receiving needs from the bus. */
export type EventReceiverParts = {
  readonly store: EventStore;
  readonly registry: HandlerRegistry;
  readonly clock: Clock;
  readonly notifier: Notifier;
  readonly observation: BusObservation | undefined;
  readonly createDelivery: (registration: HandlerRegistration, availableAt: Date) => NewDelivery;
  /** Wakes the dispatcher shortly after new deliveries were stored. */
  readonly scheduleWake: () => void;
};

/**
 * Stores events received from brokers for the local handlers (never for routes), once per
 * event id: the inbox side of the bus.
 */
export class EventReceiver {
  readonly #parts: EventReceiverParts;

  constructor(parts: EventReceiverParts) {
    this.#parts = parts;
  }

  /** Stores `envelope` for local handlers; observed when the bus is, with where it came from. */
  receive(envelope: EventEnvelope, origin?: ReceiveOrigin): Promise<ReceiveStatus> {
    // Not async: the store's promise is returned as is, so an unobserved receive allocates nothing extra.
    const { observation } = this.#parts;
    return observation === undefined
      ? this.#receive(envelope)
      : observation.receive({ envelope, origin }, () => this.#receive(envelope));
  }

  async #receive(envelope: EventEnvelope): Promise<ReceiveStatus> {
    const { store, registry, clock, notifier, createDelivery, scheduleWake } = this.#parts;
    const normalized = normalizeEnvelope(envelope);
    const registrations = registry
      .forEventType(normalized.type)
      .filter((registration) => !registration.forwardsToTransport);
    if (registrations.length === 0) {
      notifier.notify('onReceived', { envelope: normalized, status: 'ignored' });
      return 'ignored';
    }
    const now = clock.now();
    const entry: AppendEntry = {
      event: normalized,
      deliveries: registrations.map((registration) => createDelivery(registration, now)),
    };
    const result = await store.transaction((transaction) =>
      store.append(transaction, [entry], now),
    );
    const status: ReceiveStatus = result.duplicateEventIds.length > 0 ? 'duplicate' : 'accepted';
    notifier.notify('onReceived', { envelope: normalized, status });
    if (result.appendedDeliveryCount > 0) {
      scheduleWake();
    }
    return status;
  }
}
