import type { EventEnvelope } from './event.js';

/** Receives an event from a broker. Resolve only after it is durably stored. */
export type TransportMessageHandler = (envelope: EventEnvelope) => Promise<void>;

/**
 * Broker port used to exchange events between services.
 *
 * Outbound: the bus calls `publish` from a delivery, so a broker outage only
 * delays events; the outbox keeps them. `publish` must resolve only after the
 * broker acknowledged the message.
 *
 * Inbound: `subscribe` must acknowledge a message only after `onMessage`
 * resolved, and redeliver it when `onMessage` rejects (at-least-once).
 */
export interface Transport {
  /** Unique name referenced by routes and subscriptions. */
  readonly name: string;
  connect?(): Promise<void>;
  publish(envelope: EventEnvelope, destination: string): Promise<void>;
  subscribe?(destinations: readonly string[], onMessage: TransportMessageHandler): Promise<void>;
  close(): Promise<void>;
}
