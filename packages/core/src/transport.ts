import type { EventEnvelope } from './event.js';
import type { CloseOptions } from './event-store.js';

/** Where an incoming message came from, for observers and traces. */
export interface TransportMessage {
  /** Topic, routing key or queue the message arrived on. */
  readonly destination: string;
}

/** A message the transport could not turn into an event, so it never reaches the bus. */
export interface InvalidTransportMessage {
  /** Topic, routing key or queue the message arrived on. */
  readonly destination: string;
  /** What the codec or decoder threw. */
  readonly error: unknown;
}

/**
 * What the bus offers a transport at `subscribe`, beyond the message handler. Present only when
 * an observer implements the matching hook, so a transport can skip the work otherwise.
 */
export interface TransportSubscriptionHooks {
  /**
   * Call once for each inbound message the transport rejected as undecodable (dead-lettered or
   * skipped) before it reached `onMessage`. Messages the bus rejected itself are reported by
   * `onReceiveFinished` and must not be reported here. Never throws.
   */
  readonly onInvalidMessage: (message: InvalidTransportMessage) => void;
}

/**
 * Receives an event from a broker. Resolve only after it is durably stored. Transports pass
 * `message` to tell where the event arrived; it is optional for transports written before it.
 */
export type TransportMessageHandler = (
  envelope: EventEnvelope,
  message?: TransportMessage,
) => Promise<void>;

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
  /**
   * The messaging system, as OpenTelemetry names it (`kafka`, `rabbitmq`), reported to observers
   * and traces. Default: the transport's name.
   */
  readonly system?: string;
  connect?(): Promise<void>;
  /**
   * Offered the wire names that routes and catalogs declare (event type to the name the wire
   * format uses), once when the bus is created, before `connect` and `subscribe`. Check them
   * without changing anything and return the function that applies them to the codec, so the bus
   * can refuse a configuration before any transport changed. Throw
   * {@link WireTypeCollisionError} when a name clashes with one the codec already has and
   * {@link WireTypesUnsupportedError} when the codec cannot map type names or the transport
   * already started. A transport without it refuses declared wire names at boot.
   */
  declareWireTypes?(wireTypes: Readonly<Record<string, string>>): () => void;
  publish(envelope: EventEnvelope, destination: string): Promise<void>;
  subscribe?(
    destinations: readonly string[],
    onMessage: TransportMessageHandler,
    hooks?: TransportSubscriptionHooks,
  ): Promise<void>;
  /**
   * Whether the transport can currently publish and, when subscribed, is
   * consuming. Health checks use it to report a dead consumer or a lost
   * connection. Transports without it count as ready.
   */
  isReady?(): boolean;
  /**
   * Releases connections. A closed transport is not reopened. Once `options.signal` aborts (the
   * shutdown deadline passed), stop waiting and drop the connections it opened itself.
   */
  close(options?: CloseOptions): Promise<void>;
}
