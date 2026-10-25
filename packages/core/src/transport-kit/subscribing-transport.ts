import type { EventEnvelope } from '../event.js';
import type { CloseOptions } from '../event-store.js';
import type {
  Transport,
  TransportMessageHandler,
  TransportSubscriptionHooks,
} from '../transport.js';

/** One subscription of a transport: what it consumes and where the messages go. */
export interface TransportSubscription {
  readonly destinations: readonly string[];
  readonly onMessage: TransportMessageHandler;
  readonly hooks: TransportSubscriptionHooks | undefined;
}

/**
 * The skeleton of a broker transport that publishes and subscribes: the {@link Transport} port
 * with its subscribe signature in one place. A transport supplies how it publishes, opens its one
 * subscription, reports readiness and closes.
 */
export abstract class SubscribingTransport implements Transport {
  abstract readonly name: string;
  abstract readonly system: string;

  /**
   * Subscribes to `destinations`. Not async itself: the subscription's own promise is returned
   * as it is, so its checks reject rather than throw.
   */
  // eslint-disable-next-line relaybus/max-two-params -- the Transport.subscribe signature
  subscribe(
    destinations: readonly string[],
    onMessage: TransportMessageHandler,
    hooks?: TransportSubscriptionHooks,
  ): Promise<void> {
    return this.openSubscription({ destinations, onMessage, hooks });
  }

  abstract publish(envelope: EventEnvelope, destination: string): Promise<void>;

  abstract isReady(): boolean;

  abstract close(options?: CloseOptions): Promise<void>;

  /** Opens the transport's one subscription; rejects when it cannot. */
  protected abstract openSubscription(subscription: TransportSubscription): Promise<void>;
}
