import { CompositeKey } from './composite-key.js';

/**
 * Identifies one claim of a delivery: the delivery id with the exact lease token it was handed out
 * with. Two claims of the same delivery (an expired one and the current one) have different
 * identities, so bookkeeping of the older can never replace or remove the newer one's.
 */
export abstract class DeliveryLeaseIdentity {
  static of(deliveryId: string, leaseToken: string): string {
    return CompositeKey.of(deliveryId, leaseToken);
  }

  /** The start of the identity of every claim of `deliveryId`. */
  static prefixOf(deliveryId: string): string {
    return CompositeKey.startOf(deliveryId);
  }
}
