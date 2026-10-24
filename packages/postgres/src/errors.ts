import { RelayBusError } from '@relaybus/core';

/**
 * A claim was refused because giving back the deliveries of an earlier, failed
 * claim still fails. Claiming before that would take the owner's live lanes for
 * foreign ones; the next claim tries the reconciliation again.
 */
export class ReconcileFailedError extends RelayBusError {
  override name = 'ReconcileFailedError';
  override readonly code: string = 'RELAYBUS_POSTGRES_RECONCILE_FAILED';
}
