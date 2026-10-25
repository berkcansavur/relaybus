import type { ClaimedDelivery } from './event-store.js';

// Small helpers of the Dispatcher, kept apart to hold dispatcher.ts under the file size limit.

/** Settlements are pending per claim: an older claim of a delivery must never replace or remove the newer one's. */
export function pendingKey(delivery: ClaimedDelivery): string {
  return `${delivery.deliveryId}\u0000${delivery.leaseToken}`;
}

export function rejectOnAbort(signal: AbortSignal): Promise<never> {
  return new Promise<never>((_, reject) => {
    // The reason is always an Error: the dispatcher aborts with LeaseLostError,
    // HandlerTimeoutError or DispatcherStoppedError.
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
}
