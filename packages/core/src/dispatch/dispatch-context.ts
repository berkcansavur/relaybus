import type { Clock } from '../clock.js';
import type { DispatchObservation } from '../dispatch-observation.js';
import type { EventStore } from '../event-store.js';
import type { HandlerRegistry } from '../handler-registry.js';
import type { Notifier } from '../notifier.js';

export interface DispatcherSettings {
  readonly owner: string;
  readonly concurrency: number;
  readonly batchSize: number;
  readonly pollIntervalMs: number;
  readonly leaseDurationMs: number;
  readonly shutdownTimeoutMs: number;
  /** Longest a completion waits to be written together with others. Absent or 0: written one by one. */
  readonly completionBatchWindowMs?: number;
  /** Completions written at once, at most. Used with `completionBatchWindowMs`. */
  readonly completionBatchSize?: number;
}

export interface DispatcherDependencies {
  readonly store: EventStore;
  readonly registry: HandlerRegistry;
  readonly clock: Clock;
  readonly random: () => number;
  readonly notifier: Notifier;
  readonly settings: DispatcherSettings;
  /** Reports claims and handler runs; absent when nothing observes or traces them. */
  readonly observation?: DispatchObservation | undefined;
  /** Waits between attempts to confirm a completion. Replaceable in tests. */
  readonly sleep?: (milliseconds: number) => Promise<void>;
}

/**
 * Whether a stop gave up at its deadline. Then the store may be closed: nothing more is
 * started, retried or written, and running deliveries are left to lease expiry.
 */
export class ShutdownLatch {
  #abandoned = false;

  get abandoned(): boolean {
    return this.#abandoned;
  }

  abandon(): void {
    this.#abandoned = true;
  }
}
