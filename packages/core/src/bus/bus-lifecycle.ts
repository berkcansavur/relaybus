import type { Clock } from '../clock.js';
import type { Dispatcher } from '../dispatcher.js';
import { LifecycleError } from '../errors.js';
import type { CloseOptions, EventStore } from '../event-store.js';
import { withinDeadline, type ShutdownDeadline, type StopResult } from '../shutdown-deadline.js';
import type { BusMaintenance } from './bus-maintenance.js';

/** Where a bus is in its one life: it starts once and, once stopped, stays stopped. */
export type BusState = 'idle' | 'starting' | 'running' | 'stopping' | 'stopped';

/** What stopping a bus closes. */
export type BusLifecycleParts = {
  readonly store: EventStore;
  readonly clock: Clock;
  readonly dispatcher: Dispatcher;
  readonly maintenance: BusMaintenance;
  /** Closes every transport, each at most until the deadline. */
  readonly closeTransports: (deadline: ShutdownDeadline) => Promise<void>;
};

/** A stop request: the shared stop of every caller, and whether this caller started it. */
export type StopRequest = { readonly stopping: Promise<StopResult>; readonly first: boolean };

/**
 * The state machine of a bus (idle, starting, running, stopping, stopped) and the shutdown that
 * moves it to stopped: stop the background jobs and the dispatcher, then close the transports
 * and the store, all within one deadline.
 */
export class BusLifecycle {
  readonly #parts: BusLifecycleParts;
  #state: BusState = 'idle';
  #starting: Promise<void> | undefined;
  #stopping: Promise<StopResult> | undefined;

  constructor(parts: BusLifecycleParts) {
    this.#parts = parts;
  }

  get state(): BusState {
    return this.#state;
  }

  /** Throws unless the bus is idle: a bus starts once. */
  assertStartable(): void {
    if (this.#state !== 'idle') {
      throw new LifecycleError(
        this.#state === 'stopped'
          ? 'A stopped relaybus cannot be started again; create a new instance'
          : `Cannot start a relaybus that is ${this.#state}`,
      );
    }
  }

  /** Runs `startComponents` as the start in progress, which a stop waits for; returns it. */
  beginStart(startComponents: () => Promise<void>): Promise<void> {
    this.#state = 'starting';
    this.#starting = startComponents();
    return this.#starting;
  }

  /** The start in progress settled. */
  endStart(): void {
    this.#starting = undefined;
  }

  markRunning(): void {
    this.#state = 'running';
  }

  markStopped(): void {
    this.#state = 'stopped';
  }

  /** Closes the transports a failed start may have connected, within `timeoutMs`. */
  closeAfterFailedStart(timeoutMs: number): Promise<StopResult> {
    return withinDeadline({ clock: this.#parts.clock, timeoutMs }, async (deadline) => {
      await this.#parts.closeTransports(deadline);
      return 0;
    });
  }

  /** The stop of the bus within `timeoutMs`; a later call shares the first one's. */
  stop(timeoutMs: number): StopRequest {
    if (this.#stopping !== undefined) {
      return { stopping: this.#stopping, first: false };
    }
    if (this.#state !== 'running' && this.#state !== 'starting') {
      return { stopping: this.#stopOutsideRun(timeoutMs), first: false };
    }
    this.#stopping = this.#stopComponents(timeoutMs);
    return { stopping: this.#stopping, first: true };
  }

  /**
   * A bus that is not running: an idle one only settles what batches run by hand left behind (and
   * stays startable); a stopped one has nothing left to do.
   */
  async #stopOutsideRun(timeoutMs: number): Promise<StopResult> {
    if (this.#state !== 'idle') {
      return { graceful: true, abandonedDeliveries: 0, durationMs: 0 };
    }
    return withinDeadline({ clock: this.#parts.clock, timeoutMs }, (deadline) =>
      this.#parts.dispatcher.stop(deadline),
    );
  }

  /**
   * Waits for a start in progress (a failed start leaves nothing to stop), then stops the dispatcher
   * and closes the transports and the store. A start still running at the deadline is given up: the
   * transports and the store are force-closed and the start rejects when it resumes.
   */
  async #stopComponents(timeoutMs: number): Promise<StopResult> {
    try {
      return await withinDeadline({ clock: this.#parts.clock, timeoutMs }, async (deadline) => {
        if (this.#state === 'starting') {
          const started = await deadline.race(this.#starting!.catch(() => undefined));
          if (started === 'timeout') {
            this.#state = 'stopping';
            await this.#closeConnections(deadline);
            return 0;
          }
        }
        if (this.#state !== 'running') {
          return 0;
        }
        this.#state = 'stopping';
        this.#parts.maintenance.stop();
        const abandoned = await this.#parts.dispatcher.stop(deadline);
        await this.#closeConnections(deadline);
        return abandoned;
      });
    } finally {
      this.#state = 'stopped';
    }
  }

  async #closeConnections(deadline: ShutdownDeadline): Promise<void> {
    await this.#parts.closeTransports(deadline);
    const options: CloseOptions = { signal: deadline.signal };
    await deadline.race(Promise.resolve(this.#parts.store.close?.(options)));
  }
}
