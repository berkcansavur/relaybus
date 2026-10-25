import type { Clock } from './clock.js';
import type { Duration } from './duration.js';
import { schedule, type CancelTimer } from './internal/timers.js';

export interface StopOptions {
  /**
   * Longest the whole `stop` may take. Default: `dispatcher.shutdownTimeout`. Keep it below the time
   * the platform waits before killing the process (Kubernetes `terminationGracePeriodSeconds`).
   */
  readonly timeout?: Duration;
}

/** How a `stop` went. */
export interface StopResult {
  /** False when the deadline passed and `stop` aborted handlers or force-closed connections. */
  readonly graceful: boolean;
  /**
   * Deliveries whose handlers still ran at the deadline. Their leases expire after
   * `dispatcher.leaseDuration` and another process runs them again (at-least-once).
   */
  readonly abandonedDeliveries: number;
  /** Time `stop` took, on the bus clock. */
  readonly durationMs: number;
}

/**
 * Share of the shutdown timeout running handlers get before they are aborted. The rest is left for
 * releasing their deliveries, handing back leases and closing transports and the store.
 */
const DRAIN_SHARE = 0.8;

/**
 * The deadline of one `stop`. Running handlers are waited for until the drain point (four fifths of
 * the timeout), every other wait until the deadline itself, and the signal, aborted when the deadline
 * passes, tells the transports and the store to force-close what they opened themselves.
 */
export class ShutdownDeadline {
  readonly #controller = new AbortController();
  readonly #drained: Promise<'timeout'>;
  readonly #expired: Promise<'timeout'>;
  readonly #cancelTimers: CancelTimer[];

  constructor(clock: Clock, timeoutMs: number) {
    let drain!: () => void;
    this.#drained = new Promise((resolve) => {
      drain = () => resolve('timeout');
    });
    const signal = this.#controller.signal;
    this.#expired = new Promise((resolve) => {
      signal.addEventListener('abort', () => resolve('timeout'), { once: true });
    });
    this.#cancelTimers = [
      schedule(clock, { run: drain, delayMs: Math.floor(timeoutMs * DRAIN_SHARE) }),
      schedule(clock, { run: () => this.#controller.abort(), delayMs: timeoutMs }),
    ];
  }

  /** Aborted when the deadline passes. */
  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  get passed(): boolean {
    return this.#controller.signal.aborted;
  }

  /** Waits for `work` until the deadline. Rejects when `work` rejects before it. */
  race(work: Promise<unknown>): Promise<'done' | 'timeout'> {
    return Promise.race([work.then(() => 'done' as const), this.#expired]);
  }

  /** Waits for running handlers until the drain point, when they are aborted. */
  raceDrain(work: Promise<unknown>): Promise<'done' | 'timeout'> {
    return Promise.race([work.then(() => 'done' as const), this.#drained]);
  }

  /** Cancels the timers once the shutdown is over. */
  dispose(): void {
    for (const cancel of this.#cancelTimers) {
      cancel();
    }
  }
}

/**
 * Runs one shutdown under a deadline of `timeoutMs` on `clock`. `work` resolves with the deliveries it
 * abandoned; the result says whether the deadline passed and how long it all took.
 */
export async function withinDeadline(
  { clock, timeoutMs }: { readonly clock: Clock; readonly timeoutMs: number },
  work: (deadline: ShutdownDeadline) => Promise<number>,
): Promise<StopResult> {
  const startedAt = clock.now().getTime();
  const deadline = new ShutdownDeadline(clock, timeoutMs);
  try {
    const abandonedDeliveries = await work(deadline);
    return {
      graceful: !deadline.passed,
      abandonedDeliveries,
      durationMs: clock.now().getTime() - startedAt,
    };
  } finally {
    deadline.dispose();
  }
}
