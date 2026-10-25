import type { Clock } from './clock.js';
import { schedule, type CancelTimer } from './internal/timers.js';

/**
 * The timers of running deliveries (handler timeouts, waits between completion retries). A stop that
 * abandons the deliveries cancels them all, so none keeps the process alive or acts after the stop.
 */
export class DeliveryTimers {
  readonly #clock: Clock;
  /** Cancel of every timer still waiting, with what its cancellation must also do. */
  readonly #waiting = new Map<CancelTimer, () => void>();

  constructor(clock: Clock) {
    this.#clock = clock;
  }

  /** Runs `task.run` after `task.delayMs`; returns what cancels it. */
  start(task: { readonly run: () => void; readonly delayMs: number }): CancelTimer {
    return this.#start(task, () => undefined);
  }

  /** Resolves after `milliseconds`, or at once when {@link DeliveryTimers.cancelAll} runs. */
  sleep(milliseconds: number): Promise<void> {
    return new Promise((resolve) => {
      this.#start({ run: resolve, delayMs: milliseconds }, resolve);
    });
  }

  cancelAll(): void {
    for (const [cancel, onCancel] of [...this.#waiting]) {
      cancel();
      onCancel();
    }
  }

  #start(
    task: { readonly run: () => void; readonly delayMs: number },
    onCancel: () => void,
  ): CancelTimer {
    const cancel = (): void => {
      cancelTimer();
      this.#waiting.delete(cancel);
    };
    const cancelTimer = schedule(this.#clock, {
      run: () => {
        this.#waiting.delete(cancel);
        task.run();
      },
      delayMs: task.delayMs,
    });
    this.#waiting.set(cancel, onCancel);
    return cancel;
  }
}
