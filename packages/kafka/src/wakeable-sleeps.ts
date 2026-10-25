import { systemClock } from '@relaybus/core';
import { schedule } from '@relaybus/core/internal';

/** Sleeps that a close ends at once, so no backoff outlives the transport. */
export class WakeableSleeps {
  readonly #sleepers = new Set<() => void>();

  sleep(milliseconds: number): Promise<void> {
    return new Promise((resolve) => {
      const wake = (): void => {
        cancel();
        this.#sleepers.delete(wake);
        resolve();
      };
      const cancel = schedule(systemClock, { run: wake, delayMs: milliseconds });
      this.#sleepers.add(wake);
    });
  }

  wakeAll(): void {
    for (const wake of this.#sleepers) {
      wake();
    }
  }

  /**
   * Sleeps `milliseconds` in slices of at most `sliceMs`, running `between` after each, until done
   * or `stopped`: a long backoff then never outlives the session timeout.
   */
  async sleepInSlices(
    milliseconds: number,
    {
      sliceMs,
      between,
      stopped,
    }: {
      readonly sliceMs: number;
      readonly between: () => Promise<void>;
      readonly stopped: () => boolean;
    },
  ): Promise<void> {
    let remaining = milliseconds;
    do {
      const slice = Math.min(remaining, sliceMs);
      await this.sleep(slice);
      await between();
      remaining -= slice;
    } while (remaining > 0 && !stopped());
  }
}
