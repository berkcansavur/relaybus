/** Source of the current time. Replace it in tests to control retries and leases. */
export interface Clock {
  now(): Date;
  /**
   * Runs `task.run` after `task.delayMs` milliseconds, again every `delayMs` when `repeat` is set, and
   * returns a function that cancels it. `unref` marks a timer that must not keep the process alive.
   * Optional: without it relaybus uses the platform timers. Implement it to control timers in tests
   * (`FakeClock` does with `controlTimers: true`).
   */
  schedule?(task: {
    readonly run: () => void;
    readonly delayMs: number;
    readonly repeat?: boolean;
    readonly unref?: boolean;
  }): () => void;
}

export const systemClock: Clock = {
  now: () => new Date(),
};
