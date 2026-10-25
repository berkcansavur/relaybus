import type { Clock } from '../clock.js';

/** A task for {@link schedule}: what runs, after how long, whether it repeats and keeps the process alive. */
export type ScheduledTask = Parameters<NonNullable<Clock['schedule']>>[0];

/** Cancels a scheduled task; harmless once it ran or was cancelled. */
export type CancelTimer = () => void;

/**
 * The one place relaybus starts timers: through `clock.schedule` when the clock controls timers,
 * else with the platform timers.
 */
export function schedule(clock: Clock, task: ScheduledTask): CancelTimer {
  return clock.schedule === undefined ? scheduleOnPlatform(task) : clock.schedule(task);
}

/** Resolves after `milliseconds` on the timers of `clock`. */
export function sleep(clock: Clock, milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    schedule(clock, { run: resolve, delayMs: milliseconds });
  });
}

function scheduleOnPlatform(task: ScheduledTask): CancelTimer {
  if (task.repeat === true) {
    const interval = setInterval(task.run, task.delayMs);
    if (task.unref === true) {
      interval.unref();
    }
    return () => clearInterval(interval);
  }
  const timeout = setTimeout(task.run, task.delayMs);
  if (task.unref === true) {
    timeout.unref();
  }
  return () => clearTimeout(timeout);
}
