import type { Clock } from '@relaybus/core';

type ScheduledTask = Parameters<NonNullable<Clock['schedule']>>[0];

interface PendingTimer {
  readonly task: ScheduledTask;
  dueAt: number;
  /** Order of scheduling, so timers due at the same time run in the order they were set. */
  readonly sequence: number;
}

/**
 * Clock that only moves when told to. With `controlTimers: true` it also owns relaybus's timers
 * (polling, retries, heartbeats, retention, timeouts): they run inside {@link FakeClock.advance}
 * when their time comes instead of on the platform timers. Without it, timers stay real.
 */
export class FakeClock implements Clock {
  #now: number;
  readonly #timers = new Set<PendingTimer>();
  #sequence = 0;
  /** Present only with `controlTimers: true`; see {@link Clock.schedule}. */
  readonly schedule?: (task: ScheduledTask) => () => void;

  constructor(
    start: Date | string | number = '2026-01-01T00:00:00.000Z',
    options: { readonly controlTimers?: boolean } = {},
  ) {
    this.#now = new Date(start).getTime();
    if (options.controlTimers === true) {
      this.schedule = (task) => this.#schedule(task);
    }
  }

  now(): Date {
    return new Date(this.#now);
  }

  /** Moves the clock forward, running every controlled timer that falls due on the way, in time order. */
  advance(milliseconds: number): Date {
    const target = this.#now + milliseconds;
    for (let timer = this.#nextDue(target); timer !== undefined; timer = this.#nextDue(target)) {
      this.#now = Math.max(this.#now, timer.dueAt);
      if (timer.task.repeat === true) {
        timer.dueAt += Math.max(1, timer.task.delayMs);
      } else {
        this.#timers.delete(timer);
      }
      timer.task.run();
    }
    this.#now = target;
    return this.now();
  }

  set(instant: Date | string | number): Date {
    this.#now = new Date(instant).getTime();
    return this.now();
  }

  /** Controlled timers still waiting to run. */
  get pendingTimers(): number {
    return this.#timers.size;
  }

  #schedule(task: ScheduledTask): () => void {
    const timer: PendingTimer = {
      task,
      dueAt: this.#now + Math.max(0, task.delayMs),
      sequence: this.#sequence++,
    };
    this.#timers.add(timer);
    return () => {
      this.#timers.delete(timer);
    };
  }

  #nextDue(limit: number): PendingTimer | undefined {
    let next: PendingTimer | undefined;
    for (const timer of this.#timers) {
      if (
        timer.dueAt <= limit &&
        (next === undefined ||
          timer.dueAt < next.dueAt ||
          (timer.dueAt === next.dueAt && timer.sequence < next.sequence))
      ) {
        next = timer;
      }
    }
    return next;
  }
}
