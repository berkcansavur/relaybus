import { afterEach, describe, expect, it, vi } from 'vitest';
import { systemClock, type Clock } from '@relaybus/core';
import { schedule, sleep, type ScheduledTask } from '@relaybus/core/internal';

afterEach(() => {
  vi.useRealTimers();
});

describe('schedule', () => {
  it('uses the platform timers when the clock does not control them', () => {
    vi.useFakeTimers();
    const once = vi.fn();
    const repeated = vi.fn();
    const cancelOnce = schedule(systemClock, { run: once, delayMs: 100 });
    const cancelRepeated = schedule(systemClock, { run: repeated, delayMs: 100, repeat: true });
    vi.advanceTimersByTime(250);
    expect(once).toHaveBeenCalledTimes(1);
    expect(repeated).toHaveBeenCalledTimes(2);
    cancelRepeated();
    cancelOnce();
    vi.advanceTimersByTime(1_000);
    expect(repeated).toHaveBeenCalledTimes(2);
  });

  it('unrefs only the timers that ask for it', () => {
    const unref = vi.fn();
    const timer = { unref } as unknown as NodeJS.Timeout;
    const timeout = vi.spyOn(globalThis, 'setTimeout').mockReturnValue(timer);
    const interval = vi.spyOn(globalThis, 'setInterval').mockReturnValue(timer);
    schedule(systemClock, { run: () => undefined, delayMs: 1 });
    schedule(systemClock, { run: () => undefined, delayMs: 1, repeat: true });
    expect(unref).not.toHaveBeenCalled();
    schedule(systemClock, { run: () => undefined, delayMs: 1, unref: true });
    schedule(systemClock, { run: () => undefined, delayMs: 1, repeat: true, unref: true });
    expect(unref).toHaveBeenCalledTimes(2);
    timeout.mockRestore();
    interval.mockRestore();
  });

  it('hands the task to a clock that controls timers', async () => {
    const cancel = vi.fn();
    const clock: Clock = {
      now: () => new Date(0),
      schedule: vi.fn((task: ScheduledTask) => {
        task.run();
        return cancel;
      }),
    };
    const run = vi.fn();
    expect(schedule(clock, { run, delayMs: 5, repeat: true, unref: true })).toBe(cancel);
    expect(clock.schedule).toHaveBeenCalledWith({ run, delayMs: 5, repeat: true, unref: true });
    expect(run).toHaveBeenCalledTimes(1);
    await expect(sleep(clock, 10)).resolves.toBeUndefined();
  });
});
