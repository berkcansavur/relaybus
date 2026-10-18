import { describe, expect, it } from 'vitest';
import { abortableSleep } from '../src/abortable-sleep.js';
import { SerialQueue } from '../src/serial-queue.js';
import { deferred } from './fakes.js';

describe('abortableSleep', () => {
  it('resolves after the delay', async () => {
    const started = Date.now();
    await abortableSleep(30, new AbortController().signal);
    expect(Date.now() - started).toBeGreaterThanOrEqual(25);
  });

  it('resolves immediately when aborted', async () => {
    const controller = new AbortController();
    const sleeping = abortableSleep(60_000, controller.signal);
    controller.abort();
    await sleeping;
  });

  it('does not sleep at all when the signal was aborted before', async () => {
    const controller = new AbortController();
    controller.abort();
    await abortableSleep(60_000, controller.signal);
  });
});

describe('SerialQueue', () => {
  it('runs tasks in order, one at a time, and survives failures', async () => {
    const queue = new SerialQueue();
    const events: string[] = [];
    const gate = deferred();
    const first = queue.run(async () => {
      events.push('first:start');
      await gate.promise;
      events.push('first:end');
      return 1;
    });
    const second = queue.run(async () => {
      events.push('second');
      throw new Error('boom');
    });
    const third = queue.run(async () => {
      events.push('third');
      return 3;
    });
    await Promise.resolve();
    expect(events).toEqual(['first:start']);
    gate.resolve();
    expect(await first).toBe(1);
    await expect(second).rejects.toThrow('boom');
    expect(await third).toBe(3);
    expect(events).toEqual(['first:start', 'first:end', 'second', 'third']);
  });
});
