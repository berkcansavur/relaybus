import type { Clock } from '@relaybus/core';

/** Clock that only moves when told to. */
export class FakeClock implements Clock {
  #now: number;

  constructor(start: Date | string | number = '2026-01-01T00:00:00.000Z') {
    this.#now = new Date(start).getTime();
  }

  now(): Date {
    return new Date(this.#now);
  }

  advance(milliseconds: number): Date {
    this.#now += milliseconds;
    return this.now();
  }

  set(instant: Date | string | number): Date {
    this.#now = new Date(instant).getTime();
    return this.now();
  }
}
