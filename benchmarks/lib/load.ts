// Machine load around a sample: a fixed CPU spin (slower when other work competes for the cores), load average and
// host CPU utilisation. The comparison discards samples whose spin was much slower than the quietest sample.
import { cpus, loadavg } from 'node:os';
import type { LoadReading } from './sample.ts';

const SPIN_ITERATIONS = 12_000_000;
const SPIN_REPEATS = 5;

function spinOnce(): number {
  const started = performance.now();
  let state = 0x9e3779b9;
  for (let index = 0; index < SPIN_ITERATIONS; index += 1)
    state = Math.imul(state ^ index, 0x85ebca6b) >>> 0;
  // Keeps the loop from being optimised away.
  if (state === 1) throw new Error('unreachable');
  return performance.now() - started;
}

/** Fastest of a few identical CPU spins, in milliseconds. */
export function calibrate(): number {
  return Math.min(...Array.from({ length: SPIN_REPEATS }, spinOnce));
}

export interface CpuSnapshot {
  readonly idle: number;
  readonly total: number;
}

export function cpuSnapshot(): CpuSnapshot {
  let idle = 0;
  let total = 0;
  for (const cpu of cpus()) {
    const { user, nice, sys, idle: cpuIdle, irq } = cpu.times;
    idle += cpuIdle;
    total += user + nice + sys + cpuIdle + irq;
  }
  return { idle, total };
}

/** Host CPU utilisation (percent) between two snapshots. */
export function hostCpuPct(from: CpuSnapshot, to: CpuSnapshot): number {
  const total = to.total - from.total;
  return total <= 0 ? 0 : Math.round(1000 * (1 - (to.idle - from.idle) / total)) / 10;
}

export const loadAverage = (): number => Math.round(loadavg()[0]! * 100) / 100;

/** Collects the load reading of one sample: call `begin()` right before the sample, `finish()` right after. */
export class LoadMeter {
  readonly #calibrationMs: number;
  readonly #loadStart: number;
  readonly #cpuStart: CpuSnapshot;

  constructor() {
    this.#calibrationMs = Math.round(calibrate() * 10) / 10;
    this.#loadStart = loadAverage();
    this.#cpuStart = cpuSnapshot();
  }

  finish(dbCpuAvgPct: number | null): LoadReading {
    return {
      loadavgStart: this.#loadStart,
      loadavgEnd: loadAverage(),
      calibrationMs: this.#calibrationMs,
      hostCpuPct: hostCpuPct(this.#cpuStart, cpuSnapshot()),
      dbCpuAvgPct,
    };
  }
}
