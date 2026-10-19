import type { Store } from './cells.ts';
import type { DbWindow } from './db-metrics.ts';
import type { SideModules } from './deps.ts';
import type { ValidationData } from './validate.ts';

/** Measures the database-side counters of one window: call `begin()` when the window opens, `end()` after the workers stopped. */
export interface RunMonitor {
  begin(): Promise<void>;
  end(): Promise<DbWindow>;
}

export interface Connection {
  readonly url: string;
  readonly database: string;
}

/** A database server in a container, plus the operations the harness needs on it. One per store, shared by both sides. */
export interface Backend {
  readonly name: Store;
  readonly containerId: string;
  /** Server version, image and settings, for env.json. */
  describe(): Promise<Record<string, unknown>>;
  /** Creates an empty database and lets `side` set its schema up. */
  createFresh(side: SideModules, database: string): Promise<void>;
  /** Copies a preloaded database; `side` sets up whatever the copy does not carry (indexes). */
  clone(side: SideModules, template: string, database: string): Promise<void>;
  drop(database: string): Promise<void>;
  connection(database: string): Connection;
  monitor(database: string): RunMonitor;
  /** Flushes and analyzes after a bulk load, so a clone starts from a settled state. */
  afterLoad(database: string): Promise<void>;
  /** Number of deliveries per status, for logging a template. */
  countByStatus(database: string): Promise<Record<string, number>>;
  /** Ground truth for exactness validation: every committed event and every delivery that is not completed. */
  validationData(database: string): Promise<ValidationData>;
  /** Round trip latency of a trivial command. */
  roundTrip(): Promise<{ p50: number; p99: number }>;
  stop(): Promise<void>;
}

/** p50 / p99 of a list of latency samples (the list is sorted in place). */
export function latencyQuantiles(samples: number[]): { p50: number; p99: number } {
  samples.sort((a, b) => a - b);
  return {
    p50: samples[Math.floor(samples.length * 0.5)] ?? Number.NaN,
    p99: samples[Math.floor(samples.length * 0.99)] ?? Number.NaN,
  };
}
