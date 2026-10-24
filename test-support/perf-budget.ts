import { appendFileSync, readFileSync } from 'node:fs';
import { expect } from 'vitest';

interface PerformanceBudget {
  /** Highest value the measurement may reach. A budget may only ever be lowered. */
  readonly budget: number;
  readonly unit: string;
  readonly description: string;
}

const budgets = JSON.parse(
  readFileSync(new URL('../perf-budgets.json', import.meta.url), 'utf8'),
) as Readonly<Record<string, PerformanceBudget | string>>;

/**
 * Checks a deterministic performance measurement (statements, tuples, bytes, buffers, commands) against
 * its budget in `perf-budgets.json`. The check is soft, so one test reports every measurement it makes.
 * With `RELAYBUS_PERF_REPORT=<file>` every measurement is also appended to that file as one JSON line.
 */
export function expectWithinBudget(id: string, measured: number): void {
  const entry = budgets[id];
  if (typeof entry !== 'object') {
    throw new Error(`perf-budgets.json has no budget named '${id}'`);
  }
  const report = process.env['RELAYBUS_PERF_REPORT'];
  if (report !== undefined && report.length > 0) {
    appendFileSync(report, `${JSON.stringify({ id, measured, budget: entry.budget })}\n`);
  }
  expect
    .soft(measured, `${id}: measured ${measured} ${entry.unit}, budget ${entry.budget}`)
    .toBeLessThanOrEqual(entry.budget);
}
