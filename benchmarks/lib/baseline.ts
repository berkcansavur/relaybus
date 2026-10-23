// Stored measurements of a commit, keyed by machine fingerprint and commit SHA, so the base side of a comparison
// never has to be measured twice on the same machine.
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Sample } from './sample.ts';

export const BASELINE_SCHEMA = 1;

/** Serialises a value with sorted object keys, so equal data always gives equal text. */
export function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, item]) => item !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`).join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

/** Short stable hash of everything that makes two measurements comparable (machine, images, settings, harness version, cells). */
export function fingerprintOf(descriptor: unknown): string {
  return createHash('sha256').update(stableStringify(descriptor)).digest('hex').slice(0, 12);
}

export interface BaselineFile {
  readonly schema: number;
  readonly sha: string;
  readonly ref: string;
  readonly fingerprint: string;
  readonly createdAt: string;
  /** Fastest CPU calibration seen while measuring, for the staleness check against the current machine state. */
  readonly calibrationMs: number;
  readonly samples: readonly Sample[];
}

export function baselineDirectory(root: string, fingerprint: string, sha: string): string {
  return join(root, fingerprint, sha);
}

/** Returns the stored baseline of a commit on this machine fingerprint, or `undefined` when there is none. */
export function loadBaseline(
  root: string,
  fingerprint: string,
  sha: string,
): BaselineFile | undefined {
  const file = join(baselineDirectory(root, fingerprint, sha), 'samples.json');
  if (!existsSync(file)) return undefined;
  const parsed = JSON.parse(readFileSync(file, 'utf8')) as BaselineFile;
  return parsed.schema === BASELINE_SCHEMA &&
    parsed.sha === sha &&
    parsed.fingerprint === fingerprint
    ? parsed
    : undefined;
}

/** Writes (or replaces) the baseline of a commit together with the environment it was measured in. */
export function saveBaseline(root: string, baseline: BaselineFile, environment: unknown): string {
  const directory = baselineDirectory(root, baseline.fingerprint, baseline.sha);
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, 'env.json'), `${JSON.stringify(environment, null, 2)}\n`);
  writeFileSync(join(directory, 'samples.json'), `${JSON.stringify(baseline)}\n`);
  return directory;
}

/** How far the current machine state is from the one the baseline was measured in (ratio of the calibration times). */
export function calibrationDrift(baseline: BaselineFile, currentCalibrationMs: number): number {
  return currentCalibrationMs / baseline.calibrationMs;
}

/** Cells a plan needs: the id and how many samples per side it expects. */
export interface RequiredCell {
  readonly id: string;
  readonly reps: number;
}

/** The stored baseline of a commit, but only if it holds at least the planned number of samples for every cell. */
export function findUsableBaseline(
  root: string,
  fingerprint: string,
  sha: string,
  cells: readonly RequiredCell[],
): BaselineFile | undefined {
  const stored = loadBaseline(root, fingerprint, sha);
  if (stored === undefined) return undefined;
  const covers = cells.every(
    (cell) => stored.samples.filter((sample) => sample.cellId === cell.id).length >= cell.reps,
  );
  return covers ? stored : undefined;
}

/** Combines stored samples with fresh ones; the fresh samples of a cell replace all stored samples of that cell. */
export function mergeSamples(existing: readonly Sample[], fresh: readonly Sample[]): Sample[] {
  const replaced = new Set(fresh.map((sample) => sample.cellId));
  return [...existing.filter((sample) => !replaced.has(sample.cellId)), ...fresh];
}
