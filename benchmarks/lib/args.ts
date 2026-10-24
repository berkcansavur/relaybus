// Command line of the benchmark CLI: `bench quick|full [options]`.
import type { Mode } from './cells.ts';

export interface CliOptions {
  readonly mode: Mode;
  /** Git ref measured as the reference (default `main`). */
  readonly base: string;
  /** Git ref under test (default `HEAD`). */
  readonly candidate: string;
  /** Samples per side for every cell (default: the plan's own count). */
  readonly pairs: number | undefined;
  /** Only cells whose id contains one of these fragments. */
  readonly only: readonly string[];
  /** Measure the base again, interleaved with the candidate, even when a stored baseline exists. */
  readonly interleave: boolean;
  /** Ignore stored baselines (implies interleaving). */
  readonly useBaseline: boolean;
  /** Store the samples as baselines. */
  readonly store: boolean;
  /** Run although the machine is busy. */
  readonly force: boolean;
  /** Also fail on throughput and latency regressions, not only on counters and validation. */
  readonly strict: boolean;
  /** Counter names whose regression is expected (for example `deliveries.delPerUnit`); they are reported but do not fail the gate. */
  readonly accept: readonly string[];
  /** Build the two refs and stop. */
  readonly prepareOnly: boolean;
}

const VALUE_FLAGS = new Set(['--base', '--candidate', '--pairs', '--only', '--accept']);
const SWITCHES = new Set([
  '--interleave',
  '--no-baseline',
  '--no-store',
  '--force',
  '--strict',
  '--prepare',
]);

const list = (text: string | undefined): string[] =>
  (text ?? '')
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item !== '');

/** Parses the arguments after the script name. Throws on anything it does not know, so a typo never silently measures the wrong thing. */
export function parseArguments(argv: readonly string[]): CliOptions {
  const [command, ...rest] = argv;
  if (command !== 'quick' && command !== 'full')
    throw new Error(
      'Usage: bench <quick|full> [--base <ref>] [--candidate <ref>] [--pairs <n>] [--only <cell,...>] [--accept <counter,...>] [--interleave] [--no-baseline] [--no-store] [--force] [--strict] [--prepare]',
    );
  const values = new Map<string, string>();
  const switches = new Set<string>();
  for (let index = 0; index < rest.length; index += 1) {
    const flag = rest[index];
    if (flag === undefined) continue;
    if (VALUE_FLAGS.has(flag)) {
      const value = rest[index + 1];
      if (value === undefined || value.startsWith('--')) throw new Error(`${flag} needs a value`);
      values.set(flag, value);
      index += 1;
    } else if (SWITCHES.has(flag)) {
      switches.add(flag);
    } else {
      throw new Error(`Unknown option ${flag}`);
    }
  }
  const pairsText = values.get('--pairs');
  const pairs = pairsText === undefined ? undefined : Number(pairsText);
  if (pairs !== undefined && (!Number.isInteger(pairs) || pairs < 1))
    throw new Error('--pairs must be a positive integer');
  return {
    mode: command,
    base: values.get('--base') ?? 'main',
    candidate: values.get('--candidate') ?? 'HEAD',
    pairs,
    only: list(values.get('--only')),
    accept: list(values.get('--accept')),
    interleave: switches.has('--interleave'),
    useBaseline: !switches.has('--no-baseline'),
    store: !switches.has('--no-store'),
    force: switches.has('--force'),
    strict: switches.has('--strict'),
    prepareOnly: switches.has('--prepare'),
  };
}
