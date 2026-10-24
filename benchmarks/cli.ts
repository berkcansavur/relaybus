// `pnpm bench:quick` / `pnpm bench:full`: builds the base and candidate refs, measures the cells of the plan with fixed
// windows, compares them (interleaved against a freshly measured base, or against a stored baseline) and writes a report.
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { loadavg, cpus } from 'node:os';
import { parseArguments, type CliOptions } from './lib/args.ts';
import {
  BASELINE_SCHEMA,
  calibrationDrift,
  findUsableBaseline,
  loadBaseline,
  mergeSamples,
  saveBaseline,
} from './lib/baseline.ts';
import type { Backend } from './lib/backends.ts';
import {
  filterPlan,
  fullPlan,
  quickPlan,
  type Cell,
  type Plan,
  type Store,
  type TemplateDefinition,
} from './lib/cells.ts';
import { compareCell, gate, type CellComparison } from './lib/compare.ts';
import { harnessRoot, loadSide, type SideModules } from './lib/deps.ts';
import {
  collectImages,
  collectMachine,
  HARNESS_VERSION,
  machineFingerprint,
} from './lib/environment.ts';
import { buildTemplate, dropTemplate, measureCell, type MeasureContext } from './lib/measure.ts';
import { createMongoBackend } from './lib/mongo-backend.ts';
import { createPgBackend } from './lib/pg-backend.ts';
import { renderReport } from './lib/report.ts';
import type { Sample, SideName } from './lib/sample.ts';
import { packagesAreDirty, prepareSide, resolveSha, type PreparedSide } from './lib/side.ts';

const benchmarksRoot = resolve(import.meta.dirname);
const baselineRoot = join(benchmarksRoot, 'baselines');
const cacheRoot = join(benchmarksRoot, '.cache', 'sides');
const log = (message: string): void =>
  console.log(`${new Date().toISOString().slice(11, 19)} ${message}`);

/** The plan of the mode (its descriptor feeds the fingerprint, so `--only` never changes it) and the plan restricted by `--only`. */
function buildPlans(options: CliOptions): { whole: Plan; selected: Plan } {
  const whole = options.mode === 'quick' ? quickPlan(options.pairs ?? 3) : fullPlan(options.pairs);
  return { whole, selected: options.only.length === 0 ? whole : filterPlan(whole, options.only) };
}

function persist(
  sha: string,
  ref: string,
  fingerprint: string,
  fresh: readonly Sample[],
  environment: unknown,
): void {
  if (fresh.length === 0) return;
  const existing = loadBaseline(baselineRoot, fingerprint, sha);
  const samples = mergeSamples(existing?.samples ?? [], fresh);
  const calibrationMs = Math.min(...samples.map((sample) => sample.load.calibrationMs));
  const directory = saveBaseline(
    baselineRoot,
    {
      schema: BASELINE_SCHEMA,
      sha,
      ref,
      fingerprint,
      createdAt: new Date().toISOString(),
      calibrationMs,
      samples,
    },
    environment,
  );
  log(`[baseline] stored ${fresh.length} samples of ${ref} (${sha.slice(0, 8)}) in ${directory}`);
}

async function createBackend(store: Store, modules: SideModules): Promise<Backend> {
  return store === 'pg' ? createPgBackend(modules.pg) : createMongoBackend(modules);
}

function sampleLine(sample: Sample): string {
  const shown = ['deliveriesPerSec', 'eventsPerSec', 'achievedPerSec', 'txP99Ms', 'e2eP99Ms']
    .flatMap((name) =>
      sample.metrics[name] === undefined ? [] : [`${name}=${sample.metrics[name]}`],
    )
    .join(' ');
  return `[sample] ${sample.cellId} ${sample.side} #${sample.pair}: ${shown} calib=${sample.load.calibrationMs}ms ${sample.validation.valid ? 'valid' : `INVALID ${sample.validation.reasons.join('; ')}`}${sample.notes.length > 0 ? ` | ${sample.notes.join('; ')}` : ''}`;
}

async function main(): Promise<number> {
  const options = parseArguments(process.argv.slice(2));
  const started = Date.now();
  const { whole, selected: plan } = buildPlans(options);
  if (plan.cells.length === 0) throw new Error('[cli](main) no cell matches --only');
  const baseSha = resolveSha(harnessRoot, options.base);
  const candidateSha = resolveSha(harnessRoot, options.candidate);
  if (options.candidate === 'HEAD' && packagesAreDirty(harnessRoot))
    log(
      '[cli] warning: packages/ has uncommitted changes; only the committed state of HEAD is measured',
    );

  const machine = collectMachine();
  const cores = cpus().length;
  if (!options.force && loadavg()[0]! > 0.35 * cores) {
    throw new Error(
      `[cli](main) the machine is busy (load average ${loadavg()[0]!.toFixed(2)} on ${cores} cores); wait, or pass --force`,
    );
  }
  const images = collectImages();
  const fingerprint = machineFingerprint(machine, images, whole);
  const wanted = options.useBaseline && !options.interleave;
  const stored = wanted ? loadBaseline(baselineRoot, fingerprint, baseSha) : undefined;
  const usable = wanted
    ? findUsableBaseline(baselineRoot, fingerprint, baseSha, plan.cells)
    : undefined;
  const useStored = usable !== undefined;
  log(
    `[cli] ${options.mode}: ${plan.cells.length} cells, candidate ${options.candidate} (${candidateSha.slice(0, 8)}), base ${options.base} (${baseSha.slice(0, 8)}), fingerprint ${fingerprint}`,
  );
  log(
    useStored
      ? '[cli] stored baseline found: only the candidate is measured'
      : '[cli] no usable stored baseline: base and candidate are measured interleaved',
  );

  const candidate: PreparedSide = await prepareSide(harnessRoot, options.candidate, cacheRoot, log);
  const base: PreparedSide = useStored
    ? candidate
    : await prepareSide(harnessRoot, options.base, cacheRoot, log);
  if (options.prepareOnly) return 0;

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const outDir = join(benchmarksRoot, 'results', 'runs', stamp);
  mkdirSync(outDir, { recursive: true });
  const samplesFile = join(outDir, 'samples.jsonl');
  const environment: Record<string, unknown> = {
    date: new Date().toISOString(),
    harnessVersion: HARNESS_VERSION,
    fingerprint,
    machine,
    images,
    candidate: { ref: options.candidate, sha: candidateSha },
    base: { ref: options.base, sha: baseSha },
    mode: options.mode,
  };
  const writeEnvironment = (): void =>
    writeFileSync(join(outDir, 'env.json'), `${JSON.stringify(environment, null, 2)}\n`);
  writeEnvironment();
  log(`[cli] results -> ${outDir}`);

  const modules: Record<SideName, SideModules> = {
    candidate: await loadSide(candidate.root),
    base: useStored ? await loadSide(candidate.root) : await loadSide(base.root),
  };
  const roots: Record<SideName, string> = { candidate: candidate.root, base: base.root };
  const collected: Sample[] = [];
  const stores = [...new Set(plan.cells.map((cell) => cell.store))];
  for (const store of stores) {
    log(`=== starting ${store} ===`);
    const backend = await createBackend(store, modules.candidate);
    try {
      const description = await backend.describe();
      const roundTrip = await backend.roundTrip();
      environment[`${store}Server`] = { ...description, roundTripMs: roundTrip };
      writeEnvironment();
      const context: MeasureContext = { backend, modules, roots, log };
      const sides: SideName[] = useStored ? ['candidate'] : ['base', 'candidate'];
      const cells = plan.cells.filter((cell) => cell.store === store);
      const templates: TemplateDefinition[] = plan.templates.filter(
        (template) => template.store === store,
      );
      for (const side of sides)
        for (const template of templates) await buildTemplate(context, side, template);
      for (const cell of cells)
        await runCell(context, cell, sides, templates, (sample) => {
          collected.push(sample);
          appendFileSync(samplesFile, `${JSON.stringify(sample)}\n`);
          log(sampleLine(sample));
        });
      for (const side of sides)
        for (const template of templates) await dropTemplate(context, side, template);
    } finally {
      await backend.stop();
    }
  }

  const candidateSamples = collected.filter((sample) => sample.side === 'candidate');
  const baseSamples = useStored
    ? (usable?.samples ?? [])
    : collected.filter((sample) => sample.side === 'base');
  const comparisons: CellComparison[] = plan.cells.map((cell) =>
    compareCell(
      cell.id,
      baseSamples.filter((sample) => sample.cellId === cell.id),
      candidateSamples.filter((sample) => sample.cellId === cell.id),
      { pairing: useStored ? 'unpaired' : 'paired' },
    ),
  );
  const verdict = gate(comparisons, { strict: options.strict, accept: options.accept });
  const notes: string[] = [];
  if (usable !== undefined) {
    const current = Math.min(...candidateSamples.map((sample) => sample.load.calibrationMs));
    const drift = calibrationDrift(usable, current);
    if (drift > 1.15 || drift < 0.85)
      notes.push(
        `The CPU calibration now is ${drift.toFixed(2)}x the one of the stored baseline; the machine state differs. Re-run with --interleave to measure the base again.`,
      );
  }
  if (stored !== undefined && !useStored)
    notes.push(
      'A stored baseline exists but does not cover every planned cell; the base was measured again and the baseline was extended.',
    );
  if (plan.cells.some((cell) => cell.reps < 3))
    notes.push(
      'Cells with fewer than 3 samples have no confidence interval; their throughput ratios are indicative only. Their counters still gate.',
    );
  const report = renderReport({
    mode: `${options.mode}${options.only.length > 0 ? ` (only ${options.only.join(', ')})` : ''}`,
    base: { ref: options.base, sha: baseSha },
    candidate: { ref: options.candidate, sha: candidateSha },
    fingerprint,
    baseSource: useStored ? 'baseline' : 'interleaved',
    comparisons,
    cells: plan.cells,
    gate: verdict,
    notes,
    elapsedMinutes: (Date.now() - started) / 60_000,
  });
  writeFileSync(join(outDir, 'report.md'), report);
  writeFileSync(join(outDir, 'comparison.json'), `${JSON.stringify(comparisons, null, 2)}\n`);
  if (options.store) {
    if (!useStored) persist(baseSha, options.base, fingerprint, baseSamples, environment);
    if (candidateSha !== baseSha)
      persist(candidateSha, options.candidate, fingerprint, candidateSamples, environment);
  }
  log(`[cli] gate ${verdict.passed ? 'PASS' : 'FAIL'}; report: ${join(outDir, 'report.md')}`);
  for (const reason of verdict.reasons) log(`[cli]   ${reason}`);
  return verdict.passed ? 0 : 1;
}

/** Runs the samples of one cell. With two sides the samples alternate, and the side that goes first alternates per pair. */
async function runCell(
  context: MeasureContext,
  cell: Cell,
  sides: readonly SideName[],
  templates: readonly TemplateDefinition[],
  emit: (sample: Sample) => void,
): Promise<void> {
  for (let pair = 0; pair < cell.reps; pair += 1) {
    const order = sides.length === 2 && pair % 2 === 1 ? [...sides].reverse() : sides;
    for (const side of order) emit(await measureCell(context, cell, side, pair, templates));
  }
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error instanceof Error ? (error.stack ?? error.message) : error);
    process.exit(2);
  },
);
