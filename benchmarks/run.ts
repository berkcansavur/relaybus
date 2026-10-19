// One-command benchmark: `pnpm bench` (from benchmarks/) or `node benchmarks/run.ts`.
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createMongoBackend, createPgBackend, type Backend } from './lib/backends.ts';
import { execFileSync } from 'node:child_process';
import { collectEnvironment } from './lib/env.ts';
import {
  buildTemplate,
  drainRun,
  mixedRun,
  publishRun,
  type Ctx,
  type DrainSpec,
  type MixedSpec,
} from './lib/scenarios.ts';

const args = process.argv.slice(2);
const flag = (name: string, fallback?: string): string | undefined => {
  const index = args.indexOf(`--${name}`);
  return index === -1 ? fallback : (args[index + 1] ?? fallback);
};
const quick = args.includes('--quick');
const skipPublish = args.includes('--skip-publish'); // scenario 3 dispatch only, when publish rows already exist
const small = args.includes('--small'); // fewer dispatch configurations (used for the sync-commit diagnostic)
const stores = (flag('store', 'all') === 'all' ? ['pg', 'mongo'] : [flag('store')!]) as Array<'pg' | 'mongo'>;
const wanted = new Set((flag('scenarios', '1,2,3,4,5') ?? '').split(',').map((s) => s.trim()));

const N = Number(flag('events', quick ? '20000' : '200000'));
const PUBLISH_WARMUP = quick ? 2_000 : 5_000;
const PUBLISH_DURATION = quick ? 5_000 : 15_000;
const MIXED_WARMUP = quick ? 4_000 : 8_000;
const MIXED_DURATION = quick ? 10_000 : 40_000;
const DRAIN_CAP = quick ? 30_000 : 90_000;

const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const outDir = resolve(fileURLToPath(new URL('.', import.meta.url)), 'results', flag('out', stamp)!);
mkdirSync(outDir, { recursive: true });
const rowsFile = resolve(outDir, 'rows.jsonl');
const log = (message: string): void => console.log(`${new Date().toISOString().slice(11, 19)} ${message}`);
const ctxFor = (be: Backend): Ctx => ({
  be,
  log,
  emit: (row) => appendFileSync(rowsFile, `${JSON.stringify(row)}\n`),
});

const env: Record<string, any> = collectEnvironment();
env['arguments'] = { quick, N, stores, scenarios: [...wanted] };
writeFileSync(resolve(outDir, 'env.json'), JSON.stringify(env, null, 2));
log(`results -> ${outDir}`);

const DEFAULT_POOL: Record<string, number> = { pg: 10, mongo: 100 };

async function scenarioPublish(ctx: Ctx): Promise<void> {
  const be = ctx.be;
  const base = { warmupMs: PUBLISH_WARMUP, durationMs: PUBLISH_DURATION };
  for (const publishers of [1, 8, 32]) {
    for (const batch of [1, 100]) {
      await publishRun(ctx, 's1-publish', {
        ...base, label: `P=${publishers} batch=${batch} pool=default`, procs: 1, loops: publishers, batch, handlers: 1, pool: DEFAULT_POOL[be.name]!,
      });
    }
  }
  if (be.name === 'pg') {
    for (const publishers of [8, 32]) {
      for (const batch of [1, 100]) {
        await publishRun(ctx, 's1-publish', {
          ...base, label: `P=${publishers} batch=${batch} pool=${publishers + 2}`, procs: 1, loops: publishers, batch, handlers: 1, pool: publishers + 2,
        });
      }
    }
  }
  // Ceiling attempt: four publisher processes, so the Node client is not the limit.
  for (const batch of [1, 100]) {
    await publishRun(ctx, 's1-publish', {
      ...base, label: `ceiling 4 procs x 8 publishers batch=${batch}`, procs: 4, loops: 8, batch, handlers: 1, pool: 12,
    });
  }
  // Fan-out on the publish side (scenario 3, publish half).
  for (const batch of [1, 100]) {
    await publishRun(ctx, 's3-fanout-publish', {
      ...base, label: `P=8 batch=${batch} handlers=5`, procs: 1, loops: 8, batch, handlers: 5, pool: Math.max(10, DEFAULT_POOL[be.name]! === 100 ? 100 : 10),
    });
  }
}

const TUNED = { concurrency: 200, batchSize: 200, pool: 40, pollMs: 50 };
const drainConfigs = (store: string): Array<Omit<DrainSpec, 'instances' | 'handlers' | 'expected' | 'maxDrainMs' | 'label'> & { name: string }> => {
  const all = [
    { name: 'defaults (conc 10, batch 10, pool 10, poll 1s)', concurrency: 10, batchSize: 10, pool: DEFAULT_POOL[store]!, pollMs: 1000 },
    { name: 'conc 10, batch 50', concurrency: 10, batchSize: 50, pool: DEFAULT_POOL[store]!, pollMs: 1000 },
    { name: 'conc 50, batch 50, pool 20', concurrency: 50, batchSize: 50, pool: 20, pollMs: 1000 },
    { name: 'conc 50, batch 200, pool 20', concurrency: 50, batchSize: 200, pool: 20, pollMs: 1000 },
    { name: 'tuned (conc 200, batch 200, pool 40, poll 50ms)', ...TUNED },
    { name: 'tuned+ (conc 400, batch 400, pool 64, poll 50ms)', concurrency: 400, batchSize: 400, pool: 64, pollMs: 50 },
  ];
  if (small) return [all[0]!, all[4]!];
  return store === 'mongo' ? [all[0]!, all[2]!, all[4]!] : all;
};

async function scenarioDrain(ctx: Ctx): Promise<void> {
  const be = ctx.be;
  const instancesList = be.name === 'pg' && !small ? [1, 4, 8] : [1, 4];
  if (wanted.has('2')) {
    await buildTemplate(ctx, 's2-dispatch', 'tpl_unkeyed', { events: N, handlers: 1, keys: { mode: 'none', count: 0 }, procs: 2, loops: 4, batch: 500, pool: 10 });
    for (const instances of instancesList) {
      for (const config of drainConfigs(be.name)) {
        const { name, ...settings } = config;
        await drainRun(ctx, 's2-dispatch', 'tpl_unkeyed', {
          label: `${instances} inst, ${name}`, instances, handlers: 1, expected: N, maxDrainMs: DRAIN_CAP, ...settings,
        });
      }
    }
    await be.drop('tpl_unkeyed');
  }
  if (wanted.has('3')) {
    const events = Math.floor(N / 5);
    // MongoDB aborts transactions that run longer than 60 s, so its fan-out preload uses smaller batches.
    await buildTemplate(ctx, 's3-fanout', 'tpl_fanout', { events, handlers: 5, keys: { mode: 'none', count: 0 }, procs: 2, loops: 4, batch: be.name === 'mongo' ? 100 : 500, pool: 10 });
    for (const [instances, config] of [[1, 0], [1, 4], [4, 4], [8, 4]] as Array<[number, number]>) {
      if (be.name === 'mongo' && instances === 8) continue;
      const { name, ...settings } = drainConfigs('pg')[config]!;
      await drainRun(ctx, 's3-fanout', 'tpl_fanout', {
        label: `5 handlers, ${instances} inst, ${name}`, instances, handlers: 5, expected: events * 5, maxDrainMs: DRAIN_CAP, ...settings,
      });
    }
    await be.drop('tpl_fanout');
  }
}

async function scenarioKeyed(ctx: Ctx): Promise<void> {
  const be = ctx.be;
  const base = { warmupMs: PUBLISH_WARMUP, durationMs: PUBLISH_DURATION };
  for (const keys of [{ mode: 'distinct', count: 1000 }, { mode: 'hot', count: 10 }, { mode: 'hot', count: 1 }] as const) {
    await publishRun(ctx, 's4-keyed-publish', {
      ...base, label: `P=8 batch=1 keys=${keys.mode}:${keys.count}`, procs: 1, loops: 8, batch: 1, handlers: 1, pool: 10, keys: { ...keys },
    });
  }
  const shapes = [
    { name: 'tpl_k1000', events: Math.floor(N / 2), keys: { mode: 'distinct' as const, count: 1000 } },
    { name: 'tpl_k10', events: Math.floor(N / 10), keys: { mode: 'hot' as const, count: 10 } },
  ];
  for (const shape of shapes) {
    await buildTemplate(ctx, 's4-keyed', shape.name, { events: shape.events, handlers: 1, keys: shape.keys, procs: 2, loops: 8, batch: 1, pool: 12 });
    for (const [instances, config] of [[1, 0], [1, 4], [4, 4]] as Array<[number, number]>) {
      const { name, ...settings } = drainConfigs('pg')[config]!;
      await drainRun(ctx, 's4-keyed', shape.name, {
        label: `keys=${shape.keys.mode}:${shape.keys.count}, ${instances} inst, ${name}`, instances, handlers: 1, expected: shape.events,
        maxDrainMs: quick ? DRAIN_CAP : 120_000, keys: `${shape.keys.mode}:${shape.keys.count}`, ...settings,
      });
    }
    await be.drop(shape.name);
  }
}

async function scenarioMixed(ctx: Ctx): Promise<void> {
  const be = ctx.be;
  const common = { warmupMs: MIXED_WARMUP, durationMs: MIXED_DURATION, recoveryMaxMs: 60_000, maxInflight: 256 };
  const tuned = { instances: 4, concurrency: 100, batchSize: 100, pool: 30, pollMs: 100 };
  const defaults = { instances: 1, concurrency: 10, batchSize: 10, pool: DEFAULT_POOL[be.name]!, pollMs: 1000 };
  const plan: Array<[string, number, 'tuned' | 'defaults']> =
    be.name === 'pg'
      ? [['defaults', 1000, 'defaults'], ['defaults', 5000, 'defaults'], ['tuned', 1000, 'tuned'], ['tuned', 5000, 'tuned'], ['tuned', 10000, 'tuned']]
      : [['defaults', 1000, 'defaults'], ['tuned', 1000, 'tuned'], ['tuned', 5000, 'tuned']];
  for (const [name, rate, kind] of plan) {
    const spec: MixedSpec = {
      label: `${rate}/s, ${name} dispatchers`, rate, pubProcs: kind === 'tuned' ? 4 : 2, pubPool: kind === 'tuned' ? 16 : 10,
      ...(kind === 'tuned' ? tuned : defaults), ...common,
    };
    await mixedRun(ctx, 's5-mixed', spec);
  }
}

for (const store of stores) {
  log(`=== starting ${store} ===`);
  const be = store === 'pg' ? await createPgBackend() : await createMongoBackend();
  try {
    const description = await be.describe();
    const rtt = await be.rtt();
    let limits = 'unavailable';
    try {
      limits = execFileSync('docker', ['inspect', '--format', 'memory={{.HostConfig.Memory}} nanoCpus={{.HostConfig.NanoCpus}} shm={{.HostConfig.ShmSize}} cpuShares={{.HostConfig.CpuShares}}', be.containerId], { encoding: 'utf8' }).trim();
    } catch {
      // docker CLI missing
    }
    env[`${store}Server`] = { ...description, roundTripMs: rtt, containerLimits: limits };
    writeFileSync(resolve(outDir, 'env.json'), JSON.stringify(env, null, 2));
    log(`${JSON.stringify(description.server)} ping p50 ${rtt.p50.toFixed(3)} ms p99 ${rtt.p99.toFixed(3)} ms`);
    const ctx = ctxFor(be);
    if (!skipPublish && (wanted.has('1') || wanted.has('3'))) await scenarioPublish(ctx);
    if (wanted.has('2') || wanted.has('3')) await scenarioDrain(ctx);
    if (wanted.has('4')) await scenarioKeyed(ctx);
    if (wanted.has('5')) await scenarioMixed(ctx);
  } finally {
    await be.stop();
  }
}
const rows = readFileSync(rowsFile, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
const checked = rows.filter((row) => row.validation !== undefined);
const invalid = checked.filter((row) => !row.validation.valid);
log(`validation: ${checked.length - invalid.length} of ${checked.length} drain/mixed runs valid`);
for (const row of invalid) log(`INVALID ${row.store} ${row.scenario} ${row.label}: ${row.validation.reasons.join('; ')}`);
log(`done. node benchmarks/report.ts ${outDir}`);
process.exit(invalid.length > 0 ? 2 : 0);
