// Runs one cell once on one side and turns what happened into a Sample. Every measurement uses a fixed window after a
// warm-up; nothing waits for a drain to complete. Exactness validation (lost / duplicate / order / open) runs on every
// dispatch and mixed sample, and publish samples check that the database holds exactly the rows that were published.
import type { Backend } from './backends.ts';
import type { Cell, DispatchCell, MixedCell, PublishCell, TemplateDefinition } from './cells.ts';
import { NO_KEYS, ROUTED_ACK_MS } from './cells.ts';
import { counterMetrics } from './db-metrics.ts';
import type { SideModules } from './deps.ts';
import { WORKLOAD_SEED } from './deterministic.ts';
import { Fleet, nowMs, sleep, sleepUntil } from './fleet.ts';
import { Hist } from './hist.ts';
import { LoadMeter } from './load.ts';
import type {
  DispatcherResult,
  FromWorker,
  HandledItem,
  PublisherResult,
  WorkerInit,
  WorkerResult,
} from './protocol.ts';
import type { Sample, SideName, Validation } from './sample.ts';
import { validateRun } from './validate.ts';

export interface MeasureContext {
  readonly backend: Backend;
  readonly modules: Readonly<Record<SideName, SideModules>>;
  readonly roots: Readonly<Record<SideName, string>>;
  readonly log: (message: string) => void;
}

let databaseCounter = 0;
const nextDatabase = (prefix: string): string => `${prefix}_${++databaseCounter}`;
const round = (value: number, digits = 1): number =>
  Math.round(value * 10 ** digits) / 10 ** digits;

export const templateDatabase = (side: SideName, template: TemplateDefinition): string =>
  `tpl_${side}_${template.name.replace(/-/g, '_')}`;

const publisherResults = (results: readonly WorkerResult[]): PublisherResult[] =>
  results.filter((result): result is PublisherResult => result.kind === 'publisher');
const dispatcherResults = (results: readonly WorkerResult[]): DispatcherResult[] =>
  results.filter((result): result is DispatcherResult => result.kind === 'dispatcher');

function mergedHist(results: readonly PublisherResult[], field: 'pubHist' | 'txHist'): Hist {
  const hist = new Hist();
  for (const result of results) hist.merge(result[field]);
  return hist;
}

function publisherInits(
  context: MeasureContext,
  database: string,
  cell: Pick<
    PublishCell,
    'procs' | 'loops' | 'batch' | 'handlers' | 'keys' | 'pool' | 'warmupMs' | 'durationMs'
  > & { readonly routed?: boolean },
  maxEvents?: number,
): WorkerInit[] {
  const connection = context.backend.connection(database);
  return Array.from({ length: cell.procs }, (_, w) => ({
    role: 'publisher' as const,
    backend: context.backend.name,
    ...connection,
    cfg: {
      w,
      loops: cell.loops,
      batch: cell.batch,
      handlers: cell.handlers,
      keys: cell.keys,
      seed: WORKLOAD_SEED,
      ...(cell.routed === true ? { routeAckMs: ROUTED_ACK_MS } : {}),
      pool: cell.pool,
      warmupMs: cell.warmupMs,
      durationMs: cell.durationMs,
      rate: 0,
      maxInflight: 0,
      trackCommits: false,
      ...(maxEvents === undefined ? {} : { maxEvents }),
    },
  }));
}

/** Loads a template database with the side's own code, so its schema and rows are exactly what that side writes. */
export async function buildTemplate(
  context: MeasureContext,
  side: SideName,
  template: TemplateDefinition,
): Promise<void> {
  const { backend } = context;
  const name = templateDatabase(side, template);
  await backend.createFresh(context.modules[side], name);
  const perProcess = Math.floor(template.events / template.procs);
  const fleet = new Fleet(
    context.roots[side],
    publisherInits(context, name, { ...template, warmupMs: 0, durationMs: 3_600_000 }, perProcess),
  );
  try {
    await fleet.ready();
    const startAt = nowMs() + 300;
    fleet.go(startAt);
    const results = publisherResults(await fleet.done());
    const seconds = (nowMs() - startAt) / 1000;
    const loaded = results.reduce((total, result) => total + result.events, 0);
    const errors = results.reduce((total, result) => total + result.errors, 0);
    if (errors > 0)
      throw new Error(
        `[measure](buildTemplate) loading ${name} had ${errors} errors: ${results.flatMap((result) => result.errorSamples).join('; ')}`,
      );
    await backend.afterLoad(name);
    const counts = await backend.countByStatus(name);
    context.log(
      `[template] ${side} ${template.name}: ${loaded} events, ${counts['pending'] ?? 0} pending deliveries in ${round(seconds)} s`,
    );
  } finally {
    fleet.kill();
  }
}

export async function dropTemplate(
  context: MeasureContext,
  side: SideName,
  template: TemplateDefinition,
): Promise<void> {
  await context.backend.drop(templateDatabase(side, template)).catch(() => undefined);
}

interface SampleParts {
  readonly cell: Cell;
  readonly side: SideName;
  readonly pair: number;
  readonly meter: LoadMeter;
  readonly startedAt: string;
  readonly metrics: Record<string, number | null>;
  readonly validation: Validation;
  readonly dbCpuAvgPct: number | null;
  readonly notes: string[];
}

function assemble(parts: SampleParts): Sample {
  return {
    cellId: parts.cell.id,
    side: parts.side,
    pair: parts.pair,
    startedAt: parts.startedAt,
    metrics: parts.metrics,
    validation: parts.validation,
    load: parts.meter.finish(parts.dbCpuAvgPct),
    notes: parts.notes,
  };
}

// ---------------------------------------------------------------- publish

async function measurePublish(
  context: MeasureContext,
  cell: PublishCell,
  side: SideName,
  pair: number,
): Promise<Sample> {
  const { backend } = context;
  const database = nextDatabase('pub');
  await backend.createFresh(context.modules[side], database);
  const meter = new LoadMeter();
  const startedAt = new Date().toISOString();
  const fleet = new Fleet(context.roots[side], publisherInits(context, database, cell));
  try {
    await fleet.ready();
    const monitor = backend.monitor(database);
    // The counters cover the whole sample, from before the first statement to after the last worker exited, so that
    // lazily flushed statistics and the counted rows describe exactly the same work.
    await monitor.begin();
    const startAt = nowMs() + 500;
    const measureStart = startAt + cell.warmupMs;
    const measureEnd = measureStart + cell.durationMs;
    fleet.go(startAt);
    await sleepUntil(measureEnd);
    const results = publisherResults(await fleet.done());
    const window = await monitor.end();
    const events = results.reduce((total, result) => total + result.events, 0);
    const txs = results.reduce((total, result) => total + result.txs, 0);
    const errors = results.reduce((total, result) => total + result.errors, 0);
    const seconds = cell.durationMs / 1000;
    const reasons: string[] = [];
    if (errors > 0)
      reasons.push(
        `${errors} publish errors: ${results
          .flatMap((result) => result.errorSamples)
          .slice(0, 2)
          .join('; ')}`,
      );
    if (window.eventsInserted === 0) reasons.push('the database saw no inserted events');
    if (window.deliveriesInserted !== window.eventsInserted * cell.handlers) {
      reasons.push(
        `${window.deliveriesInserted} deliveries stored for ${window.eventsInserted} events x ${cell.handlers} handlers`,
      );
    }
    const metrics: Record<string, number | null> = {
      eventsPerSec: round(events / seconds),
      deliveriesPerSec: round((events * cell.handlers) / seconds),
      txP99Ms: round(mergedHist(results, 'txHist').quantile(0.99), 2),
      publishP99Ms: round(mergedHist(results, 'pubHist').quantile(0.99), 2),
      'info.txPerSec': round(txs / seconds),
      'info.clientCores': round(
        results.reduce((total, result) => total + result.cpuMs, 0) / cell.durationMs,
        2,
      ),
      'info.loopDelayP99Ms': round(Math.max(...results.map((result) => result.loopDelayP99)), 1),
      ...counterMetrics(window, window.eventsInserted),
    };
    return assemble({
      cell,
      side,
      pair,
      meter,
      startedAt,
      metrics,
      validation: { valid: reasons.length === 0, reasons },
      dbCpuAvgPct: window.dockerCpuAvgPct,
      notes: window.waits === '' ? [] : [`waits: ${window.waits}`],
    });
  } finally {
    fleet.kill();
    await backend.drop(database).catch(() => undefined);
  }
}

// ---------------------------------------------------------------- dispatch

function dispatcherInits(
  context: MeasureContext,
  database: string,
  cell: DispatchCell | MixedCell,
  handlers: number,
  routed = false,
): WorkerInit[] {
  const connection = context.backend.connection(database);
  return Array.from({ length: cell.instances }, () => ({
    role: 'dispatcher' as const,
    backend: context.backend.name,
    ...connection,
    cfg: {
      handlers,
      concurrency: cell.concurrency,
      batchSize: cell.batchSize,
      pollMs: cell.pollMs,
      pool: cell.pool,
      trackHandled: true,
      ...(routed ? { routeAckMs: ROUTED_ACK_MS } : {}),
    },
  }));
}

async function measureDispatch(
  context: MeasureContext,
  cell: DispatchCell,
  template: TemplateDefinition,
  side: SideName,
  pair: number,
): Promise<Sample> {
  const { backend } = context;
  const database = nextDatabase('disp');
  await backend.clone(context.modules[side], templateDatabase(side, template), database);
  const meter = new LoadMeter();
  const startedAt = new Date().toISOString();
  const handledAll: HandledItem[] = [];
  const fleet = new Fleet(
    context.roots[side],
    dispatcherInits(context, database, cell, cell.handlers, cell.routed === true),
    (message: FromWorker) => {
      if (message.t === 'handled') for (const item of message.items) handledAll.push(item);
    },
  );
  try {
    await fleet.ready();
    const monitor = backend.monitor(database);
    // The statistics are reset while the workers are idle, so none of their pending statistics leaks across the reset.
    await monitor.begin();
    const startAt = nowMs() + 300;
    const measureStart = startAt + cell.warmupMs;
    const capAt = measureStart + cell.durationMs;
    const target = cell.targetDeliveries;
    let measureEnd = capAt;
    let reachedTarget = true;
    let handledAtEnd = 0;
    fleet.go(startAt);
    if (target === undefined) await sleepUntil(measureEnd);
    else {
      // Fixed work: the counters cover the first `target` deliveries, which is the same data on every machine and side.
      while (fleet.handledTotal < target && nowMs() < capAt) await sleep(10);
      reachedTarget = fleet.handledTotal >= target;
      measureEnd = nowMs();
      handledAtEnd = fleet.handledTotal;
    }
    fleet.stop();
    const results = dispatcherResults(await fleet.done());
    const window = await monitor.end();
    const expected = template.events * cell.handlers;
    const drained = results.reduce((total, result) => total + result.handled, 0) >= expected;
    const lastAt = Math.max(...results.map((result) => result.lastAt));
    const effectiveEnd = drained ? Math.min(measureEnd, lastAt) : measureEnd;
    const handledInWindow = handledAll.filter(
      (item) => item[2] >= measureStart && item[2] < effectiveEnd,
    ).length;
    const seconds = (effectiveEnd - measureStart) / 1000;
    const validation = validateRun({
      handlers: cell.handlers,
      handled: handledAll,
      data: await backend.validationData(database),
      drained,
      leaseLost: results.reduce((total, result) => total + result.leaseLost, 0),
      routed: cell.routed === true,
    });
    const reasons = [...validation.reasons];
    if (!reachedTarget)
      reasons.push(
        `only ${handledAtEnd} of the ${target ?? 0} deliveries of the fixed work were handled before the time cap; counters per delivery are not comparable`,
      );
    // A fixed-work sample is as short as the machine is fast; only a fixed window can be too short for its template.
    if (target === undefined && seconds < 1)
      reasons.push(
        `the template drained ${round(seconds, 2)} s into the window; it is too small for this cell`,
      );
    const notes = drained
      ? [`template drained inside the window; rate measured over ${round(seconds)} s`]
      : [];

    if (window.waits !== '') notes.push(`waits: ${window.waits}`);
    const metrics: Record<string, number | null> = {
      deliveriesPerSec: seconds > 0 ? round(handledInWindow / seconds) : null,
      'info.clientCores': round(
        results.reduce((total, result) => total + result.cpuMs, 0) /
          1000 /
          Math.max(1, (measureEnd - startAt) / 1000),
        2,
      ),
      'info.loopDelayP99Ms': round(Math.max(...results.map((result) => result.loopDelayP99)), 1),
      'info.failures': results.reduce((total, result) => total + result.failureCount, 0),
      ...counterMetrics(window, window.deliveriesCompleted),
    };
    return assemble({
      cell,
      side,
      pair,
      meter,
      startedAt,
      metrics,
      validation: { valid: reasons.length === 0, reasons },
      dbCpuAvgPct: window.dockerCpuAvgPct,
      notes,
    });
  } finally {
    fleet.kill();
    await backend.drop(database).catch(() => undefined);
  }
}

// ---------------------------------------------------------------- mixed

async function measureMixed(
  context: MeasureContext,
  cell: MixedCell,
  side: SideName,
  pair: number,
): Promise<Sample> {
  const { backend } = context;
  const database = nextDatabase('mixed');
  await backend.createFresh(context.modules[side], database);
  const meter = new LoadMeter();
  const startedAt = new Date().toISOString();
  const connection = backend.connection(database);
  const commitAt = new Map<number, number>();
  const handledAt = new Map<number, number>();
  const identity = (w: number, n: number): number => w * 1e10 + n;
  const startAt = nowMs() + 1500;
  const measureStart = startAt + cell.warmupMs;
  const measureEnd = measureStart + cell.durationMs;
  const latency = new Hist();
  const record = (commitTs: number, handledTs: number): void => {
    if (handledTs < measureStart || handledTs >= measureEnd) return;
    latency.add(Math.max(0, handledTs - commitTs));
  };
  let committed = 0;
  const handledAll: HandledItem[] = [];
  const publishers = new Fleet(
    context.roots[side],
    Array.from({ length: cell.pubProcs }, (_, w) => ({
      role: 'publisher' as const,
      backend: backend.name,
      ...connection,
      cfg: {
        w,
        loops: 0,
        batch: 1,
        handlers: 1,
        keys: NO_KEYS,
        seed: WORKLOAD_SEED,
        pool: cell.pubPool,
        warmupMs: cell.warmupMs,
        durationMs: cell.durationMs,
        rate: cell.rate / cell.pubProcs,
        maxInflight: cell.maxInflight,
        trackCommits: true,
      },
    })),
    (message) => {
      if (message.t !== 'commits') return;
      for (const [n, ts] of message.items) {
        committed += 1;
        const key = identity(message.w, n);
        const handled = handledAt.get(key);
        if (handled === undefined) commitAt.set(key, ts);
        else {
          handledAt.delete(key);
          record(ts, handled);
        }
      }
    },
  );
  const dispatchers = new Fleet(
    context.roots[side],
    dispatcherInits(context, database, cell, 1),
    (message) => {
      if (message.t !== 'handled') return;
      for (const item of message.items) handledAll.push(item);
      for (const [w, code, ts] of message.items) {
        const key = identity(w, Math.floor(code / 16));
        const commit = commitAt.get(key);
        if (commit === undefined) handledAt.set(key, ts);
        else {
          commitAt.delete(key);
          record(commit, ts);
        }
      }
    },
  );
  let backlogMax = 0;
  let sampling = true;
  try {
    await Promise.all([publishers.ready(), dispatchers.ready()]);
    const monitor = backend.monitor(database);
    await monitor.begin();
    publishers.go(startAt);
    dispatchers.go(startAt);
    const sampler = (async () => {
      while (sampling) {
        await sleep(1000);
        const at = nowMs();
        if (at >= measureStart && at <= measureEnd)
          backlogMax = Math.max(backlogMax, committed - dispatchers.handledTotal);
      }
    })();
    await sleepUntil(measureEnd);
    const publisherSide = publisherResults(await publishers.done());
    const window = await monitor.end();
    const drainStart = nowMs();
    while (committed > dispatchers.handledTotal && nowMs() - drainStart < cell.recoveryMaxMs)
      await sleep(100);
    const recovered = committed <= dispatchers.handledTotal;
    const recoverySeconds = recovered ? (nowMs() - drainStart) / 1000 : null;
    sampling = false;
    await sampler;
    dispatchers.stop();
    const dispatcherSide = dispatcherResults(await dispatchers.done());
    const validation = validateRun({
      handlers: 1,
      handled: handledAll,
      data: await backend.validationData(database),
      drained: recovered,
      leaseLost: dispatcherSide.reduce((total, result) => total + result.leaseLost, 0),
    });
    const events = publisherSide.reduce((total, result) => total + result.events, 0);
    const seconds = cell.durationMs / 1000;
    const metrics: Record<string, number | null> = {
      achievedPerSec: round(events / seconds),
      e2eP99Ms: round(latency.quantile(0.99), 1),
      'info.e2eP50Ms': round(latency.quantile(0.5), 1),
      'info.backlogMax': backlogMax,
      'info.recoverySeconds': recoverySeconds === null ? null : round(recoverySeconds),
      'info.shedPerSec': round(
        publisherSide.reduce((total, result) => total + result.dropped, 0) / seconds,
      ),
    };
    return assemble({
      cell,
      side,
      pair,
      meter,
      startedAt,
      metrics,
      validation,
      dbCpuAvgPct: window.dockerCpuAvgPct,
      notes: recovered ? [] : ['backlog did not recover within the recovery wait'],
    });
  } finally {
    sampling = false;
    publishers.kill();
    dispatchers.kill();
    await backend.drop(database).catch(() => undefined);
  }
}

/** Runs one sample of a cell. `template` is required for dispatch cells. */
export async function measureCell(
  context: MeasureContext,
  cell: Cell,
  side: SideName,
  pair: number,
  templates: readonly TemplateDefinition[],
): Promise<Sample> {
  if (cell.kind === 'publish') return measurePublish(context, cell, side, pair);
  if (cell.kind === 'mixed') return measureMixed(context, cell, side, pair);
  const template = templates.find((candidate) => candidate.name === cell.template);
  if (template === undefined)
    throw new Error(`[measure](measureCell) cell ${cell.id} needs template ${cell.template}`);
  return measureDispatch(context, cell, template, side, pair);
}
