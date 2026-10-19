import type { Backend } from './backends.ts';
import { validateRun, type Validation } from './validate.ts';
import { Fleet, nowMs, sleep, sleepUntil } from './fleet.ts';
import { Hist } from './hist.ts';

export interface Ctx {
  be: Backend;
  emit(row: Record<string, unknown>): void;
  log(message: string): void;
}

let dbCounter = 0;
const nextDb = (prefix: string): string => `${prefix}_${++dbCounter}`;
const r1 = (v: number): number => Math.round(v * 10) / 10;
const r2 = (v: number): number => Math.round(v * 100) / 100;
const r0 = (v: number): number => Math.round(v);
const r3 = (v: number): number => Math.round(v * 1000) / 1000;

export type KeySpec = { mode: 'none' | 'distinct' | 'hot'; count: number };
const NO_KEYS: KeySpec = { mode: 'none', count: 0 };

function mergeHists(results: any[], field: string): Hist {
  const hist = new Hist();
  for (const result of results) hist.merge(result[field]);
  return hist;
}

const pct = (h: Hist): Record<string, number> => {
  const s = h.summary();
  return { p50: r2(s.p50), p95: r2(s.p95), p99: r2(s.p99), max: r1(s.max) };
};

function tableStat(db: any, table: string): Record<string, any> {
  return db.tables?.find((t: any) => String(t.table).endsWith(table)) ?? {};
}

function statement(db: any, kind: string): Record<string, any> {
  return db.statements?.find((s: any) => s.kind === kind) ?? {};
}

/** Writes per second and WAL volume derived from the database-side counters of one measured window. */
function pgDerived(db: any, events: number, deliveries: number): Record<string, unknown> {
  if (db.walBytes === undefined) return {};
  const deliveriesT = tableStat(db, 'deliveries');
  const lanesT = tableStat(db, 'lanes');
  return {
    deliveriesUpdates: deliveriesT.upd ?? null,
    deliveriesDeletes: deliveriesT.del ?? null,
    lanesUpdates: lanesT.upd ?? null,
    lanesHotUpdPct: lanesT.upd > 0 ? r1((100 * lanesT.hot_upd) / lanesT.upd) : null,
    laneUpdatesPerDelivery: lanesT.upd > 0 && deliveries > 0 ? r3(lanesT.upd / deliveries) : null,
    walMiB: r1(db.walBytes / 1048576),
    walBytesPerEvent: events > 0 ? r0(db.walBytes / events) : null,
    walBytesPerDelivery: deliveries > 0 ? r0(db.walBytes / deliveries) : null,
    walRecordsPerEvent: events > 0 ? r1(db.walRecords / events) : null,
    fpiPct: db.walRecords > 0 ? r1((100 * db.walFpi) / db.walRecords) : null,
    dbCpuAvgPct: db.docker.cpuAvgPct,
    dbCpuMaxPct: db.docker.cpuMaxPct,
    activeBackends: db.activeBackendsAvg,
    topWaits: (db.waits ?? []).slice(0, 3).map((w: any) => `${w.event} ${w.pct}%`).join(', '),
    hotUpdPct: deliveriesT.upd > 0 ? r1((100 * deliveriesT.hot_upd) / deliveriesT.upd) : null,
    deadTuples: deliveriesT.dead ?? null,
    deliveriesTableMiB: deliveriesT.table_bytes === undefined ? null : r1(deliveriesT.table_bytes / 1048576),
    deliveriesIndexMiB: deliveriesT.index_bytes === undefined ? null : r1(deliveriesT.index_bytes / 1048576),
  };
}

function mongoDerived(db: any, events: number, deliveries: number): Record<string, unknown> {
  if (db.oplogBytes === undefined) return {};
  const ops = db.opcounters;
  return {
    oplogMiB: r1(db.oplogBytes / 1048576),
    oplogEntries: db.oplogEntries ?? null,
    oplogEntriesPerDelivery: db.oplogEntries !== undefined && deliveries > 0 ? r2(db.oplogEntries / deliveries) : null,
    oplogBytesPerEvent: events > 0 ? r0(db.oplogBytes / events) : null,
    oplogBytesPerDelivery: deliveries > 0 ? r0(db.oplogBytes / deliveries) : null,
    journalBytesPerEvent: events > 0 ? r0(db.journalBytes / events) : null,
    journalBytesPerDelivery: deliveries > 0 ? r0(db.journalBytes / deliveries) : null,
    opsPerDelivery: deliveries > 0 ? r1((ops.insert + ops.update + ops.delete + ops.query + ops.getmore + ops.command) / deliveries) : null,
    updatesPerDelivery: deliveries > 0 ? r2(ops.update / deliveries) : null,
    txnAborted: db.txnAborted,
    writeConflicts: db.writeConflicts,
    dbCpuAvgPct: db.docker.cpuAvgPct,
    dbCpuMaxPct: db.docker.cpuMaxPct,
  };
}

const derive = (be: Backend, db: any, events: number, deliveries: number): Record<string, unknown> =>
  be.name === 'pg' ? pgDerived(db, events, deliveries) : mongoDerived(db, events, deliveries);

// ---------------------------------------------------------------- publish-only

export interface PublishSpec {
  label: string;
  procs: number;
  loops: number;
  batch: number;
  handlers: number;
  pool: number;
  keys?: KeySpec;
  warmupMs: number;
  durationMs: number;
}

export async function publishRun(ctx: Ctx, scenario: string, spec: PublishSpec): Promise<Record<string, any>> {
  const { be } = ctx;
  const database = nextDb('pub');
  await be.createFresh(database);
  const connection = be.connection(database);
  const inits = Array.from({ length: spec.procs }, (_, w) => ({
    role: 'publisher',
    backend: be.name,
    ...connection,
    cfg: {
      w,
      loops: spec.loops,
      batch: spec.batch,
      handlers: spec.handlers,
      keys: spec.keys ?? NO_KEYS,
      pool: spec.pool,
      warmupMs: spec.warmupMs,
      durationMs: spec.durationMs,
      rate: 0,
      maxInflight: 0,
      trackCommits: false,
    },
  }));
  const fleet = new Fleet(inits);
  try {
    await fleet.ready();
    const monitor = be.monitor(database);
    const startAt = nowMs() + 500;
    const measureStart = startAt + spec.warmupMs;
    const measureEnd = measureStart + spec.durationMs;
    fleet.go(startAt);
    await sleepUntil(measureStart - 200);
    await monitor.begin();
    await sleepUntil(measureEnd);
    const db = await monitor.end(false);
    const results = await fleet.done();
    const events = results.reduce((a, r) => a + r.events, 0);
    const txs = results.reduce((a, r) => a + r.txs, 0);
    const seconds = spec.durationMs / 1000;
    const row = {
      scenario,
      store: be.name,
      label: spec.label,
      procs: spec.procs,
      publishers: spec.procs * spec.loops,
      batch: spec.batch,
      handlers: spec.handlers,
      pool: spec.pool,
      keys: spec.keys?.mode === undefined || spec.keys.mode === 'none' ? 'none' : `${spec.keys.mode}:${spec.keys.count}`,
      events,
      eventsPerSec: r0(events / seconds),
      deliveriesPerSec: r0((events * spec.handlers) / seconds),
      txPerSec: r0(txs / seconds),
      publishCall: pct(mergeHists(results, 'pubHist')),
      txTotal: pct(mergeHists(results, 'txHist')),
      errors: results.reduce((a, r) => a + r.errors, 0),
      errorSamples: results.flatMap((r) => r.errorSamples).slice(0, 3),
      clientCores: r2(results.reduce((a, r) => a + r.cpuMs, 0) / spec.durationMs),
      clientLoopDelayP99Ms: r1(Math.max(...results.map((r) => r.loopDelayP99))),
      ...derive(be, db, events, events * spec.handlers),
      detail: db,
    };
    ctx.log(
      `[publish] ${be.name} ${spec.label}: ${row.eventsPerSec} ev/s, publish p99 ${row.publishCall.p99} ms, tx p99 ${row.txTotal.p99} ms, db cpu ${(row as any).dbCpuAvgPct}%`,
    );
    ctx.emit(row);
    return row;
  } finally {
    fleet.kill();
    await be.drop(database).catch(() => undefined);
  }
}

// ---------------------------------------------------------------- templates + drain

export interface TemplateSpec {
  events: number;
  handlers: number;
  keys: KeySpec;
  procs: number;
  loops: number;
  batch: number;
  pool: number;
}

export async function buildTemplate(ctx: Ctx, scenario: string, name: string, spec: TemplateSpec): Promise<void> {
  const { be } = ctx;
  await be.createFresh(name);
  const connection = be.connection(name);
  const perProc = Math.floor(spec.events / spec.procs);
  const inits = Array.from({ length: spec.procs }, (_, w) => ({
    role: 'publisher',
    backend: be.name,
    ...connection,
    cfg: {
      w,
      loops: spec.loops,
      batch: spec.batch,
      handlers: spec.handlers,
      keys: spec.keys,
      pool: spec.pool,
      warmupMs: 0,
      durationMs: 3_600_000,
      rate: 0,
      maxInflight: 0,
      trackCommits: false,
      maxEvents: perProc,
    },
  }));
  const fleet = new Fleet(inits);
  try {
    await fleet.ready();
    const startAt = nowMs() + 300;
    fleet.go(startAt);
    const results = await fleet.done();
    const seconds = (nowMs() - startAt) / 1000;
    const loaded = results.reduce((a, r) => a + r.events, 0);
    const errors = results.reduce((a, r) => a + r.errors, 0);
    if (errors > 0) throw new Error(`template load had ${errors} errors: ${results.flatMap((r) => r.errorSamples)}`);
    await be.afterLoad(name);
    const counts = await be.counts(name);
    ctx.log(`[template] ${be.name} ${name}: ${loaded} events, ${counts.pending} pending deliveries in ${r1(seconds)} s (${r0(loaded / seconds)} ev/s)`);
    ctx.emit({
      scenario: `${scenario}-load`,
      store: be.name,
      label: `preload ${name}`,
      events: loaded,
      handlers: spec.handlers,
      keys: spec.keys.mode === 'none' ? 'none' : `${spec.keys.mode}:${spec.keys.count}`,
      seconds: r1(seconds),
      eventsPerSec: r0(loaded / seconds),
      pendingDeliveries: counts.pending,
    });
  } finally {
    fleet.kill();
  }
}

export interface DrainSpec {
  label: string;
  instances: number;
  concurrency: number;
  batchSize: number;
  pool: number;
  pollMs: number;
  handlers: number;
  expected: number;
  maxDrainMs: number;
  keys?: string;
}

export async function drainRun(ctx: Ctx, scenario: string, template: string, spec: DrainSpec): Promise<Record<string, any>> {
  const { be } = ctx;
  const database = nextDb('drain');
  await be.clone(template, database);
  const connection = be.connection(database);
  const inits = Array.from({ length: spec.instances }, () => ({
    role: 'dispatcher',
    backend: be.name,
    ...connection,
    cfg: {
      handlers: spec.handlers,
      concurrency: spec.concurrency,
      batchSize: spec.batchSize,
      pollMs: spec.pollMs,
      pool: spec.pool,
      trackHandled: true,
    },
  }));
  const handledAll: Array<[number, number, number]> = [];
  const fleet = new Fleet(inits, (message: any) => {
    if (message.t === 'handled') for (const item of message.items) handledAll.push(item);
  });
  try {
    await fleet.ready();
    const monitor = be.monitor(database);
    await monitor.begin();
    const startAt = nowMs() + 300;
    fleet.go(startAt);
    while (fleet.handledTotal < spec.expected && nowMs() - startAt < spec.maxDrainMs) await sleep(100);
    const stoppedAt = nowMs();
    fleet.stop();
    const results = await fleet.done();
    const db = await monitor.end(false);
    const handled = results.reduce((a, r) => a + r.handled, 0);
    const finished = handled >= spec.expected;
    const lastAt = Math.max(...results.map((r) => r.lastAt));
    const drainSeconds = ((finished ? lastAt : stoppedAt) - startAt) / 1000;
    const counts = await be.counts(database);
    const validation: Validation = validateRun({
      handlers: spec.handlers,
      handled: handledAll,
      data: await be.validationData(database),
      drained: finished,
      leaseLost: results.reduce((a, r) => a + (r.leaseLost ?? 0), 0),
    });
    const row = {
      validation,
      scenario,
      store: be.name,
      label: spec.label,
      instances: spec.instances,
      concurrency: spec.concurrency,
      batchSize: spec.batchSize,
      pool: spec.pool,
      pollMs: spec.pollMs,
      handlers: spec.handlers,
      keys: spec.keys ?? 'none',
      expected: spec.expected,
      handled,
      finished,
      completedInDb: counts.completed,
      drainSeconds: r1(drainSeconds),
      deliveriesPerSec: r0(handled / drainSeconds),
      eventsPerSec: r0(handled / spec.handlers / drainSeconds),
      clientCores: r2(results.reduce((a, r) => a + r.cpuMs, 0) / 1000 / drainSeconds),
      clientLoopDelayP99Ms: r1(Math.max(...results.map((r) => r.loopDelayP99))),
      failures: results.reduce((a, r) => a + r.failureCount, 0),
      failureSamples: results.flatMap((r) => r.failures).slice(0, 3),
      claimCalls: statement(db, 'claim (fast)').calls ?? null,
      slowClaimCalls: statement(db, 'claim (slow, blocked keys)').calls ?? null,
      claimMeanMs: r2(statement(db, 'claim (fast)').meanMs ?? NaN),
      slowClaimMeanMs: r2(statement(db, 'claim (slow, blocked keys)').meanMs ?? NaN),
      completeMeanMs: r2(statement(db, 'complete').meanMs ?? NaN),
      statementsPerDelivery: db.statements ? r2(db.statements.reduce((a: number, s: any) => a + s.calls, 0) / handled) : null,
      ...derive(be, db, handled / spec.handlers, handled),
      detail: db,
    };
    ctx.log(
      `[drain] ${validation.valid ? 'VALID' : 'INVALID'} ${be.name} ${spec.label}: ${row.deliveriesPerSec} deliveries/s, ${row.drainSeconds} s${finished ? '' : ' (CAPPED)'}, client ${row.clientCores} cores, db cpu ${(row as any).dbCpuAvgPct}% | handled ${validation.handledDistinct}/${validation.expectedPairs} dup ${validation.duplicates} lost ${validation.lost} order ${validation.orderViolations} open ${validation.openAtEnd}${validation.valid ? '' : ` | ${validation.reasons.join('; ')}`}`,
    );
    ctx.emit(row);
    return row;
  } finally {
    fleet.kill();
    await be.drop(database).catch(() => undefined);
  }
}

// ---------------------------------------------------------------- mixed steady state

export interface MixedSpec {
  label: string;
  rate: number;
  pubProcs: number;
  pubPool: number;
  maxInflight: number;
  instances: number;
  concurrency: number;
  batchSize: number;
  pool: number;
  pollMs: number;
  warmupMs: number;
  durationMs: number;
  recoveryMaxMs: number;
}

export async function mixedRun(ctx: Ctx, scenario: string, spec: MixedSpec): Promise<Record<string, any>> {
  const { be } = ctx;
  const database = nextDb('mixed');
  await be.createFresh(database);
  const connection = be.connection(database);
  const commitAt = new Map<number, number>();
  const handledAt = new Map<number, number>();
  const key = (w: number, n: number): number => w * 1e10 + n;
  const startAt = nowMs() + 1500;
  const measureStart = startAt + spec.warmupMs;
  const measureEnd = measureStart + spec.durationMs;
  const e2e = new Hist();
  const thirds = [new Hist(), new Hist(), new Hist()];
  const record = (commitTs: number, handledTs: number): void => {
    if (handledTs < measureStart || handledTs >= measureEnd) return;
    const latency = Math.max(0, handledTs - commitTs);
    e2e.add(latency);
    thirds[Math.min(2, Math.floor(((handledTs - measureStart) / spec.durationMs) * 3))]!.add(latency);
  };
  let committed = 0;
  const onPub = (message: any): void => {
    if (message.t !== 'commits') return;
    for (const [n, ts] of message.items) {
      committed += 1;
      const k = key(message.w, n);
      const handled = handledAt.get(k);
      if (handled === undefined) commitAt.set(k, ts);
      else {
        handledAt.delete(k);
        record(ts, handled);
      }
    }
  };
  const handledAll: Array<[number, number, number]> = [];
  const onDisp = (message: any): void => {
    if (message.t !== 'handled') return;
    for (const item of message.items) handledAll.push(item);
    for (const [w, code, ts] of message.items) {
      const k = key(w, Math.floor(code / 16));
      const commit = commitAt.get(k);
      if (commit === undefined) handledAt.set(k, ts);
      else {
        commitAt.delete(k);
        record(commit, ts);
      }
    }
  };
  const pubFleet = new Fleet(
    Array.from({ length: spec.pubProcs }, (_, w) => ({
      role: 'publisher',
      backend: be.name,
      ...connection,
      cfg: {
        w,
        loops: 0,
        batch: 1,
        handlers: 1,
        keys: NO_KEYS,
        pool: spec.pubPool,
        warmupMs: spec.warmupMs,
        durationMs: spec.durationMs,
        rate: spec.rate / spec.pubProcs,
        maxInflight: spec.maxInflight,
        trackCommits: true,
      },
    })),
    onPub,
  );
  const dispFleet = new Fleet(
    Array.from({ length: spec.instances }, () => ({
      role: 'dispatcher',
      backend: be.name,
      ...connection,
      cfg: { handlers: 1, concurrency: spec.concurrency, batchSize: spec.batchSize, pollMs: spec.pollMs, pool: spec.pool, trackHandled: true },
    })),
    onDisp,
  );
  const series: Array<{ t: number; backlog: number }> = [];
  let sampling = true;
  try {
    await Promise.all([pubFleet.ready(), dispFleet.ready()]);
    const monitor = be.monitor(database);
    pubFleet.go(startAt);
    dispFleet.go(startAt);
    const sampler = (async () => {
      while (sampling) {
        await sleep(1000);
        series.push({ t: r0((nowMs() - startAt) / 1000), backlog: committed - dispFleet.handledTotal });
      }
    })();
    await sleepUntil(measureStart - 200);
    await monitor.begin();
    await sleepUntil(measureEnd);
    const db = await monitor.end(false);
    const pubResults = await pubFleet.done();
    const backlogAtEnd = committed - dispFleet.handledTotal;
    const drainStart = nowMs();
    while (committed > dispFleet.handledTotal && nowMs() - drainStart < spec.recoveryMaxMs) await sleep(100);
    const recoveryMs = committed > dispFleet.handledTotal ? null : nowMs() - drainStart;
    sampling = false;
    await sampler;
    dispFleet.stop();
    const dispResults = await dispFleet.done();
    const validation: Validation = validateRun({
      handlers: 1,
      handled: handledAll,
      data: await be.validationData(database),
      drained: recoveryMs !== null,
      leaseLost: dispResults.reduce((a, r) => a + (r.leaseLost ?? 0), 0),
    });
    const events = pubResults.reduce((a, r) => a + r.events, 0);
    const dropped = pubResults.reduce((a, r) => a + r.dropped, 0);
    const seconds = spec.durationMs / 1000;
    const backlogs = series.filter((s) => s.t * 1000 >= spec.warmupMs && s.t * 1000 <= spec.warmupMs + spec.durationMs).map((s) => s.backlog);
    const e2eSummary = pct(e2e);
    const row = {
      validation,
      scenario,
      store: be.name,
      label: spec.label,
      offeredPerSec: spec.rate,
      achievedPerSec: r0(events / seconds),
      shedPerSec: r0(dropped / seconds),
      pubErrors: pubResults.reduce((a, r) => a + r.errors, 0),
      instances: spec.instances,
      concurrency: spec.concurrency,
      batchSize: spec.batchSize,
      pollMs: spec.pollMs,
      e2e: e2eSummary,
      e2eP99ByThird: thirds.map((h) => r1(h.quantile(0.99))),
      e2eP50ByThird: thirds.map((h) => r1(h.quantile(0.5))),
      e2eSamples: e2e.n,
      txTotal: pct(mergeHists(pubResults, 'txHist')),
      publishCall: pct(mergeHists(pubResults, 'pubHist')),
      backlogMax: Math.max(0, ...backlogs),
      backlogFirstThirdMax: Math.max(0, ...backlogs.slice(0, Math.ceil(backlogs.length / 3))),
      backlogLastThirdMax: Math.max(0, ...backlogs.slice(-Math.ceil(backlogs.length / 3))),
      backlogAtEnd,
      recoverySeconds: recoveryMs === null ? null : r1(recoveryMs / 1000),
      bounded: recoveryMs !== null && Math.max(0, ...backlogs.slice(-Math.ceil(backlogs.length / 3))) <= Math.max(2 * spec.rate * 0.25, 1),
      pubClientCores: r2(pubResults.reduce((a, r) => a + r.cpuMs, 0) / spec.durationMs),
      dispClientCores: r2(dispResults.reduce((a, r) => a + r.cpuMs, 0) / (spec.durationMs + spec.warmupMs)),
      failures: dispResults.reduce((a, r) => a + r.failureCount, 0),
      failureSamples: dispResults.flatMap((r) => r.failures).slice(0, 3),
      ...derive(be, db, events, events),
      series,
      detail: db,
    };
    ctx.log(
      `[mixed] ${validation.valid ? 'VALID' : 'INVALID'} ${be.name} ${spec.label}: offered ${spec.rate}/s achieved ${row.achievedPerSec}/s, e2e p50/p95/p99 ${e2eSummary.p50}/${e2eSummary.p95}/${e2eSummary.p99} ms, backlog max ${row.backlogMax}, recovery ${row.recoverySeconds} s | handled ${validation.handledDistinct}/${validation.expectedPairs} dup ${validation.duplicates} lost ${validation.lost} order ${validation.orderViolations} open ${validation.openAtEnd}${validation.valid ? '' : ` | ${validation.reasons.join('; ')}`}`,
    );
    ctx.emit(row);
    return row;
  } finally {
    sampling = false;
    pubFleet.kill();
    dispFleet.kill();
    await be.drop(database).catch(() => undefined);
  }
}
