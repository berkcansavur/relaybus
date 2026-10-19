// Renders results/<run>/rows.jsonl + env.json into results/<run>/results.md
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { resolve } from 'node:path';

const dir = resolve(process.argv[2] ?? '.');
const rows: any[] = readFileSync(resolve(dir, 'rows.jsonl'), 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
const env: any = existsSync(resolve(dir, 'env.json')) ? JSON.parse(readFileSync(resolve(dir, 'env.json'), 'utf8')) : {};

const out: string[] = [];
const table = (headers: string[], body: Array<Array<string | number | null | undefined>>): void => {
  out.push(`| ${headers.join(' | ')} |`, `|${headers.map(() => '---').join('|')}|`);
  for (const row of body) out.push(`| ${row.map((cell) => (cell === null || cell === undefined || Number.isNaN(cell) ? '-' : String(cell))).join(' | ')} |`);
  out.push('');
};
const fmt = (n: number | null | undefined, digits = 0): string => (n === null || n === undefined || Number.isNaN(n) ? '-' : n.toLocaleString('en-US', { maximumFractionDigits: digits }));
const trio = (h: any): string => (h ? `${h.p50} / ${h.p95} / ${h.p99}` : '-');

out.push('# relaybus benchmark results', '');
out.push('## Environment', '');
out.push('```json', JSON.stringify(env, null, 2), '```', '');

for (const store of ['pg', 'mongo']) {
  const mine = rows.filter((r) => r.store === store);
  if (mine.length === 0) continue;
  const title = store === 'pg' ? 'PostgreSQL 17' : 'MongoDB 7 (single-node replica set)';
  out.push(`# ${title}`, '');

  for (const [scenario, heading] of [['s1-publish', 'Scenario 1: publish-only (dispatcher disabled, 1 handler)'], ['s3-fanout-publish', 'Scenario 3a: fan-out on the publish side (5 handlers)'], ['s4-keyed-publish', 'Scenario 4a: keyed publish (8 publishers, single event per transaction)']] as const) {
    const set = mine.filter((r) => r.scenario === scenario);
    if (set.length === 0) continue;
    out.push(`## ${heading}`, '', 'Latencies in ms as p50 / p95 / p99. "publish call" = `bus.publish(All)` inside the transaction; "tx total" = BEGIN to COMMIT returned.', '');
    const walHeader = store === 'pg' ? ['WAL B/event', 'WAL rec/event'] : ['oplog B/event', 'journal B/event'];
    table(
      ['config', 'events/s', 'deliveries/s', 'tx/s', 'publish call', 'tx total', 'client cores', 'DB CPU % (avg/max)', ...walHeader, ...(store === 'pg' ? ['active backends', 'top waits'] : []), 'errors'],
      set.map((r) => [
        r.label, fmt(r.eventsPerSec), fmt(r.deliveriesPerSec), fmt(r.txPerSec), trio(r.publishCall), trio(r.txTotal), r.clientCores,
        `${r.dbCpuAvgPct} / ${r.dbCpuMaxPct}`,
        ...(store === 'pg' ? [fmt(r.walBytesPerEvent), r.walRecordsPerEvent, r.activeBackends, r.topWaits] : [fmt(r.oplogBytesPerEvent), fmt(r.journalBytesPerEvent)]),
        r.errors,
      ]),
    );
  }

  const loads = mine.filter((r) => r.scenario.endsWith('-load'));
  if (loads.length > 0) {
    out.push('## Preload (template databases)', '');
    table(['template', 'events', 'handlers', 'keys', 'seconds', 'events/s'], loads.map((r) => [r.label, fmt(r.events), r.handlers, r.keys, r.seconds, fmt(r.eventsPerSec)]));
  }

  for (const [scenario, heading] of [['s2-dispatch', 'Scenario 2: dispatch throughput (preloaded events, 1 handler, no-op handler)'], ['s3-fanout', 'Scenario 3b: fan-out dispatch (5 handlers per event)'], ['s4-keyed', 'Scenario 4b: keyed dispatch']] as const) {
    const set = mine.filter((r) => r.scenario === scenario);
    if (set.length === 0) continue;
    out.push(`## ${heading}`, '', '`CAPPED` = the run hit the time cap before draining; the rate is over the capped window.', '');
    const extra = store === 'pg' ? ['WAL B/deliv', 'HOT upd %', 'claim ms (fast/slow)', 'complete ms', 'stmts/deliv', 'active backends', 'top waits (% of active samples)'] : ['oplog B/deliv', 'journal B/deliv', 'ops/deliv', 'aborted txn'];
    table(
      ['config', 'deliveries/s', 'drain s', 'client cores', 'loop delay p99 ms', 'DB CPU % (avg/max)', ...extra, 'failures'],
      set.map((r) => [
        r.label, fmt(r.deliveriesPerSec), `${r.drainSeconds}${r.finished ? '' : ' CAPPED'}`, r.clientCores, r.clientLoopDelayP99Ms, `${r.dbCpuAvgPct} / ${r.dbCpuMaxPct}`,
        ...(store === 'pg'
          ? [fmt(r.walBytesPerDelivery), r.hotUpdPct, `${r.claimMeanMs} / ${Number.isNaN(r.slowClaimMeanMs) || r.slowClaimMeanMs === null ? '-' : r.slowClaimMeanMs}`, r.completeMeanMs, r.statementsPerDelivery, r.activeBackends, r.topWaits]
          : [fmt(r.oplogBytesPerDelivery), fmt(r.journalBytesPerDelivery), r.opsPerDelivery, r.txnAborted]),
        r.failures,
      ]),
    );
  }

  const mixed = mine.filter((r) => r.scenario === 's5-mixed');
  if (mixed.length > 0) {
    out.push('## Scenario 5: mixed steady state (publishers at a fixed rate, dispatchers running)', '', 'e2e = commit returned in the publisher -> handler invoked, ms, p50 / p95 / p99 over the measured window. "bounded" = backlog stopped growing and drained after publishers stopped.', '');
    table(
      ['config', 'offered/s', 'achieved/s', 'shed/s', 'e2e p50/p95/p99', 'e2e p99 by third', 'backlog max', 'backlog end', 'recovery s', 'tx p99', 'DB CPU %', 'bounded'],
      mixed.map((r) => [r.label, fmt(r.offeredPerSec), fmt(r.achievedPerSec), fmt(r.shedPerSec), trio(r.e2e), r.e2eP99ByThird.join(' / '), fmt(r.backlogMax), fmt(r.backlogAtEnd), r.recoverySeconds, r.txTotal.p99, `${r.dbCpuAvgPct} / ${r.dbCpuMaxPct}`, r.bounded]),
    );
  }

  const detailed = mine.filter((r) => r.detail?.statements && ['s2-dispatch', 's1-publish', 's4-keyed', 's5-mixed'].includes(r.scenario));
  const best = new Map<string, any>();
  for (const r of detailed) {
    const key = r.scenario;
    const score = r.deliveriesPerSec ?? r.eventsPerSec ?? r.achievedPerSec;
    if (!best.has(key) || score > (best.get(key).deliveriesPerSec ?? best.get(key).eventsPerSec ?? best.get(key).achievedPerSec)) best.set(key, r);
  }
  if (store === 'pg' && best.size > 0) {
    out.push('## Statement profile of the best run per scenario (pg_stat_statements, measured window)', '');
    for (const [scenario, r] of best) {
      out.push(`### ${scenario}: ${r.label}`, '');
      table(
        ['statement', 'calls', 'mean ms', 'total s', 'rows', 'WAL MiB', 'WAL rec', 'buffers hit', 'buffers read', 'buffers dirtied'],
        r.detail.statements.map((s: any) => [s.kind, fmt(s.calls), s.meanMs.toFixed(3), (s.totalMs / 1000).toFixed(1), fmt(s.rows), (s.walBytes / 1048576).toFixed(1), fmt(s.walRecords), fmt(s.blksHit), fmt(s.blksRead), fmt(s.blksDirtied)]),
      );
      table(
        ['table', 'inserts', 'updates', 'HOT updates', 'deletes', 'dead tuples', 'table MiB', 'index MiB'],
        r.detail.tables.map((t: any) => [t.table, fmt(t.ins), fmt(t.upd), fmt(t.hot_upd), fmt(t.del), fmt(t.dead), (t.table_bytes / 1048576).toFixed(1), (t.index_bytes / 1048576).toFixed(1)]),
      );
    }
  }
}
writeFileSync(resolve(dir, 'results.md'), out.join('\n'));
console.log(`wrote ${resolve(dir, 'results.md')}`);
