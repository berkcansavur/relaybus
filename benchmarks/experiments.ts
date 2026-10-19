// What would the proposed optimisations buy? Each variant runs the REAL claim statement of the library against a clone of
// the same 200k-delivery table, and differs only in how a delivery is completed or in the schema.
// relaybus itself is not modified: the alternatives are plain SQL executed by this script.
// Usage: node experiments.ts [--events 200000] [--out experiments]
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createPgBackend } from './lib/backends.ts';
import { captured, captureStatements } from './lib/capture.ts';
import { pg } from './lib/deps.ts';
import { nowMs, sleep } from './lib/fleet.ts';
import { buildTemplate } from './lib/scenarios.ts';

const args = process.argv.slice(2);
const flag = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index === -1 ? fallback : (args[index + 1] ?? fallback);
};
const N = Number(flag('events', '200000'));
const outDir = resolve(fileURLToPath(new URL('.', import.meta.url)), 'results', flag('out', 'experiments'));
mkdirSync(outDir, { recursive: true });
const rowsFile = resolve(outDir, 'rows.jsonl');
if (!args.includes('--append')) writeFileSync(rowsFile, '');
const log = (m: string): void => console.log(`${new Date().toISOString().slice(11, 19)} ${m}`);

type Completion = 'row' | 'batch' | 'delete';
interface Variant {
  name: string;
  completion: Completion;
  dropAdminIndexes?: boolean;
  syncOff?: boolean;
  claimers: number;
  batch?: number;
  /** Library-like pipelining: 200 delivery slots, each completed as soon as it finishes, claim sized to the free slots. */
  pipelined?: boolean;
}

const BATCH_COMPLETE = `WITH changed AS (UPDATE relaybus_deliveries AS d
  SET status = 'completed', completed_at = $3::timestamptz, updated_at = $3::timestamptz,
      lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
  FROM unnest($1::text[], $2::text[]) AS t (id, token)
  WHERE d.id = t.id AND d.status = 'processing' AND d.lease_token = t.token RETURNING 1)
SELECT count(*)::int AS count FROM changed`;
const BATCH_DELETE = `WITH changed AS (DELETE FROM relaybus_deliveries AS d
  USING unnest($1::text[], $2::text[]) AS t (id, token)
  WHERE d.id = t.id AND d.status = 'processing' AND d.lease_token = t.token RETURNING 1)
SELECT count(*)::int AS count FROM changed`;
const ADMIN_INDEXES = [
  'relaybus_deliveries_status_created_idx',
  'relaybus_deliveries_handler_created_idx',
  'relaybus_deliveries_completed_idx',
  'relaybus_deliveries_dead_idx',
];

const VARIANTS: Variant[] = [
  { name: 'V0P library-like pipelined baseline (200 slots, one UPDATE per delivery)', completion: 'row', claimers: 1, pipelined: true },
  { name: 'V0P without the 4 admin/purge indexes', completion: 'row', claimers: 1, pipelined: true, dropAdminIndexes: true },
  { name: 'V0 baseline: claim(200) + one UPDATE per delivery', completion: 'row', claimers: 2 },
  { name: 'V0 + synchronous_commit=off (diagnostic, not durable)', completion: 'row', claimers: 2, syncOff: true },
  { name: 'V1 batched complete: one UPDATE ... FROM unnest per claim', completion: 'batch', claimers: 2 },
  { name: 'V1 + synchronous_commit=off (diagnostic)', completion: 'batch', claimers: 2, syncOff: true },
  { name: 'V1 with 1 claimer', completion: 'batch', claimers: 1 },
  { name: 'V1 with 4 claimers', completion: 'batch', claimers: 4 },
  { name: 'V1 with 8 claimers', completion: 'batch', claimers: 8 },
  { name: 'V2 delete-on-complete (batched DELETE)', completion: 'delete', claimers: 2 },
  { name: 'V0 without the 4 admin/purge indexes', completion: 'row', claimers: 2, dropAdminIndexes: true },
  { name: 'V1 without the 4 admin/purge indexes', completion: 'batch', claimers: 2, dropAdminIndexes: true },
  { name: 'V2 without the 4 admin/purge indexes', completion: 'delete', claimers: 2, dropAdminIndexes: true },
];

const only = flag('only', '');
const be = await createPgBackend();
const ctx = { be, log, emit: (row: Record<string, unknown>) => appendFileSync(rowsFile, `${JSON.stringify(row)}\n`) };
let runNumber = 0;

async function runVariant(variant: Variant): Promise<void> {
  const database = `exp_${++runNumber}`;
  await be.clone('tpl_unkeyed', database);
  const url = be.connection(database).url;
  const admin = new pg.Pool({ connectionString: url, max: 1 });
  admin.on('error', () => undefined);
  if (variant.dropAdminIndexes) for (const index of ADMIN_INDEXES) await admin.query(`DROP INDEX IF EXISTS ${index}`);
  await admin.end();
  const pool = new pg.Pool({ connectionString: url, max: 40, ...(variant.syncOff ? { options: '-c synchronous_commit=off' } : {}) });
  pool.on('error', () => undefined);
  const claim = captured.get('claim')!.sql;
  const completeRow = captured.get('complete')!.sql;
  const batch = variant.batch ?? 200;
  const monitor = be.monitor(database);
  await monitor.begin();
  const cpu0 = process.cpuUsage();
  let completed = 0;
  let claimed = 0;
  let lastCompletion = nowMs();
  const started = nowMs();
  const cap = started + 90_000;
  const claimLatencies: number[] = [];
  async function claimer(): Promise<void> {
    while (completed < N && nowMs() < cap) {
      const now = new Date();
      const t = performance.now();
      const rows = (
        await pool.query(claim, [now.toISOString(), JSON.stringify(['h1']), batch, 'exp', new Date(now.getTime() + 30_000).toISOString()])
      ).rows.filter((r: any) => r.delivery_id !== null);
      claimLatencies.push(performance.now() - t);
      if (rows.length === 0) {
        await sleep(10);
        continue;
      }
      claimed += rows.length;
      const stamp = new Date().toISOString();
      if (variant.completion === 'row') {
        await Promise.all(rows.map((r: any) => pool.query(completeRow, [r.delivery_id, r.lease_token, stamp])));
        completed += rows.length;
      } else {
        const sql = variant.completion === 'batch' ? BATCH_COMPLETE : BATCH_DELETE;
        const params = variant.completion === 'batch'
          ? [rows.map((r: any) => r.delivery_id), rows.map((r: any) => r.lease_token), stamp]
          : [rows.map((r: any) => r.delivery_id), rows.map((r: any) => r.lease_token)];
        const result = await pool.query(sql, params);
        completed += result.rows[0].count;
      }
      lastCompletion = nowMs();
    }
  }
  async function pipelinedClaimer(): Promise<void> {
    const SLOTS = 200;
    let inflight = 0;
    while ((completed < N || inflight > 0) && nowMs() < cap) {
      const free = SLOTS - inflight;
      if (free <= 0 || completed + inflight >= N) {
        await sleep(1);
        continue;
      }
      const now = new Date();
      const t = performance.now();
      const rows = (
        await pool.query(claim, [now.toISOString(), JSON.stringify(['h1']), Math.min(free, batch), 'exp', new Date(now.getTime() + 30_000).toISOString()])
      ).rows.filter((r: any) => r.delivery_id !== null);
      claimLatencies.push(performance.now() - t);
      if (rows.length === 0) {
        await sleep(5);
        continue;
      }
      claimed += rows.length;
      for (const r of rows) {
        inflight += 1;
        void pool.query(completeRow, [r.delivery_id, r.lease_token, new Date().toISOString()]).then(() => {
          inflight -= 1;
          completed += 1;
          lastCompletion = nowMs();
        });
      }
    }
  }
  await Promise.all(Array.from({ length: variant.claimers }, variant.pipelined ? pipelinedClaimer : claimer));
  const seconds = (lastCompletion - started) / 1000;
  const cpu = process.cpuUsage(cpu0);
  const db = await monitor.end(false);
  await pool.end();
  claimLatencies.sort((a, b) => a - b);
  const deliveries = db.tables.find((t: any) => String(t.table).endsWith('deliveries')) ?? {};
  const row = {
    scenario: 'experiment',
    variant: variant.name,
    deliveriesPerSec: Math.round(completed / seconds),
    drainSeconds: Math.round(seconds * 10) / 10,
    finished: completed >= N,
    completed,
    claimers: variant.claimers,
    claimMeanMs: Math.round((claimLatencies.reduce((a, b) => a + b, 0) / claimLatencies.length) * 100) / 100,
    claimP99Ms: Math.round(claimLatencies[Math.floor(claimLatencies.length * 0.99)]! * 100) / 100,
    walBytesPerDelivery: Math.round(db.walBytes / Math.max(1, completed)),
    walRecordsPerDelivery: Math.round((10 * db.walRecords) / Math.max(1, completed)) / 10,
    dbCpuAvgPct: db.docker.cpuAvgPct,
    clientCores: Math.round((100 * (cpu.user + cpu.system)) / 1000 / (seconds * 1000)) / 100,
    activeBackends: db.activeBackendsAvg,
    topWaits: db.waits.slice(0, 3).map((w: any) => `${w.event} ${w.pct}%`).join(', '),
    updates: deliveries.upd,
    hotUpdates: deliveries.hot_upd,
    deletes: deliveries.del,
    deadTuples: deliveries.dead,
    tableMiBAfter: Math.round((deliveries.table_bytes / 1048576) * 10) / 10,
    indexMiBAfter: Math.round((deliveries.index_bytes / 1048576) * 10) / 10,
    statements: db.statements,
  };
  ctx.emit(row);
  log(`${variant.name}: ${row.deliveriesPerSec} deliveries/s (${row.drainSeconds}s${row.finished ? '' : ' CAPPED'}), WAL ${row.walBytesPerDelivery} B/delivery, db cpu ${row.dbCpuAvgPct}%, claim ${row.claimMeanMs} ms, waits: ${row.topWaits}`);
  await be.drop(database);
}

try {
  await captureStatements(be, log);
  await buildTemplate(ctx, 'experiment', 'tpl_unkeyed', { events: N, handlers: 1, keys: { mode: 'none', count: 0 }, procs: 2, loops: 4, batch: 500, pool: 10 });
  for (const variant of VARIANTS) if (only === '' || variant.name.includes(only)) await runVariant(variant);
} finally {
  await be.stop();
}
process.exit(0);
