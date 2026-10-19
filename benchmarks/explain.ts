// EXPLAIN (ANALYZE, BUFFERS, WAL) of the hot statements while dispatchers and a publisher keep the database busy.
// Usage: node explain.ts [--events 200000] [--out <dir name>]
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createPgBackend } from './lib/backends.ts';
import { captured, captureStatements } from './lib/capture.ts';
import { pg } from './lib/deps.ts';
import { Fleet, nowMs, sleep, sleepUntil } from './lib/fleet.ts';
import { buildTemplate } from './lib/scenarios.ts';

const args = process.argv.slice(2);
const flag = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index === -1 ? fallback : (args[index + 1] ?? fallback);
};
const N = Number(flag('events', '200000'));
const outDir = resolve(fileURLToPath(new URL('.', import.meta.url)), 'results', flag('out', 'explain'));
mkdirSync(outDir, { recursive: true });
const log = (m: string): void => console.log(`${new Date().toISOString().slice(11, 19)} ${m}`);

const section = (title: string, plan: string[]): string => `\n===== ${title} =====\n${plan.join('\n')}\n`;

async function explainSuite(url: string, label: string, handlers: string[]): Promise<string> {
  const client = new pg.Client({ connectionString: url });
  client.on('error', () => undefined);
  await client.connect();
  const out: string[] = [`##### EXPLAIN suite: ${label} (${new Date().toISOString()})`];
  const explain = async (title: string, kind: string, params: any[]): Promise<void> => {
    const statement = captured.get(kind);
    if (statement === undefined) {
      out.push(`\n===== ${title} =====\n(statement not captured)`);
      return;
    }
    const rows = (await client.query(`EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS) ${statement.sql}`, params)).rows;
    out.push(section(title, rows.map((r: any) => r['QUERY PLAN'])));
  };
  try {
    // --- claim, fast and slow path, then use the real claim to obtain valid lease tokens for complete / extendLease
    await client.query('BEGIN');
    const now = new Date();
    const claimParams = (limit: number): any[] => [now.toISOString(), JSON.stringify(handlers), limit, 'explain', new Date(now.getTime() + 30_000).toISOString()];
    await explain('claim (fast path), limit 200', 'claim', claimParams(200));
    await client.query('ROLLBACK');

    await client.query('BEGIN');
    await explain('claimBehindBlockedKeys (slow path), limit 200', 'claimSlow', claimParams(200));
    await client.query('ROLLBACK');

    await client.query('BEGIN');
    // Concurrent dispatchers keep the head of the queue locked, so the library claim often returns nothing here;
    // lease one delivery from deeper in the queue with a plain UPDATE to get a valid lease token.
    const leased = (
      await client.query(
        `UPDATE relaybus_deliveries SET status = 'processing', attempts = attempts + 1, lease_owner = 'explain',
                lease_token = 'explain-' || id, lease_expires_at = $1::timestamptz
         WHERE id = (SELECT id FROM relaybus_deliveries WHERE status = 'pending' ORDER BY id OFFSET 20000 LIMIT 1 FOR UPDATE SKIP LOCKED)
         RETURNING id AS delivery_id, lease_token`,
        [new Date(now.getTime() + 30_000).toISOString()],
      )
    ).rows;
    if (leased.length > 0) {
      const target = leased[0];
      await explain('complete (one delivery)', 'complete', [target.delivery_id, target.lease_token, now.toISOString()]);
      await explain('extendLease (one delivery)', 'extendLease', [target.delivery_id, target.lease_token, new Date(now.getTime() + 60_000).toISOString(), now.toISOString()]);
    } else {
      out.push('\n(no deliveries could be leased for the complete / extendLease plans)');
    }
    await client.query('ROLLBACK');

    // --- append: reserve sequence, insert events, insert deliveries (new ids, so rows are really inserted, then rolled back)
    await client.query('BEGIN');
    const eventsCaptured = captured.get('insertEvents');
    const deliveriesCaptured = captured.get('insertDeliveries');
    if (eventsCaptured && deliveriesCaptured) {
      const stamp = Date.now();
      const eventRows = Array.from({ length: 100 }, (_, i) => ({
        id: `explain-${stamp}-${i}`, type: 'bench.order.placed.v1', source: 'explain', key: `ek${i % 50}`, sequence: String(1_000_000 + i),
        payload: { id: i, note: 'x'.repeat(160) }, headers: {}, correlation_id: `c${i}`, causation_id: null, occurred_at: now.toISOString(),
      }));
      const reservation = JSON.stringify([...new Set(eventRows.map((r) => r.key))].map((key) => ({ key, count: 2 })));
      await explain('append: reserveSequences (50 keys)', 'reserveSequences', [reservation]);
      await explain('append: insertEvents (100 events)', 'insertEvents', [JSON.stringify(eventRows), now.toISOString()]);
      const deliveryRows = eventRows.map((r, i) => ({ id: `explain-d-${stamp}-${i}`, event_id: r.id, handler: handlers[0], available_at: now.toISOString() }));
      await explain('append: insertDeliveries (100 deliveries)', 'insertDeliveries', [JSON.stringify(deliveryRows), now.toISOString()]);
    }
    await client.query('ROLLBACK');

    // --- table / index health at this moment
    const health = await client.query(
      `SELECT relname, n_live_tup, n_dead_tup, n_tup_upd, n_tup_hot_upd, autovacuum_count, last_autovacuum,
              pg_size_pretty(pg_table_size(relid)) AS table_size, pg_size_pretty(pg_indexes_size(relid)) AS index_size
       FROM pg_stat_user_tables ORDER BY relname`,
    );
    out.push(section('pg_stat_user_tables at EXPLAIN time', health.rows.map((r: any) => JSON.stringify(r))));
    const indexes = await client.query(
      `SELECT s.indexrelname, pg_relation_size(s.indexrelid) AS bytes, s.idx_scan, s.idx_tup_read FROM pg_stat_user_indexes s ORDER BY 2 DESC`,
    );
    const rows: string[] = [];
    for (const index of indexes.rows) {
      let density = '';
      try {
        const d = (await client.query('SELECT avg_leaf_density, leaf_fragmentation FROM pgstatindex($1)', [index.indexrelname])).rows[0];
        density = ` leaf_density=${Number(d.avg_leaf_density).toFixed(1)}% fragmentation=${Number(d.leaf_fragmentation).toFixed(1)}%`;
      } catch {
        density = '';
      }
      rows.push(`${index.indexrelname}: ${(index.bytes / 1048576).toFixed(1)} MiB, scans=${index.idx_scan}, tuples_read=${index.idx_tup_read}${density}`);
    }
    out.push(section('index sizes, scans and leaf density at EXPLAIN time', rows));
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    await client.end().catch(() => undefined);
  }
  return out.join('\n');
}

async function underLoad(be: Awaited<ReturnType<typeof createPgBackend>>, template: string, label: string, opts: { instances: number; publisherRate: number; explainAfterMs: number }): Promise<string> {
  const database = `explain_${label.replace(/\W/g, '_')}`;
  await be.clone(template, database);
  const connection = be.connection(database);
  const dispatchers = new Fleet(
    Array.from({ length: opts.instances }, () => ({
      role: 'dispatcher', backend: 'pg', ...connection,
      cfg: { handlers: 1, concurrency: 200, batchSize: 200, pollMs: 50, pool: 40, trackHandled: false },
    })),
  );
  const publishers = opts.publisherRate > 0
    ? new Fleet([{
        role: 'publisher', backend: 'pg', ...connection,
        cfg: { w: 0, loops: 0, batch: 1, handlers: 1, keys: { mode: 'none', count: 0 }, pool: 16, warmupMs: 0, durationMs: opts.explainAfterMs + 6000, rate: opts.publisherRate, maxInflight: 128, trackCommits: false },
      }])
    : undefined;
  try {
    await Promise.all([dispatchers.ready(), publishers?.ready()]);
    const startAt = nowMs() + 300;
    dispatchers.go(startAt);
    publishers?.go(startAt);
    await sleepUntil(startAt + opts.explainAfterMs);
    const monitor = be.monitor(database);
    await monitor.begin();
    const text = await explainSuite(connection.url, label, ['h1']);
    const metrics = await monitor.end(true);
    const waits = metrics.waits.map((w: any) => `${w.event}: ${w.pct}%`).join('\n');
    dispatchers.stop();
    publishers?.stop();
    await Promise.all([dispatchers.done(), publishers?.done()]);
    return `${text}\n${section('wait events while the EXPLAINs ran (share of active-backend samples)', [waits])}`;
  } finally {
    dispatchers.kill();
    publishers?.kill();
    await be.drop(database).catch(() => undefined);
  }
}

const be = await createPgBackend();
const ctx = { be, log, emit: () => undefined };
try {
  await captureStatements(be, log);
  await buildTemplate(ctx, 'explain', 'tpl_unkeyed', { events: N, handlers: 1, keys: { mode: 'none', count: 0 }, procs: 2, loops: 4, batch: 500, pool: 10 });
  const unkeyed = await underLoad(be, 'tpl_unkeyed', 'unkeyed_drain_plus_publish', { instances: 4, publisherRate: 2000, explainAfterMs: 8000 });
  writeFileSync(resolve(outDir, 'explain-unkeyed.txt'), unkeyed);
  log('wrote explain-unkeyed.txt');
  await buildTemplate(ctx, 'explain', 'tpl_hot', { events: Math.floor(N / 10), handlers: 1, keys: { mode: 'hot', count: 10 }, procs: 2, loops: 8, batch: 1, pool: 12 });
  const hot = await underLoad(be, 'tpl_hot', 'hot_keys_drain', { instances: 1, publisherRate: 0, explainAfterMs: 5000 });
  writeFileSync(resolve(outDir, 'explain-hot-keys.txt'), hot);
  log('wrote explain-hot-keys.txt');
  await buildTemplate(ctx, 'explain', 'tpl_k1000', { events: Math.floor(N / 2), handlers: 1, keys: { mode: 'distinct', count: 1000 }, procs: 2, loops: 8, batch: 1, pool: 12 });
  const distinct = await underLoad(be, 'tpl_k1000', 'distinct_keys_drain', { instances: 1, publisherRate: 0, explainAfterMs: 5000 });
  writeFileSync(resolve(outDir, 'explain-1000-keys.txt'), distinct);
  log('wrote explain-1000-keys.txt');
} finally {
  await be.stop();
}
void sleep;
process.exit(0);
