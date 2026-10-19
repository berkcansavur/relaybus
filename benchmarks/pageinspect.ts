// Captures what one delivery leaves in the heap and in every index of the deliveries table.
//   node benchmarks/pageinspect.ts --repo <checkout with built dist/> --url <postgres url> [--out file.txt]
// Needs a PostgreSQL with the pageinspect extension (contrib; the official images have it) and a superuser.
import { createRequire } from 'node:module';
import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const args = process.argv.slice(2);
const flag = (name: string): string | undefined => {
  const index = args.indexOf(`--${name}`);
  return index === -1 ? undefined : args[index + 1];
};
const repo = resolve(flag('repo') ?? process.cwd());
const url = flag('url')!;
const out = flag('out');

const require = createRequire(resolve(repo, 'packages/postgres/package.json'));
const pg = require('pg');
const dist = (name: string) => import(pathToFileURL(resolve(repo, 'packages', name, 'dist/index.js')).href);
const core = await dist('core');
const postgres = await dist('postgres');

const lines: string[] = [];
const print = (text = ''): void => void lines.push(text);

const schema = `inspect_${Date.now()}`;
const pool = new pg.Pool({ connectionString: url, max: 2 });
await pool.query('CREATE EXTENSION IF NOT EXISTS pageinspect');
const store = new postgres.PostgresEventStore({ pool, schema });
const owner = 'worker-1';
const base = new Date('2026-01-01T00:00:00.000Z');
const lease = new Date(base.getTime() + 30_000);


await store.setup();
// Autovacuum must not clean the page while we look at it.
await pool.query(`ALTER TABLE "${schema}".relaybus_deliveries SET (autovacuum_enabled = off)`);
try {
  await pool.query(`ALTER TABLE "${schema}".relaybus_lanes SET (autovacuum_enabled = off)`);
} catch {
  // The 0.1.0 schema has no lanes table.
}

const event = {
  id: core.createUuidV7(), type: 'inspect.order.v1', source: 'inspect', key: null,
  payload: { orderId: 'o-1' }, headers: {}, correlationId: null, causationId: null, occurredAt: base,
};
await store.transaction((tx: unknown) =>
  store.append(tx, [{ event, deliveries: [{ id: core.createUuidV7(), handler: 'h', availableAt: base }] }], base),
);
const [claimed] = await store.claim({ handlers: ['h'], limit: 1, leaseOwner: owner, leaseExpiresAt: lease, now: base });
await store.complete(claimed, new Date(base.getTime() + 100));

const flagsOf = (infomask: number, infomask2: number): string => {
  const names: string[] = [];
  if (infomask & 0x0800) names.push('XMAX_INVALID'); // xmax unset or aborted
  if (infomask2 & 0x4000) names.push('HOT_UPDATED');
  if (infomask2 & 0x8000) names.push('HEAP_ONLY');
  if (infomask & 0x0100) names.push('XMIN_COMMITTED');
  if (infomask & 0x0400) names.push('XMAX_COMMITTED');
  return names.join('|') || '-';
};

async function heap(table: string): Promise<void> {
  const rows = (
    await pool.query(
      `SELECT lp, lp_flags, t_xmin::text, t_xmax::text, t_ctid::text, t_infomask, t_infomask2
       FROM heap_page_items(get_raw_page($1, 0))`,
      [`${schema}.${table}`],
    )
  ).rows;
  print(`heap page of ${table} (${rows.length} line pointers)`);
  print('  lp  xmin   xmax   ctid    flags');
  for (const row of rows) {
    print(
      `  ${String(row.lp).padEnd(3)} ${String(row.t_xmin).padEnd(6)} ${String(row.t_xmax).padEnd(6)} ${String(row.t_ctid).padEnd(7)} ${flagsOf(row.t_infomask, row.t_infomask2)}`,
    );
  }
}

async function indexes(table: string): Promise<void> {
  const list = (
    await pool.query(
      `SELECT indexrelid::regclass::text AS name, pg_get_indexdef(indexrelid) AS def
       FROM pg_index WHERE indrelid = $1::regclass ORDER BY 1`,
      [`${schema}.${table}`],
    )
  ).rows;
  print(`indexes of ${table}: ${list.length}`);
  for (const index of list) {
    const items = (
      await pool.query(`SELECT itemoffset, ctid::text FROM bt_page_items($1, 1) ORDER BY itemoffset`, [index.name]).catch(
        () => ({ rows: [] }),
      )
    ).rows;
    const short = String(index.name).replace(`${schema}.`, '').replace(/^"?\w+"?\./, '');
    print(`  ${short.padEnd(46)} entries=${items.length}${items.length > 0 && items.length <= 8 ? `  -> heap tids ${items.map((i: any) => i.ctid).join(' ')}` : ''}`);
  }
}

print(`version: ${(await pool.query('SELECT version()')).rows[0].version}`);
print('scenario: publish one event with one handler, claim it, complete it');
print();
await heap('relaybus_deliveries');
await indexes('relaybus_deliveries');
const lanesExist = (await pool.query(`SELECT to_regclass($1) AS t`, [`${schema}.relaybus_lanes`])).rows[0].t !== null;
if (lanesExist) {
  print();
  const touched = (
    await pool.query(
      `SELECT lane, ctid::text AS tid FROM "${schema}".relaybus_lanes WHERE epoch > 0 OR lease_owner IS NOT NULL OR cardinality(in_flight) > 0`,
    )
  ).rows;
  const [page] = String(touched[0].tid).replace(/[()]/g, '').split(',').map(Number);
  print(`lane ${touched[0].lane} is the only lane row this claim touched. Its page, versions of that row only:`);
  const rows = (
    await pool.query(
      `SELECT lp, t_xmin::text, t_xmax::text, t_ctid::text, t_infomask, t_infomask2
       FROM heap_page_items(get_raw_page($1, $2)) WHERE t_infomask2 & 49152 <> 0`,
      [`${schema}.relaybus_lanes`, page],
    )
  ).rows;
  print('  lp  xmin   xmax   ctid    flags');
  for (const row of rows) {
    print(`  ${String(row.lp).padEnd(3)} ${String(row.t_xmin).padEnd(6)} ${String(row.t_xmax).padEnd(6)} ${String(row.t_ctid).padEnd(7)} ${flagsOf(row.t_infomask, row.t_infomask2)}`);
  }
  print(`indexes of relaybus_lanes: 1 (primary key on the lane number, 256 entries before and after: the updates added none)`);
}
print();
print('tuple counters (may lag by about a second):');
await new Promise((r) => setTimeout(r, 1500));
for (const row of (await pool.query(`SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del FROM pg_stat_user_tables WHERE schemaname = $1 ORDER BY relname`, [schema])).rows) {
  print(`  ${String(row.relname).padEnd(24)} inserted=${row.n_tup_ins} updated=${row.n_tup_upd} hot_updated=${row.n_tup_hot_upd} deleted=${row.n_tup_del}`);
}
await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
await pool.end();

const text = `${lines.join('\n')}\n`;
if (out) writeFileSync(resolve(out), text);
console.log(text);
