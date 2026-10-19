# relaybus benchmarks

Throughput and latency harness for `@relaybus/postgres` and `@relaybus/mongodb` on real
databases started with Testcontainers (PostgreSQL 17, MongoDB 7 as a single-node replica set).
It is not part of the published packages (`benchmarks/` is outside `packages/*` and
`pnpm-workspace.yaml`), it does not modify `packages/*/src`, and it needs no install step: it
imports the built `dist/` of `@relaybus/core|postgres|mongodb` and resolves `pg`, `mongodb` and
`@testcontainers/*` from the packages that already depend on them.

## Run

```sh
pnpm -r run build                       # once: the harness uses packages/*/dist
node benchmarks/run.ts                  # everything, both stores (about 1.5 hours)
node benchmarks/run.ts --quick          # smoke run: 20k events, short windows (about 15 minutes)
node benchmarks/run.ts --store pg --scenarios 1,2   # a subset
node benchmarks/explain.ts              # EXPLAIN (ANALYZE, BUFFERS, WAL) of the hot statements under load
node benchmarks/report.ts benchmarks/results/<run>  # re-render results.md from rows.jsonl
```

Requirements: Node 24 (runs the `.ts` files directly, type stripping), Docker running.
Flags: `--store pg|mongo|all`, `--scenarios 1,2,3,4,5`, `--events N` (preload size, default 200000),
`--out <name>` (directory under `benchmarks/results/`), `--small` (fewer dispatch configurations),
`BENCH_SYNC_COMMIT=off` (PostgreSQL `synchronous_commit` diagnostic), `BENCH_PG_IMAGE`, `BENCH_MONGO_IMAGE`.

Output goes to `benchmarks/results/<run>/`: `env.json` (machine, versions, commit, DB settings),
`rows.jsonl` (one JSON row per measurement, including pg_stat_statements / table / index detail),
`results.md` (tables). `explain.ts` writes `explain-*.txt`.

## How it measures

- Every publisher and every dispatcher instance is a separate Node process (`worker.ts`) with its own
  pool and `RelayBus`, so the client side is not a single event loop. Client CPU (cores used) and
  event-loop delay are reported for each run, to show when the client, not the database, is the limit.
- Warm-up is excluded: publish and mixed runs count only the window after `warmupMs`.
  Drain runs start from an identical cloned database (PostgreSQL `CREATE DATABASE ... TEMPLATE ... STRATEGY FILE_COPY`
  followed by `pg_prewarm`; MongoDB `$out` copy plus index build) and run until every delivery is completed
  or a time cap is hit (`CAPPED` in the tables, the rate is then over the capped window).
- Publish latency = the `bus.publish(All)` call inside the transaction; tx latency = `BEGIN` to `COMMIT` returned.
  End-to-end latency (mixed scenario) = timestamp after the publisher's `COMMIT` returned to the timestamp at which the
  handler is invoked, joined by event id across processes (same host clock).
- Database side (PostgreSQL): `pg_stat_statements` (reset per run; statements grouped by role), `pg_stat_user_tables`
  (inserts/updates/HOT updates/dead tuples), `pg_current_wal_lsn()` and `pg_stat_wal` diffs for WAL volume,
  `pg_stat_activity` wait-event sampling every 100 ms, `docker stats` for container CPU.
  MongoDB: `serverStatus` diffs (opcounters, transactions, write conflicts, WiredTiger journal bytes), oplog size diff, `docker stats`.
- Handlers are no-ops, so the numbers are the cost of the bus and the store, not of your handler.
- Retention is disabled (`retention.completed: false`) so a purge timer cannot interfere.

## Exactness validation (a run that fails it is INVALID)

Throughput is only quoted for runs that processed exactly what was published. Every dispatcher reports each handler
invocation as `(event, handler, timestamp)`, and after the run the harness reads the ground truth from the database
(`lib/validate.ts`, self-test: `node benchmarks/validate.selftest.ts`):

- **lost**: expected (event, handler) pairs that were neither handled nor still open (pending, processing or dead) in the database;
- **unexpected**: handled pairs that were never published;
- **duplicates**: invocations beyond the first per pair. They are allowed only as documented at-least-once redeliveries, so they
  may not exceed the number of lease-lost reports of the workers; they are counted and reported;
- **order**: for keyed events, per (handler, key) the handled sequence must follow publish (commit) order and be a gapless prefix of it;
- **drained**: when a run finished (drain reached the expected count, mixed backlog recovered) nothing may remain open, and no dead
  letters may exist at the end of any run.

Each drain and mixed row carries a `validation` object, the log prints `VALID` / `INVALID` with the counts, and `run.ts` exits with
status 2 when any run was invalid. Before/after comparisons must use the same harness on both sides.

## Scenarios

| id  | what                                                                                                                                              | stores    |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| 1   | publish-only: 1, 8, 32 concurrent publishers, one transaction each; `publish` vs `publishAll(100)`; default pool vs sized pool; 4-process ceiling | pg, mongo |
| 2   | dispatch: 200k preloaded events, 1 handler; 1/4/8 dispatcher processes x concurrency/batchSize/pool configurations                                | pg, mongo |
| 3   | fan-out: 5 handlers per event (publish side and dispatch side)                                                                                    | pg, mongo |
| 4   | keyed ordering: 1000 distinct keys vs 10 hot keys vs 1 key (publish and dispatch)                                                                 | pg, mongo |
| 5   | mixed steady state at 1k / 5k / 10k events/s, dispatchers running, lag and end-to-end latency over time                                           | pg, mongo |

Configuration names: "defaults" are the `RelayBus` defaults (concurrency 10, batchSize = concurrency, pollInterval 1s, `pg` pool max 10,
MongoDB driver pool 100). "tuned" is concurrency 200, batchSize 200, pool 40, pollInterval 50 ms.

## Caveats (read before quoting numbers)

- Docker Desktop on macOS: the databases run in a Linux VM that shares the physical cores with the Node workers on the host.
  Round trips go through the VM network (measured `SELECT 1` p50 is in `env.json`), fsync goes to a virtual disk.
  Treat absolute numbers as a lower bound for a dedicated server and the ratios between configurations as the useful part.
- PostgreSQL runs with `shared_buffers=1GB`, `max_wal_size=4GB` and `pg_stat_statements`/`track_io_timing` on (see `env.json`);
  `synchronous_commit` and `fsync` stay at their safe defaults unless `BENCH_SYNC_COMMIT=off` is set.
- A run is a single measurement of a few tens of seconds, not a statistical sample; neighbouring configurations that differ by
  under about 10 % are noise.
