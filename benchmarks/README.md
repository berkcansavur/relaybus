# relaybus benchmarks

Throughput, latency and write-amplification harness for `@relaybus/postgres` and `@relaybus/mongodb` on real
databases started with Testcontainers (PostgreSQL 17, MongoDB 7 as a single-node replica set). It is not part of the
published packages. It is typed, linted and checked by the repository's `pnpm typecheck` and `pnpm lint`, and the pure
parts (statistics, comparison, baselines, report) are unit tested in `benchmarks/test/`.

## Run

```sh
pnpm bench:quick                         # quick gate: HEAD against main, about 8-10 minutes (about 4-5 with a stored baseline)
pnpm bench:quick --base v0.1.0 --candidate feat/x
pnpm bench:full                          # full matrix against the stored baseline, about 25 minutes against a stored baseline
```

Requirements: Node 24 (runs the `.ts` files directly), Docker running, `pnpm`, `git`. The first run of a ref builds it
(about 1 minute: `git archive`, `pnpm install --frozen-lockfile`, build of core, postgres and mongodb) into
`benchmarks/.cache/sides/<sha>/`; later runs reuse that build.

| option              | meaning                                                                                                                                                        |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--base <ref>`      | reference code (default `main`)                                                                                                                                |
| `--candidate <ref>` | code under test (default `HEAD`; uncommitted changes in `packages/` are not measured)                                                                          |
| `--pairs <n>`       | samples per side for every cell (quick default 3; full default 1, headline cells 3)                                                                            |
| `--only <a,b>`      | only cells whose id contains one of the fragments, for example `--only pg-pub,mongo-disp`                                                                      |
| `--interleave`      | measure the base again, interleaved with the candidate, even when a baseline is stored                                                                         |
| `--no-baseline`     | ignore stored baselines (same as measuring the base again)                                                                                                     |
| `--no-store`        | do not write baselines                                                                                                                                         |
| `--accept <a,b>`    | counters whose regression is expected, by name as printed in the report (for example `deliveries.delPerUnit`); reported as accepted, they do not fail the gate |
| `--strict`          | also fail on throughput and latency regressions (default: only counters and validation fail)                                                                   |
| `--force`           | run although the load average is above 35 % of the cores                                                                                                       |
| `--prepare`         | build the two refs and stop                                                                                                                                    |

Exit code: 0 gate passed, 1 gate failed, 2 harness error. Output goes to `benchmarks/results/runs/<timestamp>/`
(git-ignored): `report.md`, `comparison.json`, `samples.jsonl`, `env.json`.

## What a run does

1. Builds the base and candidate refs into separate directories. Both sides run the same harness code (workers, windows,
   monitors); only the `dist/` they import differs.
2. Looks for a stored baseline of the base commit (below). If there is one that covers every planned cell, only the
   candidate is measured. Otherwise base and candidate are measured **interleaved**: for every cell, pair 0 runs base then
   candidate, pair 1 candidate then base, and so on, so drift of the machine (thermal state, other processes) hits both
   sides alike.
3. Base and candidate see **identical data**. Every publisher worker generates its event ids and ordering keys from a seeded
   generator (`lib/deterministic.ts`, seed `WORKLOAD_SEED`, one stream per worker), so the templates of both sides have the
   same lane layout (a lane is a hash of the key or id). The seed is part of the baseline fingerprint. PostgreSQL runs with
   `full_page_writes=off`: full page images depend on checkpoint timing, not on the code.
4. Every sample uses a **fixed window**: a warm-up, then a steady-state window (quick: 1.5 s + 5 s for publish and 2 s + 6 s for
   dispatch, full: 3 s + 15 s, mixed: 4 s + 20 s). Nothing waits for a drain. Dispatch cells clone a preloaded template database (built once per
   side, from that side's own code) and count the deliveries handled inside the window; a template that drains before the
   window ends is reported and the rate is taken over the time it was busy. The quick gate's dispatch cells measure a fixed
   amount of **work** instead (the first 60k / 240k / 15k / 30k deliveries; the window length is
   only a 60 s cap, and a sample that misses it is invalid): a counter per delivery depends on how much of the data was touched, so a window of fixed length
   measures something different on a fast and on a loaded machine. Templates hold twice the work of their cell.
5. Compares and writes the report.

Each publisher and each dispatcher instance is a separate Node process with its own pool and `RelayBus`; handlers are
no-ops, so the numbers are the cost of the bus and the store.

## Cells

Quick gate (8 cells): PostgreSQL publish P=8 with batch 1, batch 100 and batch 100 with 5 handlers; MongoDB publish P=8
batch 100; PostgreSQL dispatch with 1 and 4 instances (tuned) from templates of 120k (one instance) and 480k events (four instances); PostgreSQL keyed dispatch
`hot:10`; MongoDB dispatch with 1 instance (tuned) from a 60k-event template.

Full run (about 40 cells): publish P=1/8/32 x batch 1/100, 5-handler fan-out, keyed publish (`distinct:1000`, `hot:10`,
`hot:1`), 4-process ceiling; dispatch 1/4/8 instances x `defaults` / `mid` / `tuned`, fan-out, keyed (`distinct:1000`,
`hot:10`); MongoDB reduced to `defaults` and `tuned` (the other configurations were indistinguishable); mixed steady state at
1k and 5k events/s. The list lives in `lib/cells.ts`. Configuration names: `defaults` are the `RelayBus` defaults
(concurrency 10, batch size 10, poll 1 s, `pg` pool 10, MongoDB pool 100); `mid` is concurrency 50, batch 200, pool 20;
`tuned` is concurrency 200, batch 200, pool 40, poll 50 ms.

Routed cells (`pg-disp-routed-k1000-1i-tuned`, `pg-disp-routed-hot10-1i-tuned`, full run only): the same keyed templates, but the
only handler is a route to an in-process fake transport that acks every send after 5 ms, so no broker is needed and 0.2.0 and
later run it unchanged. The consumer side of the fake transport records each send; the exactness and per-key order checks read
that record. A hot key is sent one event per round trip, so `hot:10` is bounded by the ack delay.

Keyed templates are loaded with one key per transaction (`publishAll` with `key`), which is why loading them takes
seconds instead of a minute.

## How results are judged

**Primary gate: database-side counters.** For every sample the harness resets the database's statistics before the
workers start, reads them after the last worker exited (so lazily flushed statistics are complete), and divides them by
the number of events added (publish) or deliveries completed (dispatch), counted exactly in the database. The counters
therefore cover the whole sample, warm-up included (fixed-work dispatch cells: up to the target); per
event or delivery that is the same work on both sides. Counters of interleaved runs are judged by the median of the
per-pair ratios, so drift over the run cancels. `lanes.updPerUnit` follows how concurrent claims batch up and moves by
10-20 % between identical samples, so its tolerance is 15 % (extra lane work shows as whole updates per delivery); every other
counter has 3 %:

- PostgreSQL: statements (`pg_stat_statements`), WAL bytes and records (`pg_current_wal_lsn`, `pg_stat_wal`), tuples
  inserted / updated / deleted per relaybus table and the HOT update share (`pg_stat_user_tables`).
- MongoDB: opcounters (insert / update / delete / query / getmore), oplog entries and bytes. Command count, WiredTiger
  journal bytes and full page images are reported but never gate, because background flushing moves them.

These move only when the code changes, not when the laptop is busy. A counter that moves by more than **3 %** in the
wrong direction is a regression and fails the gate. A table that does not exist on one side (for example a new table)
shows as `n/a`.

**Throughput and latency** are reported as the ratio candidate / base with a 95 % bootstrap interval of the median
(paired ratios when interleaved, ratio of medians against a stored baseline). A difference counts only when the whole
interval is beyond the tolerance (7 % throughput, 15 % p99 latency); an interval that straddles the tolerance is
`inconclusive`, and a cell with fewer than 3 samples never gets a verdict. With 3 to 6 samples and typical laptop noise
(5 to 10 %), only changes of roughly 10 % or more are resolvable; smaller effects must show in the counters. These
verdicts fail the gate only with `--strict`.

**Exactness is validated on every run.** Dispatch and mixed samples check against the database that no delivery was lost,
none was handled twice without a lease loss, per-key order held, nothing is dead, and (when the template drained) nothing
is left open. Publish samples check that the database holds exactly `events x handlers` deliveries and no publish
failed. An invalid sample fails the gate and its numbers must not be quoted.

**Noise control.** Right before each sample the harness times a fixed CPU spin (`calibrationMs`) and records the load
average, host CPU utilisation and the database container's CPU. A sample whose spin was more than 1.25x slower than the
quietest sample of the same data set is `busy`; its whole pair is discarded when at least 2 clean pairs remain (otherwise
all samples are kept and the cell is marked `noisy`). The harness refuses to start when the load average is above 35 % of
the cores unless `--force` is given.

## Baselines

After a run, the samples are stored under

```
benchmarks/baselines/<fingerprint>/<commit sha>/{env.json,samples.json}
```

The fingerprint hashes everything that makes measurements comparable: CPU, cores, memory, OS, Docker VM size, Node major
version, the local PostgreSQL and MongoDB image ids, `PG_SETTINGS`, the harness version and the exact cell and template
definitions. A different machine, a changed window or a new image therefore never reuses an old baseline. When the base
commit has a baseline that covers every planned cell, `pnpm bench:quick` and `pnpm bench:full` measure only the
candidate (about half the time) and compare it with the stored samples. The report then notes the ratio of the current
CPU calibration to the stored one; if it differs by more than 15 %, re-run with `--interleave`. The samples of the
candidate are stored as well, so it can serve as the base of the next change. Baselines are small JSON files; commit them
only if you want to share them between machines with the same fingerprint.

## Caveats

- Docker Desktop on macOS runs the databases in a Linux VM that shares the physical cores with the Node workers; round
  trips cross the VM network and fsync goes to a virtual disk. Absolute numbers are a lower bound for a dedicated
  server; the ratios between base and candidate are the useful part.
- PostgreSQL runs with `shared_buffers=1GB`, `max_wal_size=4GB`, `pg_stat_statements` and `track_io_timing` on (see
  `env.json`); `synchronous_commit` and `fsync` stay at their safe defaults unless `BENCH_SYNC_COMMIT=off` is set.
- Retention is disabled so a purge timer cannot interfere.
- Environment overrides: `BENCH_PG_IMAGE`, `BENCH_MONGO_IMAGE`.
- Earlier runs of the previous harness (drain to completion, single sample per cell) are kept in `results/base`,
  `results/after`, `results/main` and `results/mongo-rest`; their numbers are not comparable with harness version 2. The
  one-off investigation scripts of that harness (`explain.ts`, `experiments.ts`, `pageinspect.ts`) were removed with the
  rework; they remain in the git history before the rework commit.
