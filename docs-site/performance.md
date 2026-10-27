---
title: 'Performance: relaybus 0.2.0 vs 0.1.0'
description: Measured throughput, latency and database write-amplification of relaybus 0.2.0 against 0.1.0 on PostgreSQL 17 and MongoDB 7, with the methodology, the exactness validation, the machine, honest sample counts and the known limits.
---

# Performance: 0.2.0 vs 0.1.0

On PostgreSQL, 0.2.0 publishes 1.1 to 1.9 times faster than 0.1.0 and dispatches 1.3 to 18 times faster (one cell is slower, see below). On MongoDB it dispatches 2 to 26 times faster and publishes at the same speed. It writes about half the WAL per delivery on PostgreSQL. Every sample was checked for lost, duplicate and out-of-order deliveries: there were none.

Read the numbers as ratios on one laptop, not as capacity planning. Where a cell has a single sample the table says so.

- [How it was measured](#how-it-was-measured)
- [Publish](#publish)
- [Dispatch on PostgreSQL](#dispatch-on-postgresql)
- [Dispatch on MongoDB](#dispatch-on-mongodb)
- [Mixed load and latency](#mixed-load-and-latency)
- [What the database does per delivery](#what-the-database-does-per-delivery)
- [Where it is not better](#where-it-is-not-better)
- [0.5.0 vs 0.4.1](#_0-5-0-vs-0-4-1)
- [0.4.0 vs 0.3.1](#_0-4-0-vs-0-3-1)
- [0.3.0 vs 0.2.0](#_0-3-0-vs-0-2-0)
- [Deep dives](#deep-dives)
- [Reproduce it](#reproduce-it)

## How it was measured

| Aspect            | What was done                                                                                                                                                                                                                                                                                                                        |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Same machine      | Both versions ran on one machine in one run (fingerprint `6ba3daf9b9a1`), against the same database images.                                                                                                                                                                                                                          |
| Interleaved       | For every scenario the base (0.1.0) and the candidate (0.2.0) alternate: base first, then candidate, then candidate first, so drift of the machine (heat, other processes) hits both alike.                                                                                                                                          |
| Fixed windows     | Each sample is a warm-up plus a fixed steady-state window (publish 3 s + 15 s, dispatch from a preloaded database, mixed 4 s + 20 s). Nothing waits for a drain, so a slow side cannot hide behind a long run.                                                                                                                       |
| Real processes    | Every publisher and every dispatcher instance is its own Node process with its own pool and bus. Handlers are no-ops, so the numbers are the cost of the bus and the store, not of your handlers.                                                                                                                                    |
| Database counters | Before each sample the database statistics are reset; afterwards statements, WAL bytes and records, rows inserted, updated and deleted (PostgreSQL) or operations and oplog bytes (MongoDB) are divided by the events or deliveries actually counted in the database. These move when the code changes, not when the laptop is busy. |
| Exactness         | After every dispatch and mixed sample the database is checked: no delivery lost, none handled twice without a lease loss, per-key order held, nothing dead. Publish samples must hold exactly `events x handlers` deliveries. **All samples in this run were valid: 0 lost, 0 duplicate, 0 out of order.**                           |
| Noise control     | A fixed CPU spin is timed before each sample; a sample that ran more than 1.25 times slower than the quietest one is discarded together with its pair when two clean pairs remain.                                                                                                                                                   |

Dispatcher configurations: `defaults` are the `RelayBus` defaults (concurrency 10, batch size 10, poll 1 s, `pg` pool 10, MongoDB pool 100). `mid` is concurrency 50, batch 200, pool 20. `tuned` is concurrency 200, batch 200, pool 40, poll 50 ms. `Ni` means N dispatcher processes.

### The machine

| Part       | Value                                                                                                                                                   |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Host       | Apple M4, 10 cores (4 performance), 16 GiB, macOS (Darwin 24.6.0)                                                                                       |
| Node.js    | v24.3.0                                                                                                                                                 |
| Docker     | Docker Desktop, 10 CPUs and 8 GiB for the VM, server 28.5.1                                                                                             |
| PostgreSQL | 17.11 (`postgres:17-alpine`): `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `shared_buffers=1GB`, `max_wal_size=4GB`, `wal_level=replica` |
| MongoDB    | 7.0.45 (`mongo:7`), single-node replica set                                                                                                             |
| Round trip | PostgreSQL p50 0.20 ms, p99 0.35 ms; MongoDB p50 0.25 ms, p99 0.66 ms                                                                                   |

Docker Desktop runs the databases in a Linux VM that shares the physical cores with the Node workers, and fsync goes to a virtual disk. Absolute numbers are a lower bound for a dedicated database server; the ratios are the useful part. The base is `main` at `d9ca3be` (0.1.0), the candidate is `eb3fbd7` (the 0.2.0 branch); the run took 38.9 minutes.

### How much to trust a number

A cell with three samples per side has a 95 % confidence interval and a verdict. A cell with **one sample per side** has neither: its ratio is **indicative**, and a difference below roughly 10 % cannot be told from noise. Database counters (WAL, rows written) are exact either way, which is why they, not throughput, gate a change. In the tables, a **3** in the _samples_ column means a confirmed result and a **1** means indicative.

## Publish

Events per second written by publishing processes, one handler unless noted. `P` is publishers, `batch` is events per `publishAll` call.

| Scenario                                      | 0.1.0 events/s | 0.2.0 events/s | Ratio | Samples |
| --------------------------------------------- | -------------: | -------------: | ----: | :-----: |
| PostgreSQL, P=1, batch 1 (*)                  |         ~1,310 |         ~1,650 | 1.26x |    1    |
| PostgreSQL, P=1, batch 100                    |         16,740 |         20,153 | 1.20x |    1    |
| PostgreSQL, P=8, batch 1                      |          3,703 |          4,603 | 1.24x |    1    |
| PostgreSQL, P=8, batch 100                    |         63,853 |         73,533 | 1.15x |    3    |
| PostgreSQL, P=32, batch 1 (*)                 |         ~5,150 |         ~6,200 | 1.20x |    1    |
| PostgreSQL, P=32, batch 100                   |         65,520 |         76,160 | 1.16x |    1    |
| PostgreSQL, 4 processes x 8, batch 100        |         59,213 |         66,967 | 1.13x |    1    |
| PostgreSQL, P=8, batch 1, 5 handlers          |          2,408 |          2,893 | 1.20x |    1    |
| PostgreSQL, P=8, batch 100, 5 handlers        |         20,987 |         29,060 | 1.39x |    3    |
| PostgreSQL, P=8, batch 1, 1,000 distinct keys |          2,482 |          3,982 | 1.60x |    1    |
| PostgreSQL, P=8, batch 1, 10 hot keys         |          2,615 |          3,975 | 1.52x |    1    |
| PostgreSQL, P=8, batch 1, 1 hot key           |            927 |          1,787 | 1.93x |    1    |
| MongoDB, P=8, batch 1                         |          1,553 |          1,599 | 1.03x |    1    |
| MongoDB, P=8, batch 100                       |         31,600 |         31,167 | 0.98x |    3    |
| MongoDB, P=32, batch 100                      |         24,460 |         31,387 | 1.28x |    1    |
| MongoDB, P=8, batch 100, 5 handlers           |         10,260 |         13,633 | 1.33x |    1    |
| MongoDB, P=8, batch 1, 10 hot keys            |            668 |            912 | 1.37x |    1    |

(*) These three cells were slower than 0.1.0 on the first run of the 0.2.0 branch. The values shown were re-measured after the prepared-statement fix described in [the publish path](/performance/publish-path), with fewer samples than the other cells. The main 38.9-minute run, which already contained the fix, gave 1,281 to 1,696, 5,052 to 6,440 and 2,056 to 3,716 for the three cells (P=1, P=32 and the dispatch cell below).

In words: single-event publishes on PostgreSQL are about a quarter faster, large batches 15 to 20 % faster, and fan-out to five handlers up to 39 % faster, because 0.2.0 writes fewer indexes and one statement per publish. Publishing to a hot ordering key is nearly twice as fast, since the key's sequence counter is now updated in one statement. MongoDB publish is unchanged within noise.

Publish latency (p99 of one publishing transaction, PostgreSQL, P=8, batch 100): 27.3 ms to 19.8 ms (confirmed, three samples).

## Dispatch on PostgreSQL

Deliveries completed per second, from a preloaded backlog of unkeyed deliveries unless noted. Handlers do nothing.

| Scenario                                    | 0.1.0 deliveries/s | 0.2.0 deliveries/s |      Ratio | Samples |
| ------------------------------------------- | -----------------: | -----------------: | ---------: | :-----: |
| 1 process, `defaults` (*)                   |             ~3,100 |             ~4,000 |      1.29x |    1    |
| 1 process, `mid`                            |              3,136 |             10,273 |      3.28x |    1    |
| 1 process, `tuned`                          |              9,329 |             14,348 |      1.82x |    3    |
| 4 processes, `defaults`                     |              2,334 |             10,043 |      4.30x |    1    |
| 4 processes, `mid`                          |              2,805 |             32,306 |     11.52x |    1    |
| 4 processes, `tuned`                        |              3,242 |             55,629 |     17.16x |    1    |
| 8 processes, `defaults`                     |              5,478 |             17,371 |      3.17x |    1    |
| 8 processes, `mid`                          |              8,750 |             45,632 |      5.22x |    1    |
| 8 processes, `tuned`                        |              9,736 |             62,937 |      6.46x |    1    |
| Fan-out to 5 handlers, 1 process, `tuned`   |              8,220 |             17,838 |      2.17x |    1    |
| Fan-out to 5 handlers, 4 processes, `tuned` |              2,964 |             54,474 |     18.38x |    1    |
| 1,000 distinct keys, 1 process, `tuned`     |              2,307 |              7,332 |      3.18x |    1    |
| 1,000 distinct keys, 4 processes, `tuned`   |       ~2,690–3,300 |       ~5,200–5,300 | 1.61–1.93x |    2    |
| 10 hot keys, 1 process, `tuned`             |              1,110 |              1,565 |      1.41x |    1    |

(*) Re-measured after the prepared-statement fix, with fewer samples than the other cells; the main run gave 2,056 to 3,716.

Why 0.1.0 does not scale with processes (2,334 at four, 3,242 with 200 handlers): every delivery was updated three times, so processes spent their time rewriting the same rows and waiting for each other. 0.2.0 workers lease lanes instead of rows, so adding a process adds throughput until the database is the limit. The single-sample cells above 10x are real in direction, but their exact multiples are noisy; the three-sample cell (1 process, `tuned`, 1.82x, interval 1.38 to 4.29) is the confirmed one.

## Dispatch on MongoDB

| Scenario                                  | 0.1.0 deliveries/s | 0.2.0 deliveries/s |  Ratio | Samples |
| ----------------------------------------- | -----------------: | -----------------: | -----: | :-----: |
| 1 process, `defaults`                     |                485 |                997 |  2.06x |    1    |
| 1 process, `tuned`                        |                560 |              6,281 | 11.12x |    3    |
| 4 processes, `defaults`                   |                505 |              3,451 |  6.84x |    1    |
| 4 processes, `tuned`                      |                587 |             15,437 | 26.31x |    1    |
| Fan-out to 5 handlers, 1 process, `tuned` |                587 |              8,599 | 14.66x |    1    |
| 1,000 distinct keys, 1 process, `tuned`   |                173 |              1,955 | 11.28x |    1    |
| 10 hot keys, 1 process, `tuned`           |                177 |                347 |  1.96x |    1    |

0.1.0 issued one conditional update per claim and one per completion, about three oplog entries per delivery. 0.2.0 claims a whole batch with one write to the lane documents and completes it with one bulk delete.

## Mixed load and latency

Publishing at a fixed rate while dispatching, measuring commit-to-handler latency (end to end). One sample per side, so indicative.

| Scenario                        | Accepted events/s 0.1.0 to 0.2.0 | p99 latency 0.1.0 | p99 latency 0.2.0 |
| ------------------------------- | -------------------------------: | ----------------: | ----------------: |
| PostgreSQL, 1,000/s, `defaults` |                    1,000 / 1,000 |            991 ms |           10.5 ms |
| PostgreSQL, 1,000/s, `tuned`    |                    1,001 / 1,000 |           59.9 ms |            9.8 ms |
| PostgreSQL, 5,000/s, `tuned`    |                    2,522 / 4,099 |            114 ms |            524 ms |
| MongoDB, 1,000/s, `tuned`       |                    1,000 / 1,000 |            18.1 s |            389 ms |
| MongoDB, 5,000/s, `tuned`       |                    1,756 / 1,741 |            21.0 s |             8.0 s |

At 1,000 events per second both versions keep up and 0.2.0 delivers in about a hundredth of the time on `defaults`. At 5,000 per second on a laptop both versions are past saturation and shed load; on PostgreSQL 0.2.0 accepts 1.6 times more events but its p99 is higher than 0.1.0's at that higher load. See [known issues](/releases#known-issues).

## What the database does per delivery

These counters are exact, not statistical. PostgreSQL dispatch, per delivery:

| Counter                           | 0.1.0          | 0.2.0                               |
| --------------------------------- | -------------- | ----------------------------------- |
| `deliveries` rows updated         | 2              | 0                                   |
| `deliveries` rows deleted         | 0              | 1                                   |
| `lanes` rows updated              | n/a (no lanes) | about 0.05 (one lane row per batch) |
| `event_completions` rows inserted | n/a            | 1                                   |
| WAL bytes (1 process, `tuned`)    | 2,336          | 1,325 (0.57x)                       |
| WAL bytes (4 processes, `tuned`)  | 2,501          | 1,316 (0.53x)                       |
| WAL bytes (fan-out 5, 1 process)  | 2,300          | 576 (0.25x)                         |
| WAL records (1 process, `tuned`)  | -              | 0.38x of 0.1.0                      |
| WAL bytes (1 process, `defaults`) | 2,240          | 3,129 (1.40x, see below)            |

Publish, per event: WAL 1,758 to 1,490 bytes (0.85x), 14.2 to 11.2 WAL records, statements 5 to 4.1 for a single-event publish. MongoDB dispatch writes 0.17 to 0.72 times the oplog entries and 0.22 to 0.61 times the oplog bytes.

**Why the benchmark gate failed, and why that is expected.** The gate flags `deliveries.delPerUnit` as "regressed from zero" in every dispatch cell, and `deletePerUnit` on MongoDB: 0.1.0 never deleted a delivery, 0.2.0 deletes each one on completion instead of updating it twice. That is the intended change, not a regression; the counter that matters is the total WAL and write volume, which fell. The one real counter regression is WAL per delivery with `defaults` on one process (1.40x): the completion inserts an `event_completions` row whose foreign-key check locks the event row, and the first change to a page after a checkpoint writes the whole page to WAL. It fades as pages are touched again within a checkpoint cycle and did not reduce throughput; see [the publish path](/performance/publish-path#dispatch-one-process-with-default-settings-pg-disp-1i-defaults).

### Write amplification and dead tuples

In 0.1.0 a delivery row was updated three times (claim, lease renewals, completion). Each update of an indexed column such as `status` writes a new row version plus an entry in every index, and leaves a dead tuple for autovacuum. Measured on 0.1.0: 0 % HOT updates and a steady stream of dead tuples and dead index entries. In 0.2.0 the same delivery is inserted once and deleted once; claiming and renewing update only one of 256 lane rows per batch, and those updates are 100 % HOT. A delete leaves one dead tuple per delivery instead of three and no dead index entries from updates. The tables are created with `autovacuum_vacuum_scale_factor = 0.02`; alert on `n_dead_tup` for sustained thousands of deliveries per second. The full explanation, with `pageinspect` evidence and diagrams, is in [Write amplification in the delivery path](/performance/write-amplification).

## Where it is not better

- **Keyed dispatch on four processes with 1,000 distinct keys:** the first full run showed 0.66x, but that came from one unusually fast 0.1.0 sample (7,072/s against 2,690–3,300/s in the re-runs). Two re-runs with 0.1.0 measured alongside put 0.2.0 at 1.61x and 1.93x, so the table shows the re-run range; it is not a regression.
- **MongoDB batch-100 publish:** 31,600 to 31,167 events/s (0.98x, three samples): no change, and a higher p99 for the transaction (79 ms to 126 ms, interval inconclusive).
- **Mixed load near saturation:** p99 can be higher than 0.1.0's, see above.
- **Single sample cells** may move by 10 % or more in either direction on a re-run. The directions are consistent with the exact counters, which is why they are listed.

## 0.5.0 vs 0.4.1

0.5.0 adds catalog routes, wire names and transactions at an isolation level; without `isolationLevel` a transaction runs the statements of 0.4.1. The numbers agree: publish and dispatch on PostgreSQL are within noise of 0.4.1, and the statements per event and per delivery are identical. Every sample was valid.

Machine fingerprint `2c551bf14f01` (the setup has changed since the runs above), candidate `adadab2` against `v0.4.1`, interleaved. Publish was measured with **6 samples per side**, dispatch with **3**. The machine was busy (load average 5 to 28 during the publish run), which is why single pairs move by up to 30 % either way. Throughput is candidate / base, the median of the per-pair ratios:

| Scenario                                     | Ratio | Interval (95 %) | Verdict      |
| -------------------------------------------- | ----: | --------------- | ------------ |
| PostgreSQL publish, P=8, 1 event per call    | 0.998 | 0.85 to 1.05    | inconclusive |
| PostgreSQL publish, P=8, 100 per call        | 1.006 | 0.84 to 1.20    | inconclusive |
| PostgreSQL publish, 100 per call, 5 handlers | 0.921 | 0.84 to 1.18    | inconclusive |
| PostgreSQL dispatch, 1 instance, `tuned`     | 1.019 | -               | no change    |
| PostgreSQL dispatch, 4 instances, `tuned`    | 1.014 | -               | within noise |
| PostgreSQL dispatch, 10 hot keys, `tuned`    | 0.964 | -               | within noise |

Database counters: publish counters (18 per cell) are all within 3 %, statements per event 1.000x. On the dispatch cells statements per delivery are 0.997x to 1.001x, WAL bytes per delivery 0.98x to 1.02x, and `lanes` updates per delivery 1.09x (1 instance), 1.08x (4 instances) and 1.00x (hot keys), inside the 15 % that this counter is gated at (it follows the timing of concurrent claims). The dispatch intervals are left out: that run stopped at the MongoDB cells, so its report was not written and the ratios above are computed from its stored samples.

MongoDB was not measured. On this machine the harness could not load the MongoDB template of the 0.4.1 side: a 500-event transaction of the loader ran past the 5 s transaction budget, three times. The candidate side was never reached, so this says nothing about 0.5.0 on MongoDB; the MongoDB store has no change in this release.

### Changes to the gate

From this release the quick gate generates the same event ids and keys on both sides (a seeded generator per publisher), so the lane layout no longer differs between base and candidate; the dispatch cells measure a fixed amount of work instead of a fixed time; counters of interleaved runs are judged per pair; and `lanes` updates per delivery gate at 15 % instead of 3 %. Identical code now passes against itself (two runs), and a deliberate regression (one extra `lanes` update per completion batch) fails it at +97 % (1 instance) and +85 % (4 instances).

### Reports

The raw samples and environment are in `benchmarks/results/runs/`:

- `2026-10-27T10-16-43-144Z`: the publish cells, 6 pairs (report written, gate PASS).
- `2026-10-27T10-05-19-668Z`: the PostgreSQL dispatch cells, 3 pairs (stopped at the MongoDB cells, no report).

## 0.4.0 vs 0.3.1

0.4.0 adds features (a `pool` for ORM stores, wake-up at the caller's commit, new Kafka options) and leaves the delivery path alone, and the numbers agree: publish and dispatch are within noise of 0.3.1 on PostgreSQL and MongoDB, and the statements per delivery are identical. Every sample was valid (6 of 6 per cell).

Same machine as above (fingerprint `f2d136dc9ad0`), candidate `a3b56bc` against `v0.3.1`, interleaved, **3 samples per side**. Throughput is candidate / base:

| Scenario                                     | Ratio | Interval (95 %) | Verdict      |
| -------------------------------------------- | ----: | --------------- | ------------ |
| PostgreSQL publish, P=8, 1 event per call    | 1.011 | 0.96 to 1.05    | no change    |
| PostgreSQL publish, P=8, 100 per call        | 1.005 | 1.00 to 1.37    | inconclusive |
| PostgreSQL publish, 100 per call, 5 handlers | 1.033 | 0.55 to 1.88    | inconclusive |
| MongoDB publish, P=8, 100 per call           | 0.984 | 0.97 to 1.17    | within noise |
| PostgreSQL dispatch, 1 instance, `tuned`     | 1.062 | 1.00 to 1.47    | inconclusive |
| PostgreSQL dispatch, 4 instances, `tuned`    | 1.048 | 0.91 to 1.13    | inconclusive |
| PostgreSQL dispatch, 10 hot keys, `tuned`    | 0.996 | 0.99 to 1.00    | no change    |
| MongoDB dispatch, 1 instance, `tuned`        | 1.009 | 0.97 to 1.20    | inconclusive |

Database counters per delivery: one delivery delete and one completion insert, as in 0.3.1. The PostgreSQL dispatch cells show `lanes` updates per delivery at 0.97x (1 instance) and 0.92x (4 instances) and WAL bytes at 0.95x and 0.88x; hot keys show WAL bytes at 1.04x and WAL records at 1.03x. Publish counters (18 per PostgreSQL cell, 6 per MongoDB cell) are all within 3 %.

### About the two counter flags

The benchmark gate flagged the 10 hot keys cell for WAL bytes (4.0 %) and WAL records (3.1 %) per delivery, so the run is reported as a gate failure. We do not read that as a regression, for this reason: the gate compares counters of two databases that the harness generates randomly, one preloaded for each side, and each side dispatches its own. To see how large that difference is on identical code, the harness ran `v0.3.1` against itself (A/A, `2026-10-25T18-59-07-978Z`). That run also failed the gate, with `lanes.updPerUnit` 9.5 % higher (1.09x) on a side that has no change at all. A separate candidate run of the same two dispatch cells (`2026-10-25T18-55-40-858Z`) passed: `lanes.updPerUnit` 0.90x and 18 counters within 3 % on the hot keys cell.

So in the one A/A run the counter gate moved by 9.5 % on identical code, well above its 3 % threshold; that is one run, not a measured noise floor. What this does and does not show: a 3 to 4 % difference in one cell is inside that floor and cannot be told from noise with 3 samples, and the statements per delivery, which do not depend on the preloaded data, are identical. It does not prove that WAL per delivery is exactly the same, only that nothing here separates it from 0.3.1.

### Reports

The raw reports, samples and environment of the three runs are in `benchmarks/results/runs/`:

- `2026-10-25T19-02-48-248Z`: the interleaved run of all the cells above (the quick gate set).
- `2026-10-25T18-59-07-978Z`: the A/A run, `v0.3.1` against `v0.3.1`, two dispatch cells.
- `2026-10-25T18-55-40-858Z`: the candidate against `v0.3.1` on the same two dispatch cells.

## 0.3.0 vs 0.2.0

0.3.0 is a code-quality release, and the numbers say it cost nothing: publish and MongoDB are unchanged within noise, most PostgreSQL dispatch cells are within about 10 to 20 % either way (which one sample cannot resolve), and keyed dispatch on PostgreSQL is 1.78 times faster in two cells. Every sample was checked for lost, duplicate and out-of-order deliveries: 0 of each.

Same machine, same method and same database images as above (fingerprint `0c08a14e7a55`). Two runs feed the tables:

- **Full run**, all cells, 39 minutes, candidate `a8587ff` against `v0.2.0`, interleaved. Each cell has **1 sample per side**, except where the table says 3. Every sample was valid except the two of PostgreSQL fan-out to 5 handlers on 4 processes (the preloaded backlog drained 0.5 s into the window on both sides), which are left out.
- **Targeted run** for the publish and mixed-load cells, candidate `5575bc4`, **2 samples per side**. The full run was taken before two publish fixes (`a13fb7e`, `2fb3cde`: the transaction budget no longer costs a statement per publish), so these cells were measured again. The 0.2.0 side of this run reused the stored 0.2.0 samples; it was not measured alongside the candidate, so machine drift is not cancelled out.

### Publish on PostgreSQL (targeted run, 2 samples)

| Scenario                                            | 0.2.0 events/s | 0.3.0 events/s | Ratio | Statements per event, 0.2.0 / 0.3.0 |
| --------------------------------------------------- | -------------: | -------------: | ----: | ----------------------------------: |
| 1 publisher, 1 event per call                       |          1,543 |          1,573 |  1.02 |                         4.07 / 4.07 |
| 1 publisher, 100 events per call                    |         18,030 |         17,960 |  1.00 |                         1.04 / 1.04 |
| 32 publishers, 1 event per call                     |          6,235 |          6,111 |  0.98 |                         4.02 / 4.02 |
| 32 publishers, 100 events per call                  |         69,580 |         68,493 |  0.98 |                         1.03 / 1.03 |
| 8 publishers, 1 event per call, 1,000 distinct keys |          3,789 |          4,145 |  1.09 |                         4.03 / 4.02 |

All intervals include 1.0, so none of these is a change.

A second targeted run (`2026-10-25T05-12-40-754Z`, candidate `13c2a13`, 2 samples per side) measured the remaining publish cells after the fixes:

| Scenario                                                              | Ratio 0.3.0 / 0.2.0 |
| --------------------------------------------------------------------- | ------------------: |
| PostgreSQL, 8 publishers, 1 event per call                            |                1.00 |
| PostgreSQL, 8 publishers, 100 events per call                         |                1.05 |
| PostgreSQL, 4 processes x 8 publishers, 100 events per call (ceiling) |                0.97 |
| PostgreSQL, 8 publishers, 1 event per call, 5 handlers                |                1.07 |
| PostgreSQL, 8 publishers, 100 events per call, 5 handlers             |                1.02 |
| PostgreSQL, 8 publishers, 1 event per call, 10 hot keys               |                0.98 |
| PostgreSQL, 8 publishers, 1 event per call, 1 hot key                 |                1.09 |
| MongoDB, 8 publishers, 1 event per call                               |                1.02 |
| MongoDB, 8 publishers, 100 events per call                            |                0.94 |
| MongoDB, 8 publishers, 100 events per call, 5 handlers                |                0.98 |
| MongoDB, 8 publishers, 1 event per call, 10 hot keys                  |                1.06 |

Every publish cell is now between 0.94 and 1.09 of 0.2.0, with two samples per side, so publishing is unchanged in 0.3.0.

### Dispatch on PostgreSQL (full run)

| Scenario                                         | 0.2.0 deliveries/s | 0.3.0 deliveries/s | Ratio | Samples |
| ------------------------------------------------ | -----------------: | -----------------: | ----: | :-----: |
| 1 process, `defaults`                            |              4,373 |              4,087 |  0.93 |  1 / 1  |
| 1 process, `mid`                                 |             11,140 |             10,890 |  0.98 |  1 / 1  |
| 1 process, `tuned`                               |             15,613 |             14,255 |  0.98 |  3 / 3  |
| 4 processes, `defaults`                          |             15,661 |             13,963 |  0.89 |  1 / 1  |
| 4 processes, `mid`                               |             34,278 |             40,803 |  1.19 |  1 / 1  |
| 4 processes, `tuned`                             |             53,844 |             56,831 |  1.06 |  1 / 1  |
| 8 processes, `defaults`                          |             18,561 |             19,274 |  1.04 |  1 / 1  |
| 8 processes, `mid`                               |             47,158 |             50,242 |  1.07 |  1 / 1  |
| 8 processes, `tuned`                             |             59,814 |             52,865 |  0.88 |  1 / 1  |
| Fan-out to 5 handlers, 1 process, `tuned`        |             20,850 |             20,701 |  0.99 |  1 / 1  |
| 1,000 distinct keys, 1 process, `tuned`          |              4,200 |              4,133 |  0.98 |  1 / 1  |
| 1,000 distinct keys, 4 processes, `tuned`        |              4,453 |              7,931 |  1.78 |  1 / 1  |
| 10 hot keys, 1 process, `tuned`                  |              2,022 |              1,955 |  0.97 |  1 / 1  |
| Routed, 1,000 distinct keys, 5 ms ack, 1 process |              3,267 |              5,827 |  1.78 |  1 / 1  |
| Routed, 10 hot keys, 5 ms ack, 1 process         |                785 |                787 |  1.00 |  1 / 1  |

The two 1.78 results are the ones this release is meant to help: with many keys and routes to one destination, per-key order now holds inside one handler instead of across several, so more keys can proceed at once. They are single samples, so the multiple is indicative; the direction is the same in two different cells. The 0.88 and 0.89 cells (8 processes `tuned`, 4 processes `defaults`) are as likely noise as a slowdown; one sample cannot tell, and a repeat run with three samples is the way to find out. Database counters, which do not depend on machine noise: WAL bytes per delivery were 8 to 11 % higher in four cells (1 process `tuned` with three samples, 4 processes `defaults` and `mid`, 8 processes `tuned`) and 5 to 8 % lower in five others. The benchmark gate flags the higher ones, which is why 0.3.0 is not described as faster per write.

### Dispatch on MongoDB (full run)

| Scenario                                  | 0.2.0 deliveries/s | 0.3.0 deliveries/s | Ratio | Samples |
| ----------------------------------------- | -----------------: | -----------------: | ----: | :-----: |
| 1 process, `defaults`                     |              1,149 |              1,161 |  1.01 |  1 / 1  |
| 1 process, `tuned`                        |              6,431 |              6,228 |  0.98 |  3 / 3  |
| 4 processes, `defaults`                   |              4,470 |              4,564 |  1.02 |  1 / 1  |
| 4 processes, `tuned`                      |             21,771 |             22,452 |  1.03 |  1 / 1  |
| Fan-out to 5 handlers, 1 process, `tuned` |              7,031 |              7,452 |  1.06 |  1 / 1  |
| 1,000 distinct keys, 1 process, `tuned`   |              2,089 |              2,131 |  1.02 |  1 / 1  |
| 10 hot keys, 1 process, `tuned`           |                347 |                349 |  1.01 |  1 / 1  |

MongoDB is unchanged. MongoDB publish was also within noise (0.94 to 0.98 in five cells, one of them with three samples and an inconclusive interval).

### Mixed load

| Scenario                        | Accepted events/s 0.2.0 to 0.3.0 | p99 latency 0.2.0 | p99 latency 0.3.0 | Samples |
| ------------------------------- | -------------------------------: | ----------------: | ----------------: | :-----: |
| PostgreSQL, 1,000/s, `defaults` |                    1,000 / 1,000 |            9.9 ms |           10.4 ms |  1 / 1  |
| PostgreSQL, 1,000/s, `tuned`    |                    1,000 / 1,000 |            9.0 ms |           14.4 ms |  1 / 1  |
| PostgreSQL, 5,000/s, `tuned`    |                    4,133 / 4,032 |    not meaningful |    not meaningful |  2 / 2  |
| MongoDB, 1,000/s, `tuned`       |                    1,000 / 1,000 |            474 ms |            377 ms |  1 / 1  |
| MongoDB, 5,000/s, `tuned`       |                      (saturated) |             7.5 s |             8.1 s |  1 / 1  |

At 5,000 per second both versions are past saturation and the p99 is dominated by queueing. The same cell gave 79 ms against 10.6 s in the full run and 2.0 s against 104 ms in the targeted run, so its p99 ratios (0.05 to 134) say nothing about the code. Accepted throughput, 0.98, is the number to read. The 14.4 ms against 9.0 ms at 1,000 per second is one sample.

### Trust

Same rule as above: one sample per side cannot separate a difference below about 10 % from noise, and where a ratio is outside that, the cell is called out. The exact counters (statements per publish, rows written, exactness) are the part that does not depend on the laptop. To check any cell on your hardware, run `pnpm bench:full --base v0.2.0 --only <cell>` with more samples.

## Deep dives

- [Write amplification in the delivery path](/performance/write-amplification): why delivery rows were rewritten, lane leases, delete-on-complete and which guarantees are preserved.
- [The publish path and prepared statements](/performance/publish-path): why three cells were slower on the first 0.2.0 run, the round trips and plan times, and the fix.
- [Tuning for your workload](/tuning): the settings that follow from these numbers.

## Reproduce it

```sh
pnpm install --frozen-lockfile
pnpm bench:full --base v0.1.0 --candidate v0.2.0 --interleave
```

Requires Docker and Node 24. Output (`report.md`, `comparison.json`, `samples.jsonl`, `env.json`) goes to `benchmarks/results/runs/<timestamp>/`. [`benchmarks/README.md`](https://github.com/berkcansavur/relaybus/blob/main/benchmarks/README.md) describes the harness, the cells and how verdicts are decided.
