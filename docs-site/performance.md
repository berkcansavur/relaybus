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

| Scenario                                    | 0.1.0 deliveries/s | 0.2.0 deliveries/s |  Ratio | Samples |
| ------------------------------------------- | -----------------: | -----------------: | -----: | :-----: |
| 1 process, `defaults` (*)                   |             ~3,100 |             ~4,000 |  1.29x |    1    |
| 1 process, `mid`                            |              3,136 |             10,273 |  3.28x |    1    |
| 1 process, `tuned`                          |              9,329 |             14,348 |  1.82x |    3    |
| 4 processes, `defaults`                     |              2,334 |             10,043 |  4.30x |    1    |
| 4 processes, `mid`                          |              2,805 |             32,306 | 11.52x |    1    |
| 4 processes, `tuned`                        |              3,242 |             55,629 | 17.16x |    1    |
| 8 processes, `defaults`                     |              5,478 |             17,371 |  3.17x |    1    |
| 8 processes, `mid`                          |              8,750 |             45,632 |  5.22x |    1    |
| 8 processes, `tuned`                        |              9,736 |             62,937 |  6.46x |    1    |
| Fan-out to 5 handlers, 1 process, `tuned`   |              8,220 |             17,838 |  2.17x |    1    |
| Fan-out to 5 handlers, 4 processes, `tuned` |              2,964 |             54,474 | 18.38x |    1    |
| 1,000 distinct keys, 1 process, `tuned`     |              2,307 |              7,332 |  3.18x |    1    |
| 1,000 distinct keys, 4 processes, `tuned`   |              7,072 |              4,660 |  0.66x |    1    |
| 10 hot keys, 1 process, `tuned`             |              1,110 |              1,565 |  1.41x |    1    |

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

- **Keyed dispatch on four processes with 1,000 distinct keys:** 7,072 to 4,660 deliveries/s (0.66x, one sample). Keyed claims still do per-key reads, and four processes contend for lanes that hold many keys. This is the only throughput cell below 1.0 for dispatch and is not yet investigated.
- **MongoDB batch-100 publish:** 31,600 to 31,167 events/s (0.98x, three samples): no change, and a higher p99 for the transaction (79 ms to 126 ms, interval inconclusive).
- **Mixed load near saturation:** p99 can be higher than 0.1.0's, see above.
- **Single sample cells** may move by 10 % or more in either direction on a re-run. The directions are consistent with the exact counters, which is why they are listed.

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
