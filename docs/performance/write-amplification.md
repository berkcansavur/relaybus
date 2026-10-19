# Write amplification in the delivery path

This document explains why the first version of relaybus (0.1.0) spent most of its database time rewriting delivery rows,
what Phase 1 changed, which guarantees that change must not weaken and how each one is preserved, and what it measured.

- [The problem](#the-problem)
- [What changed](#what-changed)
- [Guarantees and how each one is preserved](#guarantees-and-how-each-one-is-preserved)
- [Measurements](#measurements)
- [MongoDB](#mongodb)
- [Upgrading from 0.1.0](#upgrading-from-010)
- [Trade-offs and operations](#trade-offs-and-operations)
- [How to reproduce](#how-to-reproduce)

## The problem

A delivery is the unit of work: one row per (event, handler). In 0.1.0 a delivery row went through three writes after
it was inserted:

1. **claim**: `UPDATE ... SET status = 'processing', attempts = attempts + 1, lease_owner, lease_token, lease_expires_at`
2. **lease renewals** while the handler runs: `UPDATE ... SET lease_expires_at`
3. **completion**: `UPDATE ... SET status = 'completed', completed_at, lease cleared`

The table also carried eight indexes (due time, ordering keys, status, handler, purge). Several of them contain
`status` or the due time, and those are exactly the columns the three updates change.

### Dead tuples and HOT, in short

PostgreSQL never changes a row in place. An `UPDATE` writes a **new version** of the row and leaves the old version
behind as a **dead tuple** that autovacuum removes later. Normally the new version also needs a new entry in **every
index** of the table, because each index points at the physical location of a version.

There is one shortcut, a **HOT** (heap-only tuple) update. If the update changes no indexed column, and the new version
fits on the same page, PostgreSQL chains the new version to the old one inside the page and adds **no index entries**:
the indexes keep pointing at the first version of the chain. HOT updates are cheap and their dead versions are pruned
from the page on the fly. A single update of an indexed column (here `status`) disables the shortcut for that update.

0.1.0 never got HOT updates, because every state change touched `status` or the due time. Measured on the baseline
(`benchmarks/results/main`, one to eight dispatcher processes): **0% HOT updates, about 2.3 KB of WAL per delivery**
and a steady stream of dead tuples and dead index entries.

### What one delivery leaves behind

`benchmarks/pageinspect.ts` publishes one event with one handler, claims it and completes it, then reads the heap page
and every index with `pageinspect` (PostgreSQL 17, autovacuum off so the evidence stays). Before (the complete output
is in [`pageinspect-before.txt`](pageinspect-before.txt)):

```text
heap page of relaybus_deliveries (3 line pointers)
  lp  xmin   xmax   ctid    flags
  1   775    776    (0,2)   XMIN_COMMITTED|XMAX_COMMITTED
  2   776    777    (0,3)   XMIN_COMMITTED
  3   777    0      (0,3)   XMAX_INVALID
indexes of relaybus_deliveries: 10
  relaybus_deliveries_completed_idx              entries=1  -> heap tids (0,3)
  relaybus_deliveries_due_idx                    entries=2  -> heap tids (0,1) (0,2)
  relaybus_deliveries_due_unkeyed_idx            entries=2  -> heap tids (0,1) (0,2)
  relaybus_deliveries_event_handler_unique       entries=3  -> heap tids (0,1) (0,2) (0,3)
  relaybus_deliveries_handler_created_idx        entries=3  -> heap tids (0,1) (0,2) (0,3)
  relaybus_deliveries_pkey                       entries=3  -> heap tids (0,1) (0,2) (0,3)
  relaybus_deliveries_status_created_idx         entries=3  -> heap tids (0,3) (0,1) (0,2)
tuple counters:
  relaybus_deliveries      inserted=1 updated=2 hot_updated=0 deleted=0
```

Three heap tuples where each points at the next through `ctid` (none is `HEAP_ONLY` or `HEAP_HOT_UPDATED`: no HOT), two
of them dead, and **17 index entries** for one delivery (6 at publish, 6 at claim, 5 at completion). Everything except the
last tuple and its last entries is garbage that autovacuum has to find, and vacuuming the indexes costs more than
vacuuming the heap.

![Life of one delivery row, before and after](row-lifecycle.svg)

## What changed

The idea comes from queue implementations that survived this problem (Oban, River, graphile-worker, pg-boss, PgQ):
**do not write the row to say who has it, and delete the row when it is done.**

### Lane leases

Every delivery gets a `lane`, a number from 0 to 255 computed from its ordering key (or from the event id when it has
none). All deliveries of one key share a lane. Workers lease **lanes**, not rows.

![Lane lease model](lane-leases.svg)

```sql
CREATE TABLE relaybus_lanes (
  lane smallint PRIMARY KEY CHECK (lane >= 0 AND lane < 256),
  epoch bigint NOT NULL DEFAULT 0,          -- fencing token, +1 every time the lane changes hands
  lease_owner text,
  lease_expires_at timestamptz,
  in_flight text[] NOT NULL DEFAULT '{}'    -- ids of the deliveries the owner has claimed
) WITH (fillfactor = 50);
```

- **Claim** (one short transaction): lock the lane rows this worker already holds, take free lanes that have due work
  (four, then eight, ... at a time, so a worker holds only as many lanes as its batch needs), read the due deliveries of
  those lanes, then write **one lane row per lane** to record the batch in `in_flight`. No delivery row is written and no
  row lock is taken on one.
- **Renew**: `extendLease` updates the lane row, not the delivery.
- **Complete**: delete the delivery rows. The dispatcher groups the completions of deliveries that finish within
  `dispatcher.completionBatchWindow` (2 ms by default) into one `completeMany` call, which is one statement
  (`DELETE ... USING` the fenced lane rows).
- **Fail** (rare): `reschedule` and `bury` update the delivery (non-HOT, accepted) and remove it from `in_flight`.

The lane table has no secondary index and 50% free space per page, so every update of a lane row is heap-only. Measured
for one claim and completion: `relaybus_lanes inserted=256 updated=2 hot_updated=2` and, in the heap page, the chain
`HOT_UPDATED -> HOT_UPDATED|HEAP_ONLY -> HEAP_ONLY` ([`pageinspect-after.txt`](pageinspect-after.txt)):

```text
heap page of relaybus_deliveries (1 line pointers)
  lp  xmin   xmax   ctid    flags
  1   782    784    (0,1)   XMIN_COMMITTED
indexes of relaybus_deliveries: 6        -- 3 of them hold the row, the others stay empty
  relaybus_deliveries_event_handler_unique       entries=1  -> heap tids (0,1)
  relaybus_deliveries_lane_due_idx               entries=1  -> heap tids (0,1)
  relaybus_deliveries_pkey                       entries=1  -> heap tids (0,1)
lane 218: the lane row this claim touched, versions of that row only
  lp  xmin   xmax   ctid    flags
  18  779    783    (3,56)  HOT_UPDATED|XMIN_COMMITTED|XMAX_COMMITTED
  56  783    783    (3,57)  HOT_UPDATED|HEAP_ONLY|XMIN_COMMITTED|XMAX_COMMITTED
  57  783    784    (3,57)  HEAP_ONLY|XMIN_COMMITTED
tuple counters:
  relaybus_deliveries      inserted=1 updated=0 hot_updated=0 deleted=1
  relaybus_lanes           inserted=256 updated=2 hot_updated=2 deleted=0
```

One heap tuple (deleted by the completion, not rewritten) and **3 index entries** instead of 17. The lane row's updates
added no index entry and are shared by every delivery of the batch.

### Delete on complete

A completed delivery is deleted. `listDeliveries({ status: 'completed' })` and `statistics().completed` are empty unless
the store keeps a **completion history** (`completionHistory: true`): an append-only table, one insert per completion,
no updates. Events are still removed by retention (`retention.completed`) once they have no deliveries left.

### Schema without hot-path index churn

Indexes on `deliveries` now only cover columns that no happy-path statement writes:

| index                                                                         | serves                                       |
| ----------------------------------------------------------------------------- | -------------------------------------------- |
| `(event_id, handler)` unique, primary key                                     | idempotent publish, fenced single-row writes |
| `(lane, available_at, id) WHERE status = 'pending'`                           | claim, statistics                            |
| `(lane, handler, key, sequence) WHERE key IS NOT NULL AND status = 'pending'` | per-key ordering check, blocked-key claim    |
| `(updated_at, id) WHERE status = 'dead'`                                      | dead-letter purge                            |

`status_created` and `handler_created` (listing) and the unkeyed and processing partial indexes are gone. The 0.1.0
indexes also made the planner choose `status_created` for `WHERE status = 'processing' AND id = ...` updates (357 to
525 buffers per single-row `UPDATE`, against 18 to 50 through the primary key). Fenced writes now always go through the
primary key (`EXPLAIN` of a 50-delivery completion: 308 buffers, 0.23 ms). Both the lane table (`fillfactor = 50`) and
`deliveries` (`autovacuum_vacuum_scale_factor = 0.02`) carry storage parameters set by `setup()`.

### Events remember their handlers

Because the delivery row is gone after completion, `events.handlers` records every handler an event ever had a delivery
for. A broker redelivery or a publish retry with the same event id adds deliveries only for handlers that are not listed,
so a completed handler never runs twice (the RabbitMQ and Kafka inbox tests pin this). This costs one `text[]` on the event
insert and no update.

## Guarantees and how each one is preserved

| guarantee                                       | how it is preserved                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Atomic publish**                              | Unchanged: events and deliveries are inserted in the caller's transaction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| **Never lose an event**                         | A delivery row exists from publish until its handler's completion is committed. A claim writes only to the lane. If the worker dies, the delivery is still a `pending` row, and the lane lease expires and is taken over, so it is redelivered. A completion deletes the row only after the handler returned (or in the handler's own transaction).                                                                                                                                                                                                                                                                                                                                                                                     |
| **Per-handler retry and dead letter**           | Each (event, handler) is its own row. `reschedule` and `bury` update that row exactly as before; a dead row is never claimed again; `requeue` moves it back with a fresh budget.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Per-key ordering in commit order**            | Unchanged on the publish side (the key counter row is locked until commit). On the claim side a keyed delivery is picked only if no earlier delivery of its handler and key is still a `pending` row (a running one is one) and none of its key is in flight; and a key lives in one lane, owned by one worker at a time.                                                                                                                                                                                                                                                                                                                                                                                                               |
| **At-least-once**                               | An expired lane lease is taken over by the next claimer, so everything its holder had in flight runs again. Completion is the only thing that removes a row.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| **Effectively-once for transactional handlers** | `complete(lease, now, transaction)` takes `FOR KEY SHARE` on the lane row inside the handler's transaction and deletes the delivery there. A takeover locks the lane `FOR UPDATE`, which conflicts, so it skips the lane until the transaction ends. (The weakest conflicting lock is used on purpose: the owner's own claim and lease renewals are plain updates of the lane row and must not wait for every completion in progress.) Either the handler's writes and the deletion commit together, or the takeover finds the delivery and redelivers it into a transaction whose first completion rolled back. Both cannot commit. Tested in `lane-leases.integration.test.ts` ("does not let a completion and a takeover both win"). |
| **Multi-process safety, fencing**               | The lease token is `lane:epoch`. Every state change checks that the lane still has that epoch and still lists the delivery in `in_flight`. A takeover bumps the epoch, so all writes of the previous holder return `false`. Two claimers never see the same lane: the claim takes free lanes with `FOR UPDATE SKIP LOCKED`.                                                                                                                                                                                                                                                                                                                                                                                                             |
| **Attempts and poison messages**                | The claim does not write an attempt. The attempt is recorded when it ends in failure (`reschedule`/`bury` add one), and when a lane is taken over each delivery that was in its `in_flight` list is charged one attempt (a crash is a lost attempt). A crash loop therefore still counts and ends in the dead letters (contract test "keeps counting attempts of a poison delivery until it is dead-lettered"). A graceful `release` charges nothing.                                                                                                                                                                                                                                                                                   |
| **Graceful shutdown**                           | Aborted handlers are `release`d (no attempt spent), then the dispatcher calls `relinquish`, which hands back every lane that runs nothing, so other workers do not wait for the lease to expire.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Inbox deduplication**                         | `events.handlers` outlives the deliveries (see above).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

## Measurements

Both columns come from **the same harness** (`benchmarks/`, including the exactness validation described in
`benchmarks/README.md`), run on the same machine: the 0.1.0 baseline at commit `e222e0a` and Phase 1 at `perf/phase-1`. **Every one of
the 51 drain/mixed runs of each side is VALID**: for every (event, handler) pair the handled set equals the published set (lost 0,
unexpected 0), duplicates 0, per-key order violations 0, nothing left pending and no dead letters when the run finished.

Read with care: the machine is a laptop (Apple M4, Docker Desktop VM) that was shared with other work during these runs (load average
around 8), so absolute numbers vary by tens of percent between runs and the two columns were not taken back to back. A short idle re-check
of the default configuration (20k events, `--quick`) gave 1 instance, defaults: 2,215 -> 3,439 deliveries/s, and 4 instances, tuned:
5,970 -> 28,254, which is why the full-run row "1 inst, defaults" (2,165 -> 1,586) should be treated as noise. Quote ratios, not absolute values.
"Dead tuples after" is `n_dead_tup` of the deliveries table right after the window: before it counts the two dead versions per delivery that
updates leave, after it counts the one deleted tuple per delivery that autovacuum has not reached yet (`autovacuum` was on; runs are short).

**Headline.** PostgreSQL dispatch: up to **47,651** deliveries/s (8 dispatcher processes, tuned) against 3,958 before (12x); with one tuned
process 2,631 -> 12,372 (4.7x). WAL per delivery **2,377-2,692 B -> 257-625 B** (about 5-9x less), deliveries updated **0** (was 2 per delivery), lane
updates **100% HOT** (HOT updates on deliveries were 0% before and are not applicable now). MongoDB dispatch: **547-611 -> up to 10,422** deliveries/s (17x with
4 processes), oplog bytes per delivery **1,155 -> 198-266** and updates per delivery **1.0 -> 0.02-0.21**.

Known regressions to track (not fixed in Phase 1): single-event publish on both stores is slower than 0.1.0 in these runs (PostgreSQL
`P=1 batch=1` 1,350 -> 479 events/s in the full run, 867 -> 705 in the idle re-check; MongoDB large-batch publish about 0.2-0.4x), which
is the cost of the extra `events.handlers` bookkeeping and of the claim-side work moving to publish time (lane hash, richer documents). They need a
dedicated idle-machine investigation; the dispatch gains above dwarf them for any workload that processes what it publishes, but they are real.

### pg: dispatch (scenario 2, 200k preloaded events, no-op handler)

| config                                                   | deliveries/s (before -> after) | WAL B/delivery   | HOT upd % (deliveries / lanes) | dead tuples after      | stmts/delivery   |
| -------------------------------------------------------- | ------------------------------ | ---------------- | ------------------------------ | ---------------------- | ---------------- |
| 1 inst, defaults (conc 10, batch 10, pool 10, poll 1s)   | 2,165 -> **1,586** (0.7x)      | 2,377 -> **625** | 0.0 / 100                      | 74,678 -> **62,904**   | 1.21 -> **0.21** |
| 1 inst, conc 10, batch 50                                | 1,784 -> **3,146** (1.8x)      | 2,345 -> **550** | 0.0 / 100                      | 128,285 -> **0**       | 1.25 -> **0.21** |
| 1 inst, conc 50, batch 50, pool 20                       | 2,696 -> **5,337** (2.0x)      | 2,374 -> **384** | 0.0 / 100                      | 123,533 -> **135,182** | 1.06 -> **0.04** |
| 1 inst, conc 50, batch 200, pool 20                      | 2,554 -> **4,263** (1.7x)      | 2,372 -> **388** | 0.0 / 100                      | 135,746 -> **160,014** | 1.06 -> **0.04** |
| 1 inst, tuned (conc 200, batch 200, pool 40, poll 50ms)  | 2,631 -> **12,372** (4.7x)     | 2,517 -> **360** | 0.0 / 98                       | 61,146 -> **142,111**  | 1.02 -> **0.01** |
| 1 inst, tuned+ (conc 400, batch 400, pool 64, poll 50ms) | 7,613 -> **14,825** (1.9x)     | 2,432 -> **385** | 0.0 / 99                       | 316,279 -> **110,415** | 1.01 -> **0.01** |
| 4 inst, defaults (conc 10, batch 10, pool 10, poll 1s)   | 2,411 -> **5,299** (2.2x)      | 2,378 -> **461** | 0.0 / 100                      | 161,297 -> **188,701** | 1.25 -> **0.21** |
| 4 inst, conc 10, batch 50                                | 4,053 -> **7,090** (1.7x)      | 2,291 -> **458** | 0.0 / 100                      | 337,389 -> **139,203** | 1.23 -> **0.21** |
| 4 inst, conc 50, batch 50, pool 20                       | 2,377 -> **5,426** (2.3x)      | 2,529 -> **387** | 0.0 / 99                       | 125,462 -> **169,648** | 1.06 -> **0.04** |
| 4 inst, conc 50, batch 200, pool 20                      | 6,727 -> **11,039** (1.6x)     | 2,438 -> **368** | 0.0 / 99                       | 313,921 -> **51,311**  | 1.05 -> **0.04** |
| 4 inst, tuned (conc 200, batch 200, pool 40, poll 50ms)  | 7,338 -> **18,789** (2.6x)     | 2,540 -> **257** | 0.0 / 94                       | 308,922 -> **200,000** | 1.02 -> **0.01** |
| 4 inst, tuned+ (conc 400, batch 400, pool 64, poll 50ms) | 2,486 -> **25,437** (10.2x)    | 2,661 -> **286** | 0.0 / 97                       | 141,181 -> **200,000** | 1.01 -> **0.01** |
| 8 inst, defaults (conc 10, batch 10, pool 10, poll 1s)   | 3,917 -> **7,019** (1.8x)      | 2,361 -> **457** | 0.0 / 100                      | 346,585 -> **156,411** | 1.24 -> **0.21** |
| 8 inst, conc 10, batch 50                                | 1,965 -> **4,390** (2.2x)      | 2,444 -> **460** | 0.0 / 100                      | 132,804 -> **177,232** | 1.27 -> **0.21** |
| 8 inst, conc 50, batch 50, pool 20                       | 2,517 -> **26,541** (10.5x)    | 2,594 -> **387** | 0.0 / 98                       | 178,156 -> **186,785** | 1.06 -> **0.04** |
| 8 inst, conc 50, batch 200, pool 20                      | 2,490 -> **26,811** (10.8x)    | 2,605 -> **280** | 0.0 / 99                       | 140,228 -> **200,000** | 1.06 -> **0.04** |
| 8 inst, tuned (conc 200, batch 200, pool 40, poll 50ms)  | 3,958 -> **47,651** (12.0x)    | 2,601 -> **257** | 0.0 / 92                       | 332,611 -> **200,000** | 1.02 -> **0.01** |
| 8 inst, tuned+ (conc 400, batch 400, pool 64, poll 50ms) | 2,069 -> **30,332** (14.7x)    | 2,692 -> **285** | 0.0 / 95                       | 159,433 -> **200,000** | 1.01 -> **0.01** |

### pg: publish only (scenario 1)

| config                                   | events/s                    | publish call p50 / p99 ms          | WAL B/event        |
| ---------------------------------------- | --------------------------- | ---------------------------------- | ------------------ |
| P=1 batch=1 pool=default                 | 1,350 -> **479** (0.4x)     | 0.40 / 0.9 -> **0.98 / 4.9**       | 1,768 -> **1,485** |
| P=1 batch=100 pool=default               | 15,507 -> **14,733** (1.0x) | 5.28 / 11.8 -> **5.18 / 13.5**     | 1,797 -> **1,487** |
| P=8 batch=1 pool=default                 | 3,617 -> **2,604** (0.7x)   | 0.94 / 2.3 -> **1.48 / 4.0**       | 1,779 -> **1,507** |
| P=8 batch=100 pool=default               | 48,167 -> **29,593** (0.6x) | 9.99 / 25.7 -> **14.72 / 84.8**    | 1,905 -> **1,545** |
| P=32 batch=1 pool=default                | 3,737 -> **2,339** (0.6x)   | 1.13 / 2.9 -> **2.14 / 5.6**       | 1,776 -> **1,503** |
| P=32 batch=100 pool=default              | 56,807 -> **42,987** (0.8x) | 11.48 / 29.5 -> **15.02 / 43.5**   | 1,917 -> **1,551** |
| P=8 batch=1 pool=10                      | 3,343 -> **1,732** (0.5x)   | 0.95 / 2.1 -> **2.22 / 6.4**       | 1,779 -> **1,518** |
| P=8 batch=100 pool=10                    | 59,633 -> **47,720** (0.8x) | 9.13 / 21.1 -> **12.19 / 29.0**    | 1,897 -> **1,538** |
| P=32 batch=1 pool=34                     | 5,135 -> **3,227** (0.6x)   | 2.34 / 7.6 -> **3.55 / 15.9**      | 1,773 -> **1,510** |
| P=32 batch=100 pool=34                   | 58,727 -> **33,987** (0.6x) | 35.69 / 93.7 -> **53.67 / 309.2**  | 1,992 -> **1,585** |
| ceiling 4 procs x 8 publishers batch=1   | 4,789 -> **3,046** (0.6x)   | 2.38 / 9.1 -> **3.66 / 17.4**      | 1,773 -> **1,506** |
| ceiling 4 procs x 8 publishers batch=100 | 48,080 -> **35,167** (0.7x) | 43.12 / 142.3 -> **54.75 / 238.8** | 1,988 -> **1,593** |

### pg: mixed steady state (scenario 5), end-to-end commit-to-handler latency

| config                       | offered/s | achieved/s (before -> after) | e2e p50 / p95 / p99 ms before  | e2e p50 / p95 / p99 ms after   | backlog max (before -> after) |
| ---------------------------- | --------- | ---------------------------- | ------------------------------ | ------------------------------ | ----------------------------- |
| 1000/s, defaults dispatchers | 1,000     | 1,000 -> **1,000**           | 503.6 / 952.0 / 1,000.5        | 2,262.5 / 7,034.2 / 8,085.7    | 854 -> 4,731                  |
| 5000/s, defaults dispatchers | 5,000     | 3,383 -> **2,627**           | 17,053.7 / 39,731.6 / 42,175.9 | 11,684.4 / 33,548.4 / 40,128.9 | 142,912 -> 91,608             |
| 1000/s, tuned dispatchers    | 1,000     | 981 -> **1,000**             | 12.4 / 89.2 / 186.2            | 15.8 / 148.1 / 297.2           | 128 -> 197                    |
| 5000/s, tuned dispatchers    | 5,000     | 1,897 -> **2,144**           | 89.2 / 285.6 / 396.6           | 127.6 / 442.5 / 658.8          | 703 -> 627                    |
| 10000/s, tuned dispatchers   | 10,000    | 2,149 -> **2,421**           | 39.4 / 90.9 / 131.4            | 128.8 / 479.1 / 764.8          | 148 -> 906                    |

### mongo: dispatch (scenario 2, 200k preloaded events, no-op handler)

| config                                                  | deliveries/s (before -> after) | oplog B/delivery | oplog entries/delivery | journal B/delivery | ops/delivery   | updates/delivery |
| ------------------------------------------------------- | ------------------------------ | ---------------- | ---------------------- | ------------------ | -------------- | ---------------- |
| 1 inst, defaults (conc 10, batch 10, pool 10, poll 1s)  | 547 -> **1,069** (2.0x)        | 1,157 -> **266** | - -> **0.31**          | 1,445 -> **227**   | 2.8 -> **1.0** | 1.01 -> **0.21** |
| 1 inst, conc 50, batch 50, pool 20                      | 597 -> **3,359** (5.6x)        | 1,155 -> **219** | - -> **0.18**          | 1,431 -> **142**   | 2.2 -> **0.2** | 1.00 -> **0.08** |
| 1 inst, tuned (conc 200, batch 200, pool 40, poll 50ms) | 609 -> **5,621** (9.2x)        | 1,155 -> **198** | - -> **0.12**          | 1,419 -> **108**   | 2.1 -> **0.1** | 1.00 -> **0.02** |
| 4 inst, defaults (conc 10, batch 10, pool 10, poll 1s)  | 590 -> **2,901** (4.9x)        | 1,156 -> **262** | - -> **0.30**          | 1,424 -> **210**   | 5.6 -> **1.0** | 1.00 -> **0.20** |
| 4 inst, conc 50, batch 50, pool 20                      | 452 -> **7,721** (17.1x)       | 1,155 -> **223** | - -> **0.20**          | 1,419 -> **146**   | 5.1 -> **0.3** | 1.00 -> **0.09** |
| 4 inst, tuned (conc 200, batch 200, pool 40, poll 50ms) | 611 -> **10,422** (17.1x)      | 1,155 -> **199** | - -> **0.13**          | 1,414 -> **110**   | 5.0 -> **0.1** | 1.00 -> **0.03** |

### mongo: publish only (scenario 1)

| config                                   | events/s                    | publish call p50 / p99 ms             | oplog B/event      |
| ---------------------------------------- | --------------------------- | ------------------------------------- | ------------------ |
| P=1 batch=1 pool=default                 | 454 -> **360** (0.8x)       | 1.12 / 3.0 -> **1.06 / 3.7**          | 1,294 -> **1,351** |
| P=1 batch=100 pool=default               | 9,693 -> **2,453** (0.3x)   | 7.48 / 16.1 -> **26.48 / 92.8**       | 1,060 -> **1,109** |
| P=8 batch=1 pool=default                 | 2,021 -> **830** (0.4x)     | 2.01 / 4.8 -> **3.55 / 16.9**         | 1,300 -> **1,348** |
| P=8 batch=100 pool=default               | 37,587 -> **13,200** (0.4x) | 13.20 / 43.5 -> **37.51 / 123.8**     | 1,056 -> **1,107** |
| P=32 batch=1 pool=default                | 2,813 -> **1,839** (0.7x)   | 4.93 / 14.3 -> **6.77 / 21.9**        | 1,301 -> **1,353** |
| P=32 batch=100 pool=default              | 30,140 -> **5,293** (0.2x)  | 65.49 / 248.4 -> **297.18 / 1,781.8** | 1,062 -> **1,098** |
| ceiling 4 procs x 8 publishers batch=1   | 1,802 -> **1,083** (0.6x)   | 6.91 / 29.2 -> **5.95 / 29.2**        | 1,286 -> **1,342** |
| ceiling 4 procs x 8 publishers batch=100 | 25,293 -> **17,633** (0.7x) | 72.34 / 318.6 -> **105.58 / 377.3**   | 1,059 -> **1,107** |

### mongo: mixed steady state (scenario 5), end-to-end commit-to-handler latency

| config                       | offered/s | achieved/s (before -> after) | e2e p50 / p95 / p99 ms before  | e2e p50 / p95 / p99 ms after | backlog max (before -> after) |
| ---------------------------- | --------- | ---------------------------- | ------------------------------ | ---------------------------- | ----------------------------- |
| 1000/s, defaults dispatchers | 1,000     | 1,000 -> **1,000**           | 15,906.3 / 27,222.2 / 27,769.4 | 1,581.3 / 3,141.8 / 3,720.9  | 27,750 -> 2,080               |
| 1000/s, tuned dispatchers    | 1,000     | 1,000 -> **1,000**           | 20,398.8 / 31,920.2 / 32,887.4 | 168.5 / 513.7 / 1,083.4      | 33,814 -> 594                 |
| 5000/s, tuned dispatchers    | 5,000     | 1,405 -> **1,658**           | 26,160.0 / 45,218.2 / 45,218.2 | 3,019.3 / 6,895.6 / 8,414.0  | 63,928 -> 7,541               |

## MongoDB

The MongoDB store had the same problem in another form: every claim was one conditional `findOneAndUpdate` per delivery (plus per-key reads),
every completion one `updateOne` per delivery, and each of them rewrote indexed fields (`status`, `dueAt`, lease fields): about 3 oplog
entries and 3 index rewrites per delivery and roughly 2.5 round trips. The 0.1.0 baseline tops out near 600 deliveries/s for one dispatcher,
many times slower than PostgreSQL.

Phase 1 gives MongoDB the same model, adapted to what MongoDB can do atomically:

- **Lane documents.** A `<prefix>lanes` collection holds 256 documents `{ _id: lane, epoch, owner, holder, leaseExpiresAt, inFlight, lost }`.
  Deliveries carry a `laneId` (FNV-1a hash of the ordering key or event id, modulo 256). The existing `lane: 'ready' | 'parked'` field of a
  delivery (claim-index membership) is unchanged; `laneId` is the lease partition.
- **Claim without delivery writes.** A claim reads the candidates of the lanes it holds (and, a few at a time, free lanes with due work), checks
  the per-key rules, and records the batch with **one conditional `bulkWrite` on the lane documents**. No delivery document is written, so no
  indexed field of a delivery changes on the happy path. The lease token is `<laneId>:<epoch>`.
- **Completion.** `completeMany` deletes the delivery documents of a batch in one transaction (and inserts the optional `completed_deliveries`
  history documents). There is no per-delivery update.
- **Fencing without a lane write on completion.** MongoDB transactions use snapshot isolation, so two completions in one lane that both wrote the
  lane document would conflict with each other and transactional handlers would re-run. Instead a takeover moves the lane's `inFlight` ids into
  `lost` in the very write that bumps the epoch, and then charges each of them one attempt; a completion deletes the same delivery documents the
  charge updates. A completion and a takeover therefore write the same documents and exactly one of them commits; the other conflicts,
  retries, finds the new epoch and reports the lease as lost (tested, including a completion that started before a takeover). Concurrent
  completions of one lane do not conflict.
- **Attempts.** Charged when an attempt ends (`reschedule`, `bury`) or when a lane is taken over (a crash). If the process dies between the epoch
  bump and the charge, the ids stay in `lost` and the next holder charges them first; a crash after the charge and before `lost` is cleared charges
  one extra attempt (a documented limitation).
- **Restarted workers.** Lanes record the store instance (`holder`) that owns them, so a restarted process that reuses its `leaseOwner` string does
  not adopt its dead predecessor's live lanes; it takes them over after the lease expires.
- **Failure handling.** A failure after the batch was recorded gives it back to the lane; parking of blocked followers is best effort (logged through
  the `logger` option); after recording, each keyed pick is re-checked against requeued predecessors and given back if it was overtaken.
- **Events** carry `handlers` (a completed handler never runs again for a redelivered event) and `createdAt` (orphan event sweep).
- **Upgrade.** `setup()` converts a 0.1.0 database in place, repeatably: `processing` deliveries become `pending` with their attempts kept, lease
  fields are removed, `laneId` and the events' `handlers` are backfilled in batches, obsolete indexes are dropped.

The measured effect on oplog entries and bytes per delivery is in the MongoDB tables above. Keyed deliveries still pay per-key reads (an
unfinished-predecessor check per head); the store parks long backlogs of one key out of the claim index so a deep queue behind one slow delivery
costs nothing per claim.

## Upgrading from 0.1.0

MongoDB is upgraded by `setup()` as well (see above). PostgreSQL:

`setup()` (run by `bus.start()` unless `setupStore: false`) upgrades a 0.1.0 database in place, inside the same advisory-lock
transaction as a fresh install, and can be run any number of times. PostgreSQL, in order:

1. The upgrade runs only when the `deliveries.lease_token` column still exists (the marker of a 0.1.0 table). It adds
   `events.handlers` and fills it from the existing deliveries (completed ones included, so nothing completed under
   0.1.0 runs again), adds `deliveries.lane` and fills it with the same hash new deliveries use, turns `processing`
   deliveries into `pending` (their attempt stays counted, exactly as a lost lease would count it), and drops
   `lease_owner`, `lease_token` and `lease_expires_at`.
2. The 0.1.0 indexes that no longer exist are dropped (`DROP INDEX IF EXISTS`), the new ones are created
   (`CREATE INDEX IF NOT EXISTS`), the 256 lane rows are seeded (`ON CONFLICT DO NOTHING`), and the storage parameters are set.
3. Deliveries that were `completed` stay as `completed` rows and keep being listed, counted and purged by
   `retention.completed` until they are gone. New completions are deleted.

What to plan for:

- **Stop every 0.1.0 worker first.** They would fail on the dropped columns. Running two versions side by side is not supported.
- **One-off rewrite.** Step 1 updates every delivery row once. Purge old completed deliveries before upgrading a large table
  (`bus.purgeExpired()` with a short `retention.completed`), so there is less to rewrite.
- **Your own migration tool.** `createPostgresSchemaSql()` renders the same statements as one script. Apply it in one
  transaction with the workers stopped, then start the new version with `setupStore: false`.
- **Rollback.** There is no down migration: take a backup (or run the upgrade in a transaction you can roll back during
  rehearsal). Going back to 0.1.0 needs a restore.
- **Test coverage.** The upgrade is tested against the verbatim 0.1.0 DDL with pending, processing, completed and dead
  keyed and unkeyed rows, run twice (`packages/postgres/test/lane-leases.integration.test.ts`).

## Trade-offs and operations

What got worse, and what to do about it:

- **Completed deliveries are not listable by default.** `listDeliveries({ status: 'completed' })` and
  `statistics().completed` are empty. Turn on `completionHistory` (PostgreSQL, MongoDB) or `retainCompleted` (in-memory store) if you need an
  audit trail. It costs one extra insert per completion, so keep it off on hot paths.
- **Lane exclusivity.** While a worker holds a lane under a live lease, other workers do not see its pending deliveries, even if they are
  idle. A worker that crashes delays its lanes by the lease duration (`dispatcher.leaseDuration`, 30 s by default); a worker that stops
  gracefully gives them back at once. Lanes are taken per claim and handed back when they run nothing, so a backlog spreads over workers.
  A few hot ordering keys live in few lanes and therefore run on few workers, which is what ordering means anyway.
- **Deleting still leaves dead tuples.** A delete leaves one dead tuple per delivery, not three, and no dead index entries from updates;
  autovacuum still has to reclaim them. The tables are created with `autovacuum_vacuum_scale_factor = 0.02`. For sustained thousands of
  deliveries per second, lower `autovacuum_vacuum_cost_delay` on the table or schedule a `VACUUM`, and alert on `n_dead_tup`.
- **Retry and dead-letter updates are not HOT.** `reschedule` and `bury` update `available_at` and `status`, which are indexed. They are the
  rare path by design. `requeue` is an admin action.
- **Admin listings are scans.** `listDeliveries` has no dedicated index any more (it used to have two). It orders by `created_at`, so on a large
  table filter by `status` or `handler`, or add the index you need.
- **Failure accounting moved.** An attempt is charged when it ends or when a lane is taken over, not at claim. A worker that dies is charged
  at takeover, after the lease expired; between the crash and the takeover `listDeliveries` shows the deliveries as `processing`.
- **Events outlive their deliveries.** Events (and their `handlers` list) are deleted by retention, not at completion; keep
  `retention.completed` longer than your longest broker redelivery window. After a purge a redelivery is a new event.
- **A claim is more than one statement in the shallow-queue case.** When the held lanes do not fill the batch, the claim continues in a
  short transaction to take free lanes. With a deep backlog it is one statement.
- **Fixed lane count.** 256 lanes are enough for any realistic number of workers; the count is stored in each delivery, so it is not a setting.
- **Schema change.** Mixed 0.1.0 and new workers are not supported; upgrade is a stop-the-world migration (see above).

Not part of Phase 1 (neither implemented nor measured here), candidates for a next phase: wake-up of idle dispatchers across processes with `LISTEN/NOTIFY` (a latency hint only,
polling stays the correctness path), a single-statement publish that avoids the per-delivery foreign-key row lock on `events`, and a
`UNLOGGED` or partitioned delivery table.

## How to reproduce

```sh
pnpm install --frozen-lockfile
pnpm -r run build                           # the harness imports packages/*/dist
node benchmarks/run.ts --out after          # both stores, every scenario (about 1.5 hours)
node benchmarks/run.ts --store pg --scenarios 2,5 --out after-pg
node benchmarks/pageinspect.ts --repo . --url postgres://postgres:secret@localhost:5432/postgres
```

`benchmarks/README.md` describes the harness (every publisher and dispatcher is its own Node process, databases run in
Testcontainers, retention is disabled so no purge interferes). The numbers above come from a MacBook (Apple M4, 10 cores,
16 GB) running Docker Desktop, with the Node workers and the databases sharing the same cores: read the ratios, not the
absolute values. The baseline is `benchmarks/results/main` (commit `3a953d1`, version 0.1.0).
