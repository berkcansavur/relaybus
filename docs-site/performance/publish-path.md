---
title: The relaybus publish path and prepared statements
description: Why single-event publishes and default-settings dispatch were slower on the 0.2.0 branch, the counters that proved it, and the prepared-statement fix, with per-event round trips, WAL bytes and plan times against 0.1.0.
---

# Publish path and dispatch statements: per-event cost, 0.1.0 against 0.2.0

> Part of [Performance: 0.2.0 vs 0.1.0](/performance). Companion: [write amplification](/performance/write-amplification).

Three benchmark cells were slower on the 0.2.0 release branch than on 0.1.0 (`main`):
`pg-pub-p1-b1` (−37 %), `pg-pub-p32-b1` (−8 %) and `pg-disp-1i-defaults` (−6 %). This page records why,
measured with counters instead of throughput, and what changed.

## Method

A PostgreSQL 17 container with the benchmark's settings ([`benchmarks/lib/pg-backend.ts`](https://github.com/berkcansavur/relaybus/blob/main/benchmarks/lib/pg-backend.ts): `synchronous_commit=on`,
full-page writes, `pg_stat_statements` with `track_planning`), plus `pg_walinspect`. The probe publishes like the
benchmark worker (`bus.transaction` + `bus.publish`, one handler, the benchmark's 160-byte payload) and counts:

- round trips, by wrapping `client.query` of the pool;
- statements parsed and planned by the server (`pg_stat_statements.plans`, named or unnamed protocol messages);
- WAL bytes and records per relation (`pg_get_wal_block_info`), commits and WAL flushes (`pg_stat_wal`,
  `pg_stat_database`);
- client-side time of each statement.

To separate the database from relaybus' JavaScript, the captured statements were also replayed on a bare `pg`
client, once unnamed (parsed on every call) and once as named prepared statements. Timings are medians of two
runs of 10,000 publishes on an M4 laptop with Docker Desktop; the counts do not depend on the machine.

## Publish, one event per transaction (`pg-pub-p1-b1`)

| per publish                            | 0.1.0                        | 0.2.0 before           | 0.2.0 now              |
| -------------------------------------- | ---------------------------- | ---------------------- | ---------------------- |
| round trips in the transaction         | 4 (BEGIN, 2 inserts, COMMIT) | 3 (BEGIN, 1, COMMIT)   | 3                      |
| SQL text sent                          | 694 + 617 bytes              | 2,726 bytes            | none after the first   |
| statements parsed and planned          | 2                            | 1 (large)              | 0                      |
| server time, plan + execute            | 0.035 + 0.105 ms             | 0.084 + 0.060 ms       | 0 + 0.066 ms           |
| client time BEGIN / statement / COMMIT | 0.11 / 0.18 + 0.24 / 0.21 ms | 0.19 / 0.57 / 0.31 ms  | 0.11 / 0.26 / 0.21 ms  |
| total per transaction                  | 0.75 ms (≈1,330 /s)          | 1.15 ms (≈860 /s)      | 0.59 ms (≈1,700 /s)    |
| WAL bytes per event                    | 1,740                        | 1,475                  | 1,475                  |
| rows written                           | events 1, deliveries 1       | events 1, deliveries 1 | events 1, deliveries 1 |
| extra commit (trailing NOTIFY)         | none                         | 0.07 to 0.15           | 0.04 to 0.07           |

**Cause.** 0.2.0 publishes with one statement (`insertFreshEvents`: reserve key sequences, insert events,
insert deliveries, notify) instead of two small inserts. The statement is 2.7 kB of SQL with five CTEs over four
relations, and `pg` sent it unnamed, so the server parsed, analysed, rewrote and planned it on every publish.
`pg_stat_statements` shows only 0.14 ms of planning and execution; the rest of the 0.57 ms is parse analysis of
the text, which `pg_stat_statements` does not time. Replaying the exact statements on a bare client:

| replay of one publish transaction  | unnamed | prepared |
| ---------------------------------- | ------- | -------- |
| 0.1.0 statements                   | 0.75 ms | 0.61 ms  |
| 0.2.0 statement                    | 1.07 ms | 0.59 ms  |
| 0.2.0 without the key-sequence CTE | 0.99 ms | -        |

The BEGIN and COMMIT around the unnamed statement also took 0.08 to 0.10 ms longer; they return to 0.1.0's times
once the statement is prepared (measured, the mechanism was not isolated). Ruled out:

- **NOTIFY**: a build that never notifies published at 1.19 to 1.22 ms, the same as with notifications.
- **WAL and rows**: 0.2.0 writes 15 % less WAL per event, because deliveries have 5 indexes instead of 9.
- **Key sequences**: an unkeyed publish locks and writes no `key_sequences` row; dropping that CTE from the
  text saved only 0.08 ms.

**Fix.** The store marks every statement whose text never changes, and `PgExecutor` sends it as a named prepared
statement (name: `relaybus_` + 24 hex digits of its SHA-256). Each connection parses it once; later calls only
bind parameters. After five executions PostgreSQL may switch to a generic plan; a design test forces generic
plans for claim, takeover claim, completion, renewal and both publishes over 50,000 deliveries and checks that
they use no sequential scan and stay far below the JIT threshold (highest estimate 10,608, threshold 100,000).

### Where the WAL of one published event goes

| relation, record                               | 0.1.0 bytes | 0.2.0 bytes |
| ---------------------------------------------- | ----------- | ----------- |
| events heap insert                             | 449         | 473         |
| events: pkey, created index                    | 198         | 198         |
| events: row lock of the foreign key check      | 54          | 54          |
| deliveries heap insert                         | 179         | 179         |
| deliveries: indexes                            | 7 = 717     | 3 = 301     |
| speculative insert confirmations (ON CONFLICT) | 96          | 96          |
| commit record                                  | 34          | 34          |

The events row is 24 bytes larger because an event remembers its handlers (`handlers text[]`). No lane, key
sequence or completion row is written when publishing.

### Wake-up notifications

A publishing transaction notifies only when it inserted a delivery due now, and a store sends at most one
`NOTIFY` per 10 ms from its publishing statements; publishes in between leave one trailing `SELECT pg_notify`
that the store sends after their commit, on a pool connection, outside the publishing transaction. It is not a
round trip of the publish, but it is a transaction of its own with a commit record, at most 100 per second per
store. Notifying from every publishing transaction instead would cost far more: PostgreSQL holds a database-wide
lock from the commit of a notifying transaction until it is flushed, and with 32 publishers that measured
3,600 to 3,800 events/s against 6,500 coalesced. `wakeUp: 'poll'` removes notifications altogether.

## Publish with 32 connections (`pg-pub-p32-b1`)

| 32 publishers, one event per transaction | 0.1.0  | 0.2.0 before | 0.2.0 now |
| ---------------------------------------- | ------ | ------------ | --------- |
| events per second                        | ≈5,150 | ≈4,760       | ≈6,500    |
| WAL bytes per event                      | 1,744  | 1,472        | 1,472     |

Same cause: with 32 connections the parse and planning of the 2.7 kB text on every publish keeps the server's
CPUs busy. Coalesced notifications cost nothing measurable here (6,580 events/s without any notification against
6,470 with them).

## Dispatch, one process with default settings (`pg-disp-1i-defaults`)

Concurrency 10, batch 10, 40,000 unkeyed deliveries after `VACUUM ANALYZE` and `CHECKPOINT`, 8 s.

| per batch of 10 deliveries   | 0.1.0                 | 0.2.0 before                           | 0.2.0 now            |
| ---------------------------- | --------------------- | -------------------------------------- | -------------------- |
| claim statements             | 2 (of about 5)        | 1                                      | 1                    |
| claim SQL text               | about 2 kB each       | 11.6 kB (21 kB when taking free lanes) | none after the first |
| claim, client time           | 1.40 ms each          | 2.86 ms                                | 1.34 to 1.47 ms      |
| claim, server plan + execute | 0.77 ms               | 1.19 ms                                | 0.41 ms (generic)    |
| completion statements        | 10 (one per delivery) | 1                                      | 1                    |
| completion, client time      | 0.83 ms each          | 1.05 ms                                | 0.93 to 0.97 ms      |
| commits per delivery         | 1.13                  | 0.20                                   | 0.20                 |
| deliveries per second        | 2,800 to 3,400        | 2,200 to 2,370                         | 3,390 to 3,620       |

Same cause again: the claim statement is long, and its parse and planning took more time than its execution.
Prepared, a claim costs less than 0.1.0's two claims per ten deliveries.

**WAL after a checkpoint.** The benchmark gate also flags `walBytesPerUnit` of this cell. Per delivery, 0.2.0
writes fewer records than 0.1.0 (about 8 against 17), but the completion inserts an
`event_completions` row whose foreign key check locks the event row. Locking dirties the events page, and the
first change of a page after a checkpoint writes the whole page to WAL: 0.11 to 0.17 full-page images per delivery
(about 1 kB), against 0.05 for 0.1.0, whose deliveries were updated in place on fewer pages. It fades as pages are
touched again within a checkpoint cycle and does not affect throughput in these measurements; it was left as is.

## Guards

[`perf-budgets.json`](https://github.com/berkcansavur/relaybus/blob/main/perf-budgets.json) holds the measured values; a budget may only go down.

| budget                                               | value  | 0.1.0  |
| ---------------------------------------------------- | ------ | ------ |
| `postgres.publish.unkeyedSingle.statements`          | 3      | 4      |
| `postgres.publish.unkeyedSingle.parsedStatements`    | 0      | 2      |
| `postgres.publish.keyedSingle.parsedStatements`      | 0      | 2 to 3 |
| `postgres.dispatch.parsedStatementsPerBatch`         | 0      | 2+     |
| `postgres.wal.publishSingle.bytesPerEvent`           | 1,144  | -      |
| `postgres.explain.generic.largeTableSequentialScans` | 0      | -      |
| `postgres.explain.generic.maximumEstimatedCost`      | 25,000 | -      |

The WAL test publishes 200 events of the test suite's envelope, one per transaction and more than 10 ms apart, on a
server without full-page writes; with the benchmark's event 0.1.0 writes 1,740 bytes and 0.2.0 1,475.

## Not changed

- **Adapters** (Prisma, TypeORM, Drizzle, Knex, Kysely) send every statement unprepared: their APIs do not name
  statements. Their publishes and claims still pay the parsing; see the [tuning guide](/tuning) for running dispatchers on
  a `pg` pool.
- **PgBouncer before 1.21 in transaction mode** does not keep prepared statements. `preparedStatements: false`
  avoids them; left on, the first statement that lands on another server session fails, after which the executor
  stops preparing and logs a warning.
