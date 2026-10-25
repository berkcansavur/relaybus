---
title: relaybus releases and versioning
description: What is new in relaybus 0.3.0 (code-quality overhaul, per-key order across event types, codecs, NestJS transaction decorator, bounded shutdown, ESLint plugin, OpenTelemetry), its breaking changes, deprecations and known issues, and how relaybus is versioned and released.
---

# Releases

- [0.3.1](#_0-3-1-2026-10-25): internal readability refactor, same behaviour
- [0.3.0](#_0-3-0-2026-10-25): code quality, per-key order across event types, codecs, transactions in NestJS, bounded shutdown, ESLint rules, OpenTelemetry
- [0.2.0](#_0-2-0-2026-10-24): high-throughput delivery, event catalogs, decoding of foreign messages
- [0.1.x](#_0-1-x): first public release
- [How versioning works](#how-versioning-works)

## 0.3.1 (2026-10-25)

A readability release: no behaviour, API, SQL or wire-format change. Nothing to do when upgrading from 0.3.0.

- Every source file is now at most 300 code lines and every function at most 40 (enforced by lint). The largest files shrank from 794 to 294 lines (`relay-bus.ts`) and from 772 to 173 lines (`dispatcher.ts`).
- Shared skeletons moved into abstract base classes: one lease settlement flow for the PostgreSQL, MongoDB and in-memory stores, one claim flow for PostgreSQL and MongoDB, and one subscription lifecycle for Kafka and RabbitMQ. Value objects replace repeated primitives with rules (durations, backoff, lease identity, composite keys, bounded integers).
- Proof of no behaviour change: the public API snapshot, the SQL snapshot of every statement and the performance budgets are unchanged; 2,798 tests with 100% coverage.
- Additive type change: `KafkaTransport` and `RabbitMqTransport` now extend the internal `SubscribingTransport` base class.
- Benchmark against 0.3.0 (quick gate, 3 interleaved pairs per cell): publish 0.99–1.02x, dispatch 0.98–1.04x, every interval spans 1.0, so speed is unchanged. The 4-instance PostgreSQL dispatch cell was not valid on either side (its template drains before the measurement window ends); that is a harness sizing issue, not a regression.

## 0.3.0 (2026-10-25)

Follow [Upgrading from 0.2 to 0.3](/upgrading). Nothing has to be migrated in the database; read [Breaking changes](#breaking-changes) first. What stays stable between releases is described in [Stability](/stability), what is tested in [Compatibility](/compatibility).

### What is new

| Area                      | Change                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Code quality              | The code now follows a strict set of limits: strict `tsconfig` flags (`noUnusedLocals`, `noImplicitReturns`, `noPropertyAccessFromIndexSignature` and more), lint rules for function size (60 lines), complexity (10), depth (3), at most two positional parameters, one class per file and a `[ClassName](methodName)` prefix on every log line. The two largest files, the PostgreSQL and MongoDB stores, are split into modules per concern with byte-identical SQL and unchanged filters. 2,773 tests, 100 % coverage unchanged. |
| Errors                    | Every error class carries a stable `code` (`RELAYBUS_LEASE_LOST`, ...), reported on dead letters, retries and observer events and listed on [Error codes](/concepts/error-codes). Remaining bare throws became coded classes. Kafka and RabbitMQ dead letters carry the error name and code.                                                                                                                                                                                                                                         |
| API surface               | A snapshot test pins every export of every package, so a change of the public API is always deliberate.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Ordering                  | Per-key order now holds across event types routed to one destination: all routes to one destination are one handler. [Ordering keys](/concepts/ordering-keys).                                                                                                                                                                                                                                                                                                                                                                       |
| Codecs                    | Kafka and RabbitMQ take a symmetric `codec` (`cloudEventsBinary`, `cloudEventsStructured`, `jsonEnvelope`, or your own `{ encode, decode }`) for the wire format both ways. The default stays byte for byte the same. [Guide](/recipes/interoperating-with-other-services).                                                                                                                                                                                                                                                          |
| Typed ordering key        | `defineEvent(type, { schema, key: (payload) => payload.orderId })` derives the key from the payload; a wrong field fails to compile.                                                                                                                                                                                                                                                                                                                                                                                                 |
| Transactions in NestJS    | `@RelayBusTransaction()` opens a transaction around a method, `RelayBusTransactionHost` hands it to repositories, and every transaction is held to `transactionTimeout` (default `'5s'`). [Guide](/recipes/transactions-in-nestjs).                                                                                                                                                                                                                                                                                                  |
| Decorators                | `@RelayBusConsumer(Event)` is the method decorator (default style) and `@RelayBusCatalogConsumer(Catalog)` the optional class decorator. `RelayBusModule.forFeature({ consumes, produces })` fails the boot when a module lacks a consumer or a route. [Event catalogs](/concepts/event-catalogs).                                                                                                                                                                                                                                   |
| Bounded shutdown          | `bus.stop({ timeout })` covers the whole shutdown, force-closes what relaybus opened itself at the deadline and resolves with a `StopResult { graceful, abandonedDeliveries, durationMs }`. NestJS `app.close()` is bounded the same way.                                                                                                                                                                                                                                                                                            |
| `@relaybus/eslint-plugin` | New package. `no-external-call-in-transaction` and `publish-requires-transaction`, both `error` in the recommended config.                                                                                                                                                                                                                                                                                                                                                                                                           |
| `@relaybus/otel`          | New package. OpenTelemetry metrics (publish, claim, handler, end-to-end latency, retries, dead letters, backlog gauges, a counter of undecodable inbound messages) and traces across services. [Observability](/concepts/observability).                                                                                                                                                                                                                                                                                             |
| Observer hooks            | Timed hooks (`onPublishFinished`, `onClaimFinished`, `onHandlerStarted`, `onHandlerFinished`, `onTransportSent`, `onReceiveFinished`, `onInvalidMessage`, `onBacklog`), skipped entirely when no observer implements them.                                                                                                                                                                                                                                                                                                           |
| Testing                   | Every timer of the packages runs through the `Clock`, so `FakeClock` controls them all.                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Benchmarks                | `bench:full` gains two routed cells (`distinct:1000` and `hot:10` keyed events sent to an in-process fake transport that acks after 5 ms).                                                                                                                                                                                                                                                                                                                                                                                           |

### Breaking changes

| What                      | 0.2.0                                                            | 0.3.0                                                                                                                                             | Action                                                                                                                                                                            |
| ------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Route handler ids         | `relaybus.transport:<transport>:<destination>:<eventType>`       | `relaybus.transport:<transport>:<destination>`                                                                                                    | Update dashboards and code that match route ids (`handlers()`, `listDeliveries({ handler })`, `HandlerContext.handler`, observer events). Stored 0.2.0 deliveries are still sent. |
| Routes to one destination | Different `retry` or `timeout` were allowed                      | They must be equal, and an event type may be routed to a destination once; otherwise `new RelayBus` throws `ConfigurationError`                   | Give such routes the same settings.                                                                                                                                               |
| Transaction time budget   | No limit                                                         | `transactionTimeout`, default `'5s'`: a longer `bus.transaction` or `@RelayBusTransaction()` method is rolled back with `TransactionTimeoutError` | Move slow calls out of the transaction, or raise the option.                                                                                                                      |
| `bus.stop()`              | Returns `Promise<void>`, waits without a deadline                | Returns `Promise<StopResult>`, bounded by `timeout` (default `dispatcher.shutdownTimeout`, 30 s)                                                  | Only code that types the result as `void` needs a change.                                                                                                                         |
| Error names               | Five Kafka and RabbitMQ errors reported the name `RelayBusError` | They report their own class name                                                                                                                  | Code that matched `error.name === 'RelayBusError'` on dead letters must use the class name or `code`.                                                                             |
| Failure stage             | `onFailure` stages without `'backlog'`                           | New stage `'backlog'`                                                                                                                             | An exhaustive `switch` over the stage needs a new case.                                                                                                                           |

### Deprecations

Removed in 1.0.0; they still work and behave the same.

- `@Subscribe` is the earlier name of `@RelayBusConsumer`; `@EventConsumer` of `@RelayBusCatalogConsumer`. Their option and metadata types are deprecated too.
- The inbound-only `decode` option of `KafkaTransport` and `RabbitMqTransport`. Use `codec`; giving both is a configuration error.

### Benchmark: 0.2.0 vs 0.3.0

In short: 0.3.0 is as fast as 0.2.0 on publish and on MongoDB, and about 1.8 times faster for keyed PostgreSQL dispatch in two cells. No cell showed a lost, duplicated or out-of-order delivery. Most cells have one sample per side, so a difference below about 10 % is noise. The full tables and caveats are in [Performance: 0.3.0 vs 0.2.0](/performance#_0-3-0-vs-0-2-0).

Method: `pnpm bench:full --base v0.2.0`, interleaved on one machine (Apple M4, Docker Desktop, PostgreSQL 17, MongoDB 7, Node 24.3.0), fixed windows, database counters and exactness validation, as described in the [benchmark README](https://github.com/berkcansavur/relaybus/blob/main/benchmarks/README.md). The full run (39 minutes, all cells) used commit `a8587ff`; every sample was valid except the two of the PostgreSQL fan-out cell on four instances, whose preloaded backlog was too small on both sides, so that cell is left out. The publish and mixed-load cells were measured again after two publish fixes, with 2 samples per side and the 0.2.0 side taken from stored 0.2.0 samples.

| Scenario                                          | 0.2.0 per second | 0.3.0 per second |        Ratio |        Samples        |
| ------------------------------------------------- | ---------------: | ---------------: | -----------: | :-------------------: |
| PostgreSQL publish, 1 publisher, 1 event per call |            1,543 |            1,573 |         1.02 |         2 / 2         |
| PostgreSQL publish, 32 publishers, 1 event        |            6,235 |            6,111 |         0.98 |         2 / 2         |
| PostgreSQL publish, 8 publishers, 1,000 keys      |            3,789 |            4,145 |         1.09 |         2 / 2         |
| PostgreSQL mixed load, 5,000 per second, tuned    |            4,133 |            4,032 |         0.98 |         2 / 2         |
| PostgreSQL routed keyed dispatch, 1,000 keys      |            3,267 |            5,827 |         1.78 |         1 / 1         |
| PostgreSQL keyed dispatch, 1,000 keys, 4 workers  |            4,453 |            7,931 |         1.78 |         1 / 1         |
| PostgreSQL and MongoDB publish, all other cells   |                - |                - | 0.94 to 1.09 |         2 / 2         |
| PostgreSQL dispatch, all other cells              |                - |                - | 0.88 to 1.19 | 1 / 1, one cell 3 / 3 |
| MongoDB dispatch, all cells                       |                - |                - | 0.98 to 1.06 | 1 / 1, one cell 3 / 3 |

Statements per publish are 4 in both versions (4.07 against 4.07 at one publisher), so the transaction budget costs no extra round trip. Mixed-load p99 ratios are not meaningful at 5,000 per second: both sides are saturated and p99 moved between 79 ms and 10.6 s from run to run.

Two things to know. First, PostgreSQL WAL bytes per delivery were 8 to 11 % higher in four dispatch cells and 5 to 8 % lower in five others (one sample each, except one cell with three), so there is no clear direction yet. Second, the first full run showed 25 % more statements per publish (5 against 4). That was the transaction budget and was fixed before this table was taken, in `a13fb7e` and `2fb3cde`. The publish cells that were not measured again (8 publishers with batches, five handlers, hot keys) were 0.79 to 1.02 in the full run, before the fix.

### Known issues

- **p99 latency near saturation.** Carried over from 0.2.0 and not changed by this release: at about 5,000 events per second on a laptop, end-to-end p99 on PostgreSQL was higher than 0.1.0's. Re-measure with `bench:full` for your hardware.
- **Order across the upgrade.** Deliveries stored under 0.2.0 route ids are a separate queue from new ones. See [Upgrading](/upgrading#order-during-the-upgrade).
- **RabbitMQ and the shutdown deadline.** A connection opened with `url` is cut at the deadline, even during the TCP or AMQP handshake. A `connect` function you supply gets `connect({ signal })` and must honour it; a socket it keeps opening is not stopped by relaybus. Kafka has no forced disconnect, so past the deadline the bus stops waiting for it.
- **`TimeoutNegativeWarning` on Node 23 and newer.** `kafkajs` can print this warning; it comes from kafkajs, not from relaybus.
- **ORM adapters do not use prepared statements.** Unchanged from 0.2.0; run dispatchers on a `pg` pool ([Tuning](/tuning)).

## 0.2.0 (2026-10-24)

**Breaking.** Completed deliveries are now deleted when they complete instead of being kept. To keep them for `listDeliveries`, pass `completionHistory: true`. The 0.1.0 PostgreSQL and MongoDB schemas are upgraded in place by `setup()`. Follow [Upgrading from 0.1 to 0.2](/upgrading); stop every 0.1.0 worker first.

### What is new

| Area                  | Change                                                                                                                                                                                                                                                                      |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Delivery path         | Workers lease 256 _lanes_ instead of rows, claim and complete in batches, and delete a delivery when it completes. The delivery row is written once and deleted once. [Why](/performance/write-amplification)                                                               |
| Throughput            | On one laptop against 0.1.0: PostgreSQL publish 1.1 to 1.9x, PostgreSQL dispatch up to 18x, MongoDB dispatch 2 to 26x, about half the WAL per delivery. [Numbers and method](/performance)                                                                                  |
| Deadlock-free locking | One documented lock order for every statement, and permanent stress tests: 10 dispatchers on PostgreSQL and on MongoDB, concurrent publishers, takeovers and keyed ordering.                                                                                                |
| Wake-ups              | PostgreSQL `LISTEN`/`NOTIFY` (`wakeUp: 'notify'`) starts new work on other processes within milliseconds; at most one `NOTIFY` per 10 ms per store. `wakeUp: 'poll'` turns it off.                                                                                          |
| Prepared statements   | The statements relaybus repeats are parsed once per connection (`preparedStatements`, on by default for `pg` pools). [Why](/performance/publish-path)                                                                                                                       |
| Event catalogs        | `defineCatalog`, `HandlersOf`, `defineHandlers`, `bus.forCatalog`, `bus.subscribeCatalog` and a boot-time coverage check. `consume(...)` and `produce(...)` roles let services share one catalog; `@EventConsumer` in `@relaybus/nestjs`. [Guide](/concepts/event-catalogs) |
| Foreign messages      | Kafka and RabbitMQ `decode` option with `plainJsonDecoder` and `decodeCloudEvent` (structured mode). Default decoding is unchanged. [Guide](/recipes/interoperating-with-other-services)                                                                                    |
| MongoDB               | Lane documents, bulk completion, smaller new documents.                                                                                                                                                                                                                     |
| Tuning                | New options `wakeUp`, `preparedStatements` and `completionHistory`. [Tuning guide](/tuning)                                                                                                                                                                                 |
| Tooling               | Shared test containers (suite about 6 to 3.5 minutes), an interleaved benchmark harness (`pnpm bench:quick`, `pnpm bench:full`), performance budgets that can only go down.                                                                                                 |

Verification: 2,292 tests with 100 % statement, branch, function and line coverage; typecheck, lint, build and publint green; 0 lost, 0 duplicate and 0 out-of-order deliveries in the full benchmark.

### Known issues

- **p99 latency near saturation.** At 5,000 events per second on a laptop, end-to-end p99 on PostgreSQL is higher than 0.1.0's (524 ms against 114 ms in the single-sample run), although 0.2.0 accepts more load there. Below saturation (1,000 per second) p99 is far lower. To be addressed in a later release.
- **Ordering scope.** Order is guaranteed between deliveries of the same handler and the same ordering key. Unkeyed events, and deliveries to different handlers, have no relative order.
- **`bus.stop()` has no deadline yet.** It waits for running handlers to finish; a handler that never returns holds shutdown until the process manager kills it. Give handlers their own timeouts (`timeout` on `@Subscribe`).
- **ORM adapters do not use prepared statements.** Prisma, TypeORM, Drizzle, Knex and Kysely send every statement unprepared, so each publish and claim is parsed again (about 0.3 ms per publish and 1.3 ms per claim of 10 deliveries). Run dispatchers on a `pg` pool; see [Tuning](/tuning).

## 0.1.x

First public release: the transactional outbox, per-handler deliveries, retries, dead letters and ordering keys for PostgreSQL and MongoDB, the Kafka and RabbitMQ transports and the NestJS module. Upgrade with [Upgrading from 0.1 to 0.2](/upgrading).

## How versioning works

Every push to `main` with releasable [Conventional Commits](https://www.conventionalcommits.org/) publishes all seven `@relaybus/*` packages to npm with **one shared version**, computed by semantic-release. There is no committed changelog: the [GitHub Releases](https://github.com/berkcansavur/relaybus/releases) page is the changelog.

- Current release line: **0.2.x**. While the version is below 1.0.0, a breaking change bumps the minor version.
- `feat` commits bump the minor version; `fix`, `perf`, `refactor` and `revert` bump the patch version (a refactor changes the published code, never its behaviour). `docs`, `chore`, `ci`, `test`, `build` and `style` do not release.
- Prereleases are published from the `next` and `beta` branches under the matching npm dist-tags.
- Releases are published by CI with npm Trusted Publishing (OIDC) and provenance, so a tarball links back to the workflow run that built it.

## Install a specific version

```sh
pnpm add @relaybus/core@0.3.0 @relaybus/nestjs@0.3.0 @relaybus/postgres@0.3.0
```

Keep all `@relaybus/*` packages on the same version.

## Links

- [Upgrading from 0.1 to 0.2](/upgrading)
- [Performance: 0.2.0 vs 0.1.0](/performance)
- [GitHub Releases](https://github.com/berkcansavur/relaybus/releases)
- [npm: @relaybus/core](https://www.npmjs.com/package/@relaybus/core)
- [Security policy](https://github.com/berkcansavur/relaybus/blob/main/SECURITY.md)
- [Contributing](https://github.com/berkcansavur/relaybus/blob/main/CONTRIBUTING.md)
