---
title: relaybus releases and versioning
description: What is new in relaybus 0.3.0 (code-quality overhaul, per-key order across event types, codecs, NestJS transaction decorator, bounded shutdown, ESLint plugin, OpenTelemetry), its breaking changes, deprecations and known issues, and how relaybus is versioned and released.
---

# Releases

- [0.3.0](#_0-3-0-unreleased): code quality, per-key order across event types, codecs, transactions in NestJS, bounded shutdown, ESLint rules, OpenTelemetry
- [How versioning works](#how-versioning-works)

## 0.3.0 (unreleased)

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

Method: `pnpm bench:full --base v0.2.0`, interleaved on one machine, fixed windows, database counters and exactness validation (0 lost, 0 duplicate, 0 out of order), as described in the [benchmark README](https://github.com/berkcansavur/relaybus/blob/main/benchmarks/README.md).

The full 0.2.0 against 0.3.0 matrix has not been run yet. This table holds what has been measured so far (Apple M4, Docker Desktop, PostgreSQL 17, Node 24.3.0, one sample per side, so ratios are indicative only) and is completed from the full run before the release is tagged.

| Cell                                                                 | Metric                  | 0.2.0            | 0.3.0            | Ratio | Samples |
| -------------------------------------------------------------------- | ----------------------- | ---------------- | ---------------- | ----- | ------- |
| Routed dispatch `hot:10`, 5 ms ack, 1 instance, tuned                | deliveries per second   | 731              | 745              | 1.02  | 1 / 1   |
| Routed dispatch `hot:10`, 5 ms ack, 1 instance, tuned                | WAL bytes per delivery  | 1,619            | 1,885            | 1.16  | 1 / 1   |
| Routed dispatch `hot:10`, 5 ms ack, 1 instance, tuned                | statements per delivery | 1.20             | 1.20             | 1.00  | 1 / 1   |
| Publish, PostgreSQL and MongoDB dispatch, keyed dispatch, mixed load |                         | not yet measured | not yet measured |       |         |

Both samples were valid. The WAL difference is within what one sample can show (its full-page-image share moved from 0.10 to 0.16 per delivery); it needs the repeated run before it is called a regression or noise.

### Known issues

- **p99 latency near saturation.** Carried over from 0.2.0 and not changed by this release: at about 5,000 events per second on a laptop, end-to-end p99 on PostgreSQL was higher than 0.1.0's. Re-measure with `bench:full` for your hardware.
- **Order across the upgrade.** Deliveries stored under 0.2.0 route ids are a separate queue from new ones. See [Upgrading](/upgrading#order-during-the-upgrade).
- **RabbitMQ and the shutdown deadline.** A connection opened with `url` is cut at the deadline, even during the TCP or AMQP handshake. A `connect` function you supply gets `connect({ signal })` and must honour it; a socket it keeps opening is not stopped by relaybus. Kafka has no forced disconnect, so past the deadline the bus stops waiting for it.
- **`TimeoutNegativeWarning` on Node 23 and newer.** `kafkajs` can print this warning; it comes from kafkajs, not from relaybus.
- **ORM adapters do not use prepared statements.** Unchanged from 0.2.0; run dispatchers on a `pg` pool ([Tuning](/tuning)).

## How versioning works

Every push to `main` with releasable [Conventional Commits](https://www.conventionalcommits.org/) publishes all nine `@relaybus/*` packages to npm with **one shared version**, computed by semantic-release. There is no committed changelog: the [GitHub Releases](https://github.com/berkcansavur/relaybus/releases) page is the changelog.

- Current release line: **0.3.x** once 0.3.0 is tagged (0.2.x until then). While the version is below 1.0.0, a breaking change bumps the minor version.
- `feat` commits bump the minor version; `fix`, `perf` and `revert` bump the patch version. `docs`, `chore`, `ci`, `test`, `build`, `refactor` and `style` do not release.
- Prereleases are published from the `next` and `beta` branches under the matching npm dist-tags.
- Releases are published by CI with npm Trusted Publishing (OIDC) and provenance, so a tarball links back to the workflow run that built it.

## Install a specific version

```sh
pnpm add @relaybus/core@0.3.0 @relaybus/nestjs@0.3.0 @relaybus/postgres@0.3.0
```

Keep all `@relaybus/*` packages on the same version.

## Links

- [GitHub Releases](https://github.com/berkcansavur/relaybus/releases)
- [npm: @relaybus/core](https://www.npmjs.com/package/@relaybus/core)
- [Security policy](https://github.com/berkcansavur/relaybus/blob/main/SECURITY.md)
- [Contributing](https://github.com/berkcansavur/relaybus/blob/main/CONTRIBUTING.md)
