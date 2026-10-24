---
title: relaybus releases and versioning
description: What is new in relaybus 0.2.0 (high-throughput delivery, event catalogs, foreign message decoding), its breaking change and known issues, and how relaybus is versioned and released.
---

# Releases

- [0.2.0](#_0-2-0-2026-10-24): high-throughput delivery, event catalogs, decoding of foreign messages
- [0.1.x](#_0-1-x): first public release
- [How versioning works](#how-versioning-works)

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
- `feat` commits bump the minor version; `fix`, `perf` and `revert` bump the patch version. `docs`, `chore`, `ci`, `test`, `build`, `refactor` and `style` do not release.
- Prereleases are published from the `next` and `beta` branches under the matching npm dist-tags.
- Releases are published by CI with npm Trusted Publishing (OIDC) and provenance, so a tarball links back to the workflow run that built it.

## Install a specific version

```sh
pnpm add @relaybus/core@0.2.0 @relaybus/nestjs@0.2.0 @relaybus/postgres@0.2.0
```

Keep all `@relaybus/*` packages on the same version.

## Links

- [Upgrading from 0.1 to 0.2](/upgrading)
- [Performance: 0.2.0 vs 0.1.0](/performance)
- [GitHub Releases](https://github.com/berkcansavur/relaybus/releases)
- [npm: @relaybus/core](https://www.npmjs.com/package/@relaybus/core)
- [Security policy](https://github.com/berkcansavur/relaybus/blob/main/SECURITY.md)
- [Contributing](https://github.com/berkcansavur/relaybus/blob/main/CONTRIBUTING.md)
