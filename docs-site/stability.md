---
title: relaybus API stability policy
description: 'What relaybus versions promise: semver for 0.x and 1.x, what counts as public API, the deprecation policy, wire-format stability, database schema changes and the supported runtimes.'
---

# Stability

This page says what you can rely on when you upgrade relaybus, and what you cannot. [Compatibility](/compatibility) lists what is tested; [Releases](/releases) lists what changed.

## Versioning

All `@relaybus/*` packages share one version and are released together. Install them at the same version.

| Version         | A breaking change bumps    | A new feature bumps | A fix bumps |
| --------------- | -------------------------- | ------------------- | ----------- |
| `0.x`           | the minor (`0.2` to `0.3`) | the minor           | the patch   |
| `1.x` and later | the major                  | the minor           | the patch   |

Below 1.0.0 a minor release may contain breaking changes. Each one is marked `BREAKING CHANGE` in the commit, listed under "Breaking changes" in [Releases](/releases), and has a step in [Upgrading](/upgrading). From 1.0.0 on, only a major release breaks the public API, the wire format or a documented behaviour.

## What is public API

The public API is what the API snapshot test pins: the exported names of the entry point of every package (`@relaybus/core`, `nestjs`, `postgres`, `mongodb`, `kafka`, `rabbitmq`, `testing`, `eslint-plugin`, `otel`), each as a value, a type or both. The snapshot is committed, so a change to the exported surface shows up in review and is always deliberate. Alongside the names, the documented behaviour of those exports is public: option defaults, error classes and their `code`, the guarantees on [Guarantees](/guarantees).

Not covered by the policy:

- **`@relaybus/core/internal`.** A subpath shared by the packages (validators, decoder helpers, timers). It is pinned by a snapshot too, but only so the packages stay consistent; it is undocumented and can change in any release. Do not import it.
- **Anything not exported**, deep imports into `dist/`, and the contents of `src/`.
- **Log message bodies.** Lines start with `[ClassName](methodName)`, but wording can change. Branch on error `code`, not on log text or error messages.
- **Error messages.** Codes are stable; messages are not.
- **Tables, collections, indexes and statements.** You may read the tables for debugging; write only through relaybus.
- **Benchmark numbers** and the `benchmarks/` harness.

## Deprecation policy

An API is deprecated, with a warning in the types, for at least one minor release before it is removed, and only a major release removes it. Below 1.0.0 the removal waits for 1.0.0.

- A deprecated API keeps working and keeps its behaviour. It is marked `@deprecated` in the types, so editors strike it through, and its documentation names the replacement.
- Deprecations are listed in [Releases](/releases) with the version that removes them. Today: `@Subscribe`, `@EventConsumer` and the inbound-only `decode` option of the Kafka and RabbitMQ transports, all removed in 1.0.0.
- Error `code` values are never renamed or reused once released.

## Wire format

What relaybus writes to a broker is a compatibility surface of its own, because other services read it.

- **CloudEvents binary mode is the default** for Kafka and RabbitMQ, and the default codec writes byte for byte the same message as the release before it. Golden tests pin the bytes, so a refactoring cannot change them silently.
- Changing the default wire format is a breaking change: 1.x does it only in a major release, and 0.x says so as a breaking change.
- New wire formats are opt-in through `codec` (`cloudEventsStructured`, `jsonEnvelope`, or your own).
- New headers may be added (for example the error name and code on dead letters, or `traceparent` when you enable a tracer). Consumers must ignore headers they do not know.

## Database schema

- A new release may change the store schema. It does so in place: `setup()` upgrades an older schema inside one transaction under an advisory lock, and is safe to run again.
- **Applying the schema yourself.** If the application role has no DDL rights, render the script with `createPostgresSchemaSql({ schema, tablePrefix })` and run it with your migration tool, as one transaction with the workers stopped. The release notes say when the script changed.
- There is no down migration. Back up before upgrading and rehearse on a copy.
- 0.3.0 changed no table, column or index.
- Running two versions against one database is supported only where [Upgrading](/upgrading) says so (0.2 and 0.3 during a rolling deploy).

## Runtimes

| Runtime             | Policy                                                                                                                                                                                    |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Node.js             | Node.js 22.12 and newer (`engines`); CI runs the tests on Node 22 and 24. Dropping a Node version is announced in [Releases](/releases) and, from 1.0.0, happens only in a major release. |
| NestJS              | The majors in the peer dependency range (`^11                                                                                                                                             |     | ^12`), each tested in CI. |
| PostgreSQL, MongoDB | The versions in [Compatibility](/compatibility). Dropping a server version is announced in [Releases](/releases).                                                                         |
| Brokers and clients | As listed in [Compatibility](/compatibility).                                                                                                                                             |

Bun and Deno are not tested and not supported.

## Reporting a break

If a release broke something this page says it should not have, that is a bug: open an issue on [GitHub](https://github.com/berkcansavur/relaybus/issues). Security problems follow the [security policy](https://github.com/berkcansavur/relaybus/blob/main/SECURITY.md).
