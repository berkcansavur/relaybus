# Contributing to relaybus

Thanks for helping. This guide covers setup, the test strategy and the release flow.

## Setup

Requirements:

- Node.js 22.12 or newer (`.nvmrc` pins 22; CI also runs 24)
- pnpm (the exact version is pinned in `package.json` under `packageManager`; use Corepack: `corepack enable`)
- Docker, running and reachable by your user. Integration tests start PostgreSQL, MongoDB, Kafka and RabbitMQ with Testcontainers.

```bash
git clone https://github.com/berkcansavur/relaybus.git
cd relaybus
pnpm install --frozen-lockfile
pnpm build
pnpm test
```

## Scripts

| Script                  | What it does                                                       |
| ----------------------- | ------------------------------------------------------------------ |
| `pnpm build`            | Builds every package in `packages/*` with tsup (ESM + CJS + types) |
| `pnpm typecheck`        | Type checks sources, tests and examples with the root `tsconfig`   |
| `pnpm lint`             | ESLint with type-aware rules                                       |
| `pnpm format`           | Prettier, write mode                                               |
| `pnpm format:check`     | Prettier, check mode (used by CI)                                  |
| `pnpm test`             | All tests (unit + integration)                                     |
| `pnpm test:unit`        | Unit project only, no Docker needed                                |
| `pnpm test:integration` | Integration projects only, needs Docker                            |
| `pnpm test:affected`    | Only tests that import files changed since `HEAD` (`--changed`)    |
| `pnpm test:pull-images` | Pre-pulls the Docker images the integration tests use              |
| `pnpm coverage`         | All tests with V8 coverage; fails below the thresholds             |
| `pnpm mutation`         | Stryker mutation testing                                           |
| `pnpm release:dry-run`  | semantic-release dry run (needs a GitHub token, see docs)          |

## Test strategy

- Unit tests live in `packages/<name>/test/**/*.test.ts` and must not need Docker or the network. Use the fake clock and in-memory transport from `@relaybus/testing`.
- Integration tests are named `*.integration.test.ts` and run against real services through Testcontainers.
- **Contract suite for new stores.** Every `EventStore` implementation must pass the shared contract suite exported by `@relaybus/testing`. Adding a store means adding an integration test that calls the contract suite with your store factory, then adding store-specific tests for anything outside the contract (leasing details, transactions, indexes).
- Property-based tests use `fast-check` where invariants matter (ordering keys, retry backoff bounds).

### How the integration tests run

`vitest.global-setup.ts` starts one server per backend (two PostgreSQL versions, a MongoDB replica set, RabbitMQ, Kafka) in parallel before the integration tests and stops them afterwards. Each test file keeps apart through its own Postgres database, a unique Mongo database name, and unique RabbitMQ and Kafka names, so files run in parallel (project `integration`). Files that depend on lease timing or restart a server (`lane-leases`, `claim-concurrency`, `broker-restart`) run one at a time after the rest (project `integration-serial`). A test that needs its own server (the standalone mongod, the RabbitMQ restart test) starts a dedicated container.

- Test PostgreSQL runs with its data directory on tmpfs and `fsync=off`, `synchronous_commit=off`, `full_page_writes=off`. No test asserts durability or crash recovery; set `RELAYBUS_TEST_DURABLE_POSTGRES=1` to use the image defaults.
- A path filter such as `pnpm vitest run packages/postgres` starts only that package's backend. `RELAYBUS_TEST_BACKENDS=postgres,mongodb` (or `all`) chooses explicitly. A run without integration tests starts nothing.
- A new integration test must isolate itself with `uniqueSchema()`, `uniqueDatabaseName()` or `unique()` and must not assert on server-wide state. Add it to `serialIntegrationTests` in `vitest.config.ts` if it depends on wall-clock timing.
- `pnpm test:affected` is the quick local loop. A change in `packages/core` reaches every package, so expect a full run there.

## Coverage policy

Coverage is 100% for statements, branches, functions and lines, enforced in `vitest.config.ts` and in CI.

- Do not use `/* v8 ignore */`, `/* istanbul ignore */` or similar comments.
- Do not add files to the coverage `exclude` list to dodge the gate. The only exclusions are barrel `index.ts` files and `packages/core/src/standard-schema.ts` (type-only vendored spec).
- If code cannot be covered, it is usually dead or needs a different design. Delete it or make it testable.

## Mutation testing

Stryker checks that tests fail when the code changes (`pnpm mutation`, config in `stryker.config.json` and `vitest.stryker.config.ts`). It runs weekly in CI and on demand, against the Docker-free unit tests only. Surviving mutants in code you touched are worth killing with a sharper assertion. The HTML report is written to `reports/mutation/mutation.html`.

Stryker runs **in place** (`inPlace: true`): it rewrites the source files of the checkout it runs in and restores them afterwards. This is required so that tests importing `@relaybus/*` see the mutants. It also means you must **not run it in a dirty or busy working copy**: an interrupted run can leave mutated files behind, and file modes (such as the executable bit on scripts) are not preserved on restore. Run it in a clean checkout or a throwaway worktree:

```bash
git worktree add ../relaybus-mutation HEAD
cd ../relaybus-mutation
pnpm install --frozen-lockfile
pnpm mutation                                   # whole repo
pnpm mutation --mutate "packages/core/src/duration.ts"   # one file
cd - && git worktree remove --force ../relaybus-mutation
```

`@stryker-mutator/vitest-runner` 10.0.0 builds its per-test filter with space-separated test names, but Vitest 5 joins suite and test names with `>`, so no test would run for most mutants and they would all survive. `patches/@stryker-mutator__vitest-runner@10.0.0.patch` (applied by pnpm through `patchedDependencies`) makes the filter accept both separators. Drop the patch once a Stryker release supports Vitest 5.

Scope: `core`, `nestjs`, `kafka`, `rabbitmq` and the `testing` helpers (`fake-clock`, `in-memory-transport`). Three things are deliberately left out of `mutate`, because the unit project cannot measure them fairly:

- `packages/postgres/src` and `packages/mongodb/src`: the stores are covered by the Docker-based integration tests, so a unit-only run reports most of their mutants as uncovered (postgres about 25%, mongodb about 14%).
- `packages/testing/src/event-store-contract.ts`: it is a test suite, and most of its mutants are test titles and fixture data that no assertion can distinguish.

Thresholds (break, low, high) are in `stryker.config.json`. The break threshold sits a few points under the score the current tests achieve (about 90) and must never drop below 75; raise it when the score improves.

## Log lines

Every log line starts with `[ClassName](methodName) message`, naming the class and method the call is
written in (`#` dropped; outside a class, the module's main class and the enclosing function), followed
by `| key=value` fields. The `relaybus/log-prefix` lint rule (`eslint-rules/log-prefix.js`) enforces it,
so a log moved into another method must be renamed with it.

## Parameters and timers

A function takes at most two positional parameters (`relaybus/max-two-params`); further ones travel
in one typed input object. Only public port and API signatures (EventStore, Transport and executor
methods, published functions) keep more, each exempted on its line with the reason. Timers start
through `schedule()`/`sleep()` of `@relaybus/core/internal`, never with `setTimeout`/`setInterval`
directly, so a `Clock` can control them.

## Commit and PR conventions

Use [Conventional Commits](https://www.conventionalcommits.org/) for commit messages and PR titles:

```
feat(postgres): lease events with SKIP LOCKED
fix(core): clamp retry delay to maxDelay
docs: explain ordering keys
chore(deps): bump testcontainers
```

Common types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`. Mark breaking changes with `!` (`feat(core)!: ...`) and a `BREAKING CHANGE:` footer.

## Versioning and releases

There are no changesets. The version is computed from Conventional Commits on `main`, and PRs are squash-merged, so **the PR title is the commit that counts**. A workflow checks it, and a husky `commit-msg` hook runs commitlint on local commits (`pnpm install` enables the hook through the `prepare` script).

| Commit                                                      | Release (while below 1.0.0) |
| ----------------------------------------------------------- | --------------------------- |
| `feat`                                                      | minor                       |
| `fix`, `perf`, `revert`                                     | patch                       |
| `!` or a `BREAKING CHANGE:` footer                          | minor (major from 1.0.0)    |
| `docs`, `chore`, `test`, `ci`, `build`, `refactor`, `style` | no release                  |

Allowed scopes: `core`, `nestjs`, `postgres`, `mongodb`, `kafka`, `rabbitmq`, `testing`, `eslint-plugin`, `otel`, `repo`, `ci`, `deps`, `release`. A scope is optional.

All packages always share one version. Contributors never publish and never edit version fields. Dependabot PRs use `chore(deps)` and therefore do not release; a maintainer retitles one to `fix(deps): ...` when a dependency change must ship. See [docs/releasing.md](docs/releasing.md) for how releases work, prerelease channels and troubleshooting.

See [docs/ci-cd.md](docs/ci-cd.md) for the details of each workflow.

## Code of conduct and security

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
