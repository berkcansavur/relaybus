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
| `pnpm test:integration` | Integration project only, needs Docker                             |
| `pnpm coverage`         | All tests with V8 coverage; fails below the thresholds             |
| `pnpm mutation`         | Stryker mutation testing                                           |
| `pnpm release:dry-run`  | semantic-release dry run (needs a GitHub token, see docs)          |

## Test strategy

- Unit tests live in `packages/<name>/test/**/*.test.ts` and must not need Docker or the network. Use the fake clock and in-memory transport from `@relaybus/testing`.
- Integration tests are named `*.integration.test.ts` and run against real services through Testcontainers.
- **Contract suite for new stores.** Every `EventStore` implementation must pass the shared contract suite exported by `@relaybus/testing`. Adding a store means adding an integration test that calls the contract suite with your store factory, then adding store-specific tests for anything outside the contract (leasing details, transactions, indexes).
- Property-based tests use `fast-check` where invariants matter (ordering keys, retry backoff bounds).

## Coverage policy

Coverage is 100% for statements, branches, functions and lines, enforced in `vitest.config.ts` and in CI.

- Do not use `/* v8 ignore */`, `/* istanbul ignore */` or similar comments.
- Do not add files to the coverage `exclude` list to dodge the gate. The only exclusions are barrel `index.ts` files and `packages/core/src/standard-schema.ts` (type-only vendored spec).
- If code cannot be covered, it is usually dead or needs a different design. Delete it or make it testable.

## Mutation testing

Stryker checks that tests fail when the code changes (`pnpm mutation`, config in `stryker.config.json`). It runs weekly in CI and on demand. Current thresholds: break 75, low 80, high 90. Surviving mutants in code you touched are worth killing with a sharper assertion. The HTML report is written to `reports/mutation/mutation.html`.

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

Allowed scopes: `core`, `nestjs`, `postgres`, `mongodb`, `kafka`, `rabbitmq`, `testing`, `repo`, `ci`, `deps`, `release`. A scope is optional.

All packages always share one version. Contributors never publish and never edit version fields. Dependabot PRs use `chore(deps)` and therefore do not release; a maintainer retitles one to `fix(deps): ...` when a dependency change must ship. See [docs/releasing.md](docs/releasing.md) for how releases work, prerelease channels and troubleshooting.

See [docs/ci-cd.md](docs/ci-cd.md) for the details of each workflow.

## Code of conduct and security

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
