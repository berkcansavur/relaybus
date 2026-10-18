# CI/CD

All automation lives in `.github/`. Every third-party action is pinned to a full commit SHA with a version comment; Dependabot (`github-actions` ecosystem) keeps them current.

## Workflows

| File                    | Trigger                                   | Purpose                                                                                   |
| ----------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------- |
| `ci.yml`                | push to `main`, pull requests, reusable   | quality, tests with coverage, build checks, NestJS compatibility, aggregate `CI pass` job |
| `release.yml`           | push to `main`, `next`, `beta`            | Runs `ci.yml` as a gate, then semantic-release: tag, npm publish (OIDC), GitHub Release   |
| `pr-title.yml`          | pull requests (`pull_request_target`)     | Fails PRs whose title is not a valid Conventional Commit (type and scope allow-lists)     |
| `codeql.yml`            | push, PR, weekly                          | CodeQL static analysis (`javascript-typescript`, `security-extended`)                     |
| `dependency-review.yml` | pull requests                             | Fails PRs that add dependencies with known vulnerabilities (moderate and above)           |
| `scorecard.yml`         | weekly, push to `main`, branch rule edits | OpenSSF Scorecard, published results and SARIF upload to code scanning                    |
| `mutation.yml`          | weekly, manual                            | Stryker mutation testing, uploads the HTML/JSON report, fails below `thresholds.break`    |

`.github/actions/setup` is a local composite action shared by the jobs: it installs pnpm (version from `packageManager`), Node.js with the pnpm store cached, and runs `pnpm install --frozen-lockfile`.

### ci.yml jobs

- `quality`: `pnpm format:check`, `pnpm lint`, `pnpm typecheck`.
- `test (node 22.x | 24.x)`: `pnpm coverage` runs unit and integration projects (Testcontainers use the Docker daemon on `ubuntu-latest`). It fails when any 100% threshold from `vitest.config.ts` is missed. `coverage/lcov.info` and `coverage-summary.json` are uploaded as an artifact and to Codecov with OIDC (`use_oidc: true`, `fail_ci_if_error: false`), so no `CODECOV_TOKEN` is needed.
- `build`: `pnpm build`, per-package `typecheck`, `publint` and `attw --pack` for every package.
- `nestjs compat (@nestjs/* 11 | 12)`: re-resolves the `@nestjs/*` dev dependencies of `packages/nestjs` to the matrix major with `pnpm --filter @relaybus/nestjs add --save-dev`, asserts the resolved major, then runs the typecheck and the nestjs tests. The changed manifest and lockfile exist only on the runner.
- `CI pass`: succeeds only when all of the above succeeded. Use this single check in branch protection.

### release.yml flow

1. `gate` calls `ci.yml` (Codecov upload skipped). The `release` job needs it to pass.
2. The `release` job runs in its own job (never inside a called workflow, because npm Trusted Publishing checks the calling workflow filename) in the GitHub environment `npm-publish`. It checks out the full history, installs npm 11.20.0 (>= 11.5.1 is required for OIDC) and runs `semantic-release`.
3. semantic-release reads the commits since the last `v*` tag, computes the next version, and if there is one: `scripts/set-version.mjs` writes it into every `packages/*/package.json` (CI workspace only), the packages are built, `scripts/publish-packages.mjs` runs `pnpm pack` (which rewrites `workspace:` ranges) and `npm publish <tarball> --provenance --tag <channel>` for `core`, `testing`, then the rest, skipping versions already on npm. Finally the tag and the GitHub Release are created. No commit is pushed to `main`.
4. `concurrency: release` serializes releases and never cancels a started one.

Full rules, prerelease channels and recovery steps: [releasing.md](releasing.md).

## What the owner must do manually

### 1. npm organization, bootstrap and Trusted Publishers

Follow [releasing.md](releasing.md#bootstrap-one-time): create the npm org `relaybus`, publish the `0.0.0` placeholders with `scripts/bootstrap-npm.sh`, attach a Trusted Publisher to each package (workflow `release.yml`, environment `npm-publish`), and create the `v0.0.0` tag.

### 2. Codecov

1. Sign in at codecov.io with GitHub and add `berkcansavur/relaybus`.
2. Enable the Codecov GitHub App on the repository (OIDC uploads for public repos need no token). Do not add a token secret.
3. Optional: add a `codecov.yml` later to tune PR comments. The 100% gate is enforced by Vitest, not Codecov.

### 3. Repository settings (Settings in GitHub)

- General: enable "Allow squash merging" only, default message "Pull request title"; enable "Automatically delete head branches".
- Actions, General: "Workflow permissions" = Read repository contents; Restrict allowed actions to "Allow actions created by GitHub and verified creators" or an allow-list; SHA pinning policy can be required here.
- Rules, Rulesets, new branch ruleset targeting `main`:
  - Require a pull request before merging (1 approval once there is a second maintainer; code owner review optional)
  - Require status checks: `CI pass`, `analyze (javascript-typescript)`, `dependency review`, `conventional PR title`; require branches to be up to date
  - Require linear history
  - Block force pushes and deletions
  - Signed commits are not required
- Code security: enable Dependency graph, Dependabot alerts, Dependabot security updates, Secret scanning with push protection, and Private vulnerability reporting (needed by `SECURITY.md`). CodeQL is configured by `codeql.yml`, so leave "default setup" off.
- Discussions: enable if you keep the link in `ISSUE_TEMPLATE/config.yml`.
- Settings, Environments: create `npm-publish` (the `release` job uses it and the npm Trusted Publisher form must name it). Required reviewers are optional and add a manual approval before every publish; restrict deployment branches to `main`, `next` and `beta`.
- Rulesets: tags matching `v*` must be creatable by the Actions integration (the release workflow pushes them). Do not require signed commits for tags.

### 4. Mutation testing dependencies

Stryker is not in the lockfile yet. Install it once, commit `package.json` and `pnpm-lock.yaml`, then `mutation.yml` works:

```bash
pnpm add -w -D @stryker-mutator/core@10.0.0 @stryker-mutator/vitest-runner@10.0.0
```

`stryker.config.json` runs the Docker-free unit project through `vitest.stryker.config.ts`. Treat the thresholds (high 90, low 80, break 75) as a starting point and raise `break` as the score improves.

## Local equivalents

```bash
pnpm format:check && pnpm lint && pnpm typecheck   # quality
pnpm coverage                                      # test job (needs Docker)
pnpm build && pnpm -r exec publint . && pnpm -r exec attw --pack .   # build job
pnpm mutation                                      # after installing Stryker
```

Validate workflows with actionlint:

```bash
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest -color
```
