# CI/CD

All automation lives in `.github/`. Every third-party action is pinned to a full commit SHA with a version comment; Dependabot (`github-actions` ecosystem) keeps them current.

## Workflows

| File                    | Trigger                                   | Purpose                                                                                   |
| ----------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------- |
| `ci.yml`                | push to `main`, pull requests, reusable   | quality, tests with coverage, build checks, NestJS compatibility, aggregate `CI pass` job |
| `release.yml`           | push to `main`                            | Runs `ci.yml` as a gate, then Changesets: "Version Packages" PR or npm publish            |
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

1. `gate` calls `ci.yml` (Codecov upload skipped). The release job needs it to pass.
2. `changesets/action` runs. If unreleased changesets exist it opens or updates a "chore: version packages" PR (it pushes through the GitHub API, so commits are verified). When that PR is merged, the next run finds no changesets and runs `pnpm release` (build, then `changeset publish`), pushes tags and creates GitHub releases.
3. Publishing authenticates with npm Trusted Publishing (OIDC). The job has `id-token: write` and upgrades npm to `>= 11.5.1`. Provenance is on through `publishConfig.provenance` and `NPM_CONFIG_PROVENANCE`.

Fallback without Trusted Publishing: create an npm automation token, add it as the repository secret `NPM_TOKEN`, and uncomment the `NODE_AUTH_TOKEN` line in `release.yml`. Remove both once Trusted Publishing works.

Note: `changeset publish` calls `pnpm publish`, which hands the tarball to the `npm` binary. That is why the npm upgrade step matters. After the first real release, confirm the packages show a provenance badge; if publishing fails with an auth error, check the Trusted Publisher fields below first (workflow filename must be exactly `release.yml`).

## What the owner must do manually

### 1. npm organization and Trusted Publishers

1. Create the npm organization `relaybus` on npmjs.com (Add Organization, free plan for public packages).
2. A package must exist before a Trusted Publisher can be attached. For the very first publish of each new package either publish `0.0.0`/first version once from your machine with `npm publish --access public` (2FA), or use the placeholder approach npm documents for new packages.
3. For each of `@relaybus/core`, `@relaybus/nestjs`, `@relaybus/postgres`, `@relaybus/mongodb`, `@relaybus/kafka`, `@relaybus/rabbitmq`, `@relaybus/testing`: open the package on npmjs.com, Settings, "Trusted Publisher", choose GitHub Actions and enter:
   - Organization or user: `berkcansavur`
   - Repository: `relaybus`
   - Workflow filename: `release.yml`
   - Environment: leave empty (or the environment name if you add `environment:` to the release job)
4. Optionally set "Require two-factor authentication and disallow tokens" on each package once OIDC publishing works.

### 2. Codecov

1. Sign in at codecov.io with GitHub and add `berkcansavur/relaybus`.
2. Enable the Codecov GitHub App on the repository (OIDC uploads for public repos need no token). Do not add a token secret.
3. Optional: add a `codecov.yml` later to tune PR comments. The 100% gate is enforced by Vitest, not Codecov.

### 3. Repository settings (Settings in GitHub)

- General: enable "Allow squash merging" only, default message "Pull request title"; enable "Automatically delete head branches".
- Actions, General: "Workflow permissions" = Read repository contents; enable "Allow GitHub Actions to create and approve pull requests" (required for the Version Packages PR). Restrict allowed actions to "Allow actions created by GitHub and verified creators" or an allow-list; SHA pinning policy can be required here.
- Rules, Rulesets, new branch ruleset targeting `main`:
  - Require a pull request before merging (1 approval once there is a second maintainer; code owner review optional)
  - Require status checks: `CI pass`, `analyze (javascript-typescript)`, `dependency review`; require branches to be up to date
  - Require linear history
  - Block force pushes and deletions
  - Signed commits are not required
- Code security: enable Dependency graph, Dependabot alerts, Dependabot security updates, Secret scanning with push protection, and Private vulnerability reporting (needed by `SECURITY.md`). CodeQL is configured by `codeql.yml`, so leave "default setup" off.
- Discussions: enable if you keep the link in `ISSUE_TEMPLATE/config.yml`.
- Optionally create an `npm-publish` environment with required reviewers for a manual approval before publishing; then add `environment: npm-publish` to the `release` job and the same name in the npm Trusted Publisher form.

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
